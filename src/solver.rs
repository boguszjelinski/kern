/// Kabina minibus/taxi dispatcher
/// Copyright (c) 2025 by Bogusz Jelinski bogusz.jelinski@gmail.com
/// 
use std::io::prelude::*;
use std::process::Command;
use hungarian::minimize;
use std::fs::File;
use log::{debug, warn};
use crate::repo::create_reloc_route;
use crate::model::{Order, Stop, Cab};
use crate::distance::DIST;

pub const MAXLCM : usize = 40000; // !! max number of cabs or orders sent to LCM in C

// TODO: this is a very primitive greedy - does not search the lowest in the whole array but in the current row
// improve it!
//
// move free cabs (that exceed the capacity of their stops) to a nearest stop with enough capacity
pub fn relocate_free_cabs(free_cabs: &Vec<Cab>, stops: &Vec<Stop>, max_route_id: &mut i64, max_leg_id: &mut i64) -> String {
    // how to decide which cab should be moved? Maybe the one with highest battery charge?
    // find cabs that should move
    
    // now constraint for the greedy algo 
    // keeping track of available places at a stop, this vector will have the same length as 'stops'
    let mut stop_capa: Vec<i16> = count_capacity(free_cabs, stops);
    
    // now iterate over cabs and find first available stop, nearest stop!!!
    // create the return vector of indexes of stops: [0]=5 means first cab goes to sixth stop
    let mut ret: Vec<i32> = vec!();

    for c in free_cabs.iter() {
        // firstly - does the cab need to move? 
        if capacity(c.location, stops, &stop_capa) >= 0 { // a stop with capacity 0 would give a negative here, at least -1
            ret.push(-1); // do not move the cab at that index
            continue;
        }
        // then find nearest stop with enough capacity
        let mut dist = 1000; // any big value, we are looking for the nearest
        let mut dest: i32 = -1; // index of destination, index of Vec<Stop>
        for (idx, capa) in stop_capa.iter().enumerate() {
            let dist_to_stop = unsafe { DIST[c.location as usize][stops[idx].id as usize] };
            if *capa > 0 && dist_to_stop < dist {
                dist = dist_to_stop;
                dest = idx as i32;
            }
        }
        if dest == -1 {
            panic!("no stop found with enough capacity");
        }
        // decrease capacity of that stop 
        stop_capa[dest as usize] -= 1;
        ret.push(dest); // we could create the route here but we want to be compatible with any GLPK interface
    }
    let mut sql: String = String::from("");
    let mut total_dist: i32 = 0;
    for (idx, r) in ret.iter().enumerate() {
        if *r == -1 { // enough capacity for this cab
            continue;
        }
        debug!("Relocating cab_id={} to stop={}, distance={}", free_cabs[idx].id, stops[*r as usize].id,
                    unsafe { DIST[free_cabs[idx].location as usize][stops[*r as usize].id as usize]});
        total_dist += unsafe { DIST[free_cabs[idx].location as usize][stops[*r as usize].id as usize] as i32 };
        sql += & create_reloc_route(&free_cabs[idx], stops[*r as usize].id, max_route_id, max_leg_id);
    }
    if sql.len() > 0 {
        debug!("Total cost of relocation (LCM): {}", total_dist);
    }
    return sql;
}

pub fn count_capacity(free_cabs: &Vec<Cab>, stops: &Vec<Stop>) -> Vec<i16> {
    let mut stop_capa: Vec<i16> = vec!();
    // sum up occupated places
    for s in stops.iter() {
        // sum up free cabs at the stop
        let mut count = 0;
        for c in free_cabs.iter() {
            if c.location as i64 == s.id  {
                count += 1;
            }
        } 
        stop_capa.push(s.capacity - count);
    }
    return stop_capa;
}

fn capacity(stop_id: i32, stops: &Vec<Stop>, stop_capa: &Vec<i16>) -> i16 {
    for (idx, s) in stops.iter().enumerate() {
        if s.id as i32 == stop_id {
            return stop_capa[idx];
        }
    }
    panic!("Cab's location not found in the list of stops");
}

pub fn relocate_free_cabs_glpk(free_cabs: &Vec<Cab>, stops: &Vec<Stop>, max_route_id: &mut i64, max_leg_id: &mut i64) -> String {
    let mut sql: String = String::from("");
    if free_cabs.len() == 0 { // nothing to do
        return sql;
    }
    // count how many places are still available at stops
    let mut stop_capa: Vec<i16> = count_capacity(free_cabs, stops);
    // now reduce the size of cabs and stops vectors - cabs that do not need to be moved 
    //and stops with no capacity
    let mut cab_idx: Vec<usize> = vec!(); // index i free_cabs
    let mut stop_idx: Vec<usize> = vec!(); // index i stops
    for (idx, c) in free_cabs.iter().enumerate() {
        // does the cab need to move? 
        if capacity(c.location, stops, &stop_capa) < 0 { // a stop with capacity 0 would always give a negative here, at least -1
            cab_idx.push(idx);
        }
    }
    if cab_idx.len() == 0 { // all cabs rest at a stop with enough capacity
        return sql;
    }
    for (idx, _s) in stops.iter().enumerate() {
        // does the cab need to move? 
        if stop_capa[idx] > 0 {
            stop_idx.push(idx);
        }
    }
    if stop_idx.len() == 0 {
        warn!("No stop with enough capacity for cabs in need");
        return sql;
    }
    let result = run_glpk(free_cabs, &cab_idx, stops, &stop_idx, &stop_capa);
    let mut total_dist: i32 = 0;
    for (stop_i, cab_i) in result {
        debug!("Relocating cab_id={} to stop={}", free_cabs[cab_i].id, stops[stop_i].id);
        total_dist += unsafe { DIST[free_cabs[cab_i].location as usize][stops[stop_i].id as usize] as i32 };
        sql += & create_reloc_route(&free_cabs[cab_i], stops[stop_i].id, max_route_id, max_leg_id);
    }
    println!("Total cost of relocation (GLPK): {}", total_dist);
    return sql;
}

// returns vectors of indexes in free_cabs and stops
fn run_glpk(free_cabs: &Vec<Cab>, cab_idx: &Vec<usize>, stops: &Vec<Stop>, stop_idx: &Vec<usize>, 
            capacity: &Vec<i16>) -> Vec<(usize, usize)> {
    let mut file = File::create("glpk.mod").unwrap();
    let s = "param ii, integer, > 0;\n\
        set I := 1..ii;\n\
        param jj, integer, > 0;\n\
        set J := 1..jj;\n\
        param capacity{j in J}, integer;\n\
        param c{i in I, j in J};\n\
        var x{i in I, j in J} >= 0, binary;\n\
        s.t. cabs{i in I}: sum{j in J} x[i,j] = 1;\n\
        s.t. stops{j in J}: sum{i in I} x[i,j] <= capacity[j];\n\
        minimize cost: sum{i in I, j in J} c[i,j] * x[i,j];\n\
        solve;\n\
        table tbl{(j, i) in {J, I}: x[i,j] = 1} OUT \"CSV\" \"out.csv\": j,i;\n\
        data;\n";
    let mut str: String = format!("{}param ii := {};\nparam jj := {};\nparam capacity := ", 
                            s, cab_idx.len(), stop_idx.len());
    for (idx, s) in stop_idx.iter().enumerate() {
        str += &format!("{} {},", idx + 1, capacity[*s]); // GLPK's indexes start with 1
    }
    // remove last comma
    str.truncate(str.len() - 1);
    str += ";\nparam c : ";
    for (idx, _) in stop_idx.iter().enumerate() {
        str += &format!("{} ", idx + 1); // GLPK's indexes start with 1
    }
    str += ":=\n";
    for (cab_i, c) in cab_idx.iter().enumerate() {
        str += &format!("  {}", cab_i + 1);
        for (stop_i, s) in stop_idx.iter().enumerate() {
            str += &format!(" {}", unsafe { 
                DIST[free_cabs[cab_idx[cab_i]].location as usize][stops[stop_idx[stop_i]].id as usize]});
        }
        str += "\n";
    }
    str += ";\nend;";
    file.write_all(str.as_bytes()).unwrap();

    if cfg!(target_os = "windows") {
        Command::new("cmd")
            .args(["/C", "glpsol -m glpk.mod"])
            .output()
            .expect("failed to execute process")
    } else {
        Command::new("sh")
            .arg("-c")
            .arg("glpsol -m glpk.mod")
            .output()
            .expect("failed to execute process")
    };

    let file = std::fs::File::open("out.csv").unwrap();
    let mut rdr = csv::ReaderBuilder::new()
       .has_headers(true)
       .from_reader(file);
    let mut ret: Vec<(usize, usize)> = vec!();
    for result in rdr.records().into_iter() {
       let record = result.unwrap();
       let stop_i = record[0].to_string().parse::<usize>().unwrap() - 1; // -1 as GLPK index starts with 1
       let cab_i  = record[1].to_string().parse::<usize>().unwrap() - 1; 
       ret.push((stop_idx[stop_i], cab_idx[cab_i]));
    }
    return ret;
}

/*
# glpsol -m glpk.mod

param ii, integer, > 0;
set I := 1..ii;
param jj, integer, > 0;
set J := 1..jj;
param capacity{j in J}, integer;
param c{i in I, j in J};
var x{i in I, j in J} >= 0, binary;

s.t. cabs{i in I}: sum{j in J} x[i,j] = 1;
s.t. stops{j in J}: sum{i in I} x[i,j] <= capacity[j];
minimize cost: sum{i in I, j in J} c[i,j] * x[i,j];
solve;
table tbl{(j, i) in {J, I}: x[i,j] = 1} OUT "CSV" "out.csv": j,i;

data;
param ii := 4;
param jj := 3;
param capacity := 1 2, 2 2, 3 2;

param c :     1 2 3 :=
           1  5 1 9
           2  5 2 9
           3  0 3 5
           4  5 8 0;
end;
*/
/* The above glpk.mod programme will give the below out.csv file, 
the second stop will get the cab 1 and 2:
j,i
1,3
2,1
2,2
3,4
*/

// returns indexes of orders assigned to cabs - vec[1]==5 would mean 2nd cab assigned 6th order
pub fn munkres(cabs: &Vec<Cab>, orders: &Vec<Order>) -> Vec<i16> {
    let mut ret: Vec<i16> = vec![];
    let mut matrix: Vec<i32> = vec![];
    
    for c in cabs.iter() {
        for o in orders.iter() {
            unsafe {
                let dst = (DIST[c.location as usize][o.from as usize] + c.dist) as i32;
                matrix.push(if dst <= o.wait { dst } else { 1000 }); // 1k = block this 
            }
        }
    }
    let assignment = minimize(&matrix, cabs.len() as usize, orders.len() as usize);
    
    for s in assignment {
        if s.is_some() {
            ret.push(s.unwrap() as i16);
        } else {
            ret.push(-1);
        }
    }
    return ret;
}
