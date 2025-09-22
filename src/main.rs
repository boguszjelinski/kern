/// Kabina minibus/taxi dispatcher
/// Copyright (c) 2025 by Bogusz Jelinski bogusz.jelinski@gmail.com

mod repo;
mod model;
mod distance;
mod extender;
mod pool;
mod stats;
mod utils;
mod solver;
use distance::{DIST, read_dist};
use model::{KernCfg, Order, OrderStatus, OrderTransfer, Stop, Cab, CabStatus, Branch,
            MAXSTOPSNUMB, MAXCABSNUMB, MAXORDERSNUMB, MAXBRANCHNUMB, MAXINPOOL};
use stats::{Stat,update_max_and_avg_time,update_max_and_avg_stats,incr_val};
use pool::{MAX_THREAD_NUMB, orders_to_transfer_array, cabs_to_array, stops_to_array, find_pool};
use repo::{assign_pool_to_cab, assign_requests_for_free_cabs, run_sql, find_free_cab_and_on_last_leg, 
            find_cab_by_status, assign_order_to_cab_lcm};
use extender::{find_matching_routes, get_handle}; // write_sql_to_file
use solver::{munkres, relocate_free_cabs, lcm_slow};
use utils::get_elapsed;
use mysql::*;
use mysql::prelude::*;
use chrono::{Local, Duration};
use std::collections::HashMap;
use std::ptr::addr_of;
use std::time::Instant;
use std::{thread, env};
use std::cmp::min;

use log::{info,warn,debug,error,LevelFilter};
use log4rs::{
    append::{
        console::{ConsoleAppender, Target},
        file::FileAppender,
    },
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
    filter::threshold::ThresholdFilter,
};

const CFG_FILE_DEFAULT: &str = "kern.toml";
const MAXLCM : usize = 40000; // !! max number of cabs or orders sent to LCM in C

#[link(name = "dynapool")]
unsafe extern "C" {
    unsafe fn dynapool(
		numbThreads: i32,
        poolsize: &[i32; MAXINPOOL - 1], // max sizes
		distance: *const [[i16; MAXSTOPSNUMB]; MAXSTOPSNUMB], 
		distSize: i32,
		stops: &[Stop; MAXSTOPSNUMB],
		stopsSize: i32,
		orders: &[OrderTransfer; MAXORDERSNUMB],
		ordersSize: i32,
		cabs: &[Cab; MAXCABSNUMB],
		cabsSize: i32,
        maxAngle: i16,
        maxAngleDist: i16,
        stopWait: i16,
        sortFunc: i8,
		ret: &mut [Branch; MAXBRANCHNUMB], // returned values
		retSize: i32,
		count: &mut i32, // returned count of values
        pooltime: &mut [i32; MAXINPOOL - 1] // performance statistics
    );
    
    unsafe fn slow_lcm(
        distance: *const [[i16; MAXSTOPSNUMB]; MAXSTOPSNUMB],
        distSize: i32,
        orders: &[OrderTransfer; MAXORDERSNUMB],
        ordersSize: i32,
        cabs: &[Cab; MAXCABSNUMB],
        cabsSize: i32,
        how_many: i32,
        supply: &mut [i32; MAXLCM], // returned values
        demand: &mut [i32; MAXLCM], // returned values
        count: &mut i32 // returned count of values
    );

    unsafe fn initMem();
    unsafe fn freeMem();
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>>  {
    // cargo rustc --release -- -L /Users/bogusz.jelinski/Rust/kern/pool
    // RUSTFLAGS='-L /Users/bogusz.jelinski/Rust/kern/pool' cargo build --release
    //println!("cargo:rustc-link-arg=--max-memory=20294967296");
    // reading Config
    let mut cfg_file: String = CFG_FILE_DEFAULT.to_string();

    // command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() == 3 && args[1] == "-f" {
        cfg_file = args[2].to_string();
    }
    info!("Config file: {cfg_file}");
    let settings = config::Config::builder()
        .add_source(config::File::with_name(&cfg_file))
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `APP_DEBUG=1 ./target/app` would set the `debug` key
        //.add_source(config::Environment::with_prefix("APP"))
        .build()
        .unwrap();
    let cfg = settings
        .try_deserialize::<HashMap<String, String>>()
        .unwrap();
    let db_conn_str = &cfg["db_conn"];

    info!("Database: {db_conn_str}");
    setupcfg(&cfg);
    let cfig = KernCfg::access();

    // init DB
    let url: &str = &db_conn_str;
    let pool = Pool::new(url)?;
    let mut conn = pool.get_conn()?;

    let stops = repo::read_stops(&mut conn);
    
    let dist_file = &cfg["dist_file"];
    if dist_file.len() > 0 { // distances will be read from file
        read_dist(dist_file, stops.len());
    } else { // distances will be calculated
        distance::init_distance(&stops, cfig.cab_speed);
        //dump_dist("distance.txt", MAXSTOPSNUMB);
    }

    unsafe {
        if cfig.use_extern_pool {
            initMem();
        }
    }

    let run_by = &cfg["run_by"];
    // Kern main, infinite loop, but see "run_by"
    loop {
        conn = pool.get_conn()?;
        let start = Instant::now();
        // get newly requested trips and free cabs, reject expired orders (no luck this time)
        let tmp_model = prepare_data(&mut conn, cfig.max_assign_time);
        match tmp_model {
            Some(mut x) => { 
                dispatch(&db_conn_str, &mut conn, &mut x.0, &mut x.1, &stops, *cfig);
            },
            None => {
                info!("Nothing to do");
            }
        }
        update_max_and_avg_time(Stat::AvgSchedulerTime, Stat::MaxSchedulerTime, start);

        if run_by != "iterate" { // exit after one iteration
            return Ok(());
        }
        // check if we should wait for new orders
        let mut wait: u64 = cfig.run_delay.saturating_sub(start.elapsed().as_secs());
        if wait == 0 {
           continue;
        }

        if wait > 60 {
            // TODO: find the bug!
            warn!("Strange wait time: {}", wait);
            wait = 0;
        }
        if wait > 0 {
            debug!("Sleeping in {} secs", wait);
            thread::sleep(std::time::Duration::from_secs(wait));
        }
    }
}

fn setupcfg(cfg: & HashMap<String, String>) {
    let mut c: KernCfg = KernCfg::new();
    c.max_assign_time = cfg["max_assign_time"].parse().unwrap();
    c.max_solver_size = cfg["max_solver_size"].parse().unwrap();
    c.run_delay      = cfg["run_delay"].parse().unwrap();
    c.max_legs       = cfg["max_legs"].parse().unwrap();
    c.max_angle      = cfg["max_angle"].parse().unwrap();
    c.max_angle_dist = cfg["max_angle_dist"].parse().unwrap();
    c.use_pool       = cfg["use_pool"].parse::<bool>().unwrap();
    c.use_extern_pool= cfg["use_extern_pool"].parse::<bool>().unwrap();
    c.use_extender   = cfg["use_extender"].parse::<bool>().unwrap();
    c.thread_numb    = cfg["thread_numb"].parse().unwrap();
    c.stop_wait      = cfg["stop_wait"].parse().unwrap();
    c.cab_speed      = cfg["cab_speed"].parse().unwrap();
    c.max_pool5_size = cfg["max_pool5_size"].parse().unwrap();
    c.max_pool4_size = cfg["max_pool4_size"].parse().unwrap();
    c.max_pool3_size = cfg["max_pool3_size"].parse().unwrap();
    c.max_pool2_size = cfg["max_pool2_size"].parse().unwrap();
    c.max_pool2_size = cfg["max_pool2_size"].parse().unwrap();
    c.solver_delay =cfg["solver_delay"].parse().unwrap();

    if c.thread_numb > MAX_THREAD_NUMB as i32 - 1 {
        c.thread_numb = MAX_THREAD_NUMB as i32 - 1; // because of peculiar construct with children.push in pool.rs
    }
    KernCfg::put(c);

    setup_logger(cfg["log_file"].clone());
    info!("Starting up with config:"); 
    info!("max_assign_time: {}", c.max_assign_time);
    info!("max_solver_size: {}", c.max_solver_size);
    info!("run_by: {}", cfg["run_by"]);
    info!("run_delay: {}", c.run_delay);
    info!("max_legs: {}", c.max_legs);
    info!("max_angle: {}", c.max_angle);
    info!("max_angle_dist: {}", c.max_angle_dist);
    info!("use_pool: {}", c.use_pool);
    info!("use_extern_pool: {}", c.use_extern_pool);
    info!("use_extender: {}", c.use_extender);
    info!("thread_numb: {}", c.thread_numb);
    info!("stop_wait: {}", c.stop_wait);
    info!("cab_speed: {}", c.cab_speed);
    info!("pool5_size: {}", c.max_pool5_size);
    info!("pool4_size: {}", c.max_pool4_size);
    info!("pool3_size: {}", c.max_pool3_size);
    info!("pool2_size: {}", c.max_pool2_size);
    info!("solver_delay: {}", c.solver_delay);
}

fn setup_logger(file_path: String) {
    let level = log::LevelFilter::Info;
    // Build a stderr logger.
    let stderr = ConsoleAppender::builder().target(Target::Stderr).build();
    // Logging to log file.
    let logfile = FileAppender::builder()
        // Pattern: https://docs.rs/log4rs/*/log4rs/encode/pattern/index.html
        .encoder(Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S)} {l} - {m}\n")))
        .build(file_path)
        .unwrap();

    // Log Trace level output to file where trace is the default level
    // and the programmatically specified level to stderr.
    let config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .appender(
            Appender::builder()
                .filter(Box::new(ThresholdFilter::new(level)))
                .build("stderr", Box::new(stderr)),
        )
        .build(
            Root::builder()
                .appender("logfile")
                .appender("stderr")
                .build(LevelFilter::Debug),
        )
        .unwrap();

    // Use this to change log levels at runtime.
    // This means you can change the default log level to trace
    // if you are trying to debug an issue and need more logs on then turn it off
    // once you are done.
    let _handle = log4rs::init_config(config);
}

fn run_extender(conn: &mut PooledConn, orders: &Vec<Order>, stops: &Vec<Stop>, 
                max_leg_id: &mut i64, label: &str, cfg: &KernCfg) -> Vec<Order> {
    let len_before = orders.len();
    if cfg.use_extender {
        let start_extender = Instant::now();
        let demand = find_matching_routes(conn, orders, &stops, max_leg_id, cfg);
        update_max_and_avg_time(Stat::AvgExtenderTime, Stat::MaxExtenderTime, start_extender);
        let len_after = demand.len();
        if len_before != len_after {
            info!("{}: route extender allocated {} requests, max_leg_id: {}", label, len_before - len_after, max_leg_id);
        } else {
            info!("{}: extender has not helped", label);
        }
        return demand;
    } else {
        return orders.to_vec();
    }
}

// three steps:
// 1) route extender
// 2) pool finder
// 3) solver (LCM in most scenarious won't be called)
// SQL updates execute in background as async
fn dispatch(host: &String, conn: &mut PooledConn, orders: &mut Vec<Order>, mut cabs: &mut Vec<Cab>, stops: &Vec<Stop>, cfg: KernCfg) -> usize {
    let mut max_route_id : i64 = repo::read_max(conn, "route"); // +1, first free ID
    let mut max_leg_id : i64 = repo::read_max(conn, "leg");

    if orders.len() == 0 {
        info!("No demand, no dispatch");
        // but check orders from free cabs
        assign_requests_for_free_cabs(conn, &mut max_route_id, &mut max_leg_id);
        return 0;
    }
    stats::update_max_and_avg_stats(Stat::AvgDemandSize, Stat::MaxDemandSize, orders.len() as i64);

    // orders that cannot be shared won't be sent to extender and pool finder
    // but they have to be added before Munkres is called, let's split the order set
    let mut demand_nonshared = orders.clone();
    orders.retain(|&o| o.shared == true);    
    demand_nonshared.retain(|&o| o.shared == false);

    // check if we want to run extender is done in run_extender
    let mut demand
        = run_extender(conn, orders, &stops, &mut max_leg_id, "FIRST", &cfg);

    if cabs.len() == 0 {
        info!("No cabs");
        return 0;
    }
    // POOL FINDER
    if cfg.use_pool && orders.len() > 1 {
        let start_pool = Instant::now();
        stats::update_max_and_avg_stats(Stat::AvgPoolDemandSize, Stat::MaxPoolDemandSize, demand.len() as i64);
        let pl: Vec<Branch>;
        let sql: String;
        // 2 versions available - in C (external) and Rust
        info!("Find pool: demand size: {}", demand.len());
        if cfg.use_extern_pool {
            (pl, sql) = find_external_pool(&mut demand, cabs, stops, cfg.thread_numb, &mut max_route_id, &mut max_leg_id, cfg);
        } else {
            (pl, sql) = find_internal_pool(&mut demand, cabs, stops, &mut max_route_id, &mut max_leg_id, cfg);
        }
        update_max_and_avg_time(Stat::AvgPoolTime, Stat::MaxPoolTime, start_pool);
        //write_sql_to_file(itr, &sql, "pool");
        //for s in split_sql(sql, 150) {
        //    client.batch_execute(&s).unwrap();
        //}
        run_sql(conn, sql);
        // marking assigned orders to get rid of them; cabs are marked in find_pool 
        let numb = count_orders(pl, &demand);
        info!("Pool finder - number of assigned orders: {}", numb);

        // let's try extender on the new routes if there still is demand
        (*cabs, demand) = shrink(&cabs, demand);
        demand
            = run_extender(conn, &demand, &stops, &mut max_leg_id, "SECOND", &cfg);
    }

    // we don't want to run solver on new requests, without giving it a chance for a pool in another iteration, 
    // after some time with no luck in pool finder/extender we give it to the solver
    demand = get_old_orders(&demand, cfg.solver_delay);
    // nonshared orders should be delayed
    demand.append(&mut demand_nonshared);

    if demand.len() > 0 {
        // shrinking vectors, getting rid of .id == -1 and (TODO) distant orders and cabs !!!!!!!!!!!!!!!
        (*cabs, demand) = shrink(&cabs, demand);
        stats::update_max_and_avg_stats(Stat::AvgSolverDemandSize, Stat::MaxSolverDemandSize, demand.len() as i64);
        if cabs.len() == 0 {
            info!("No cabs after pool finder");
            return 0;
        }
        if demand.len() == 0 {
            info!("No demand after pool finder");
            return 0;
        }
        // LCM presolver
        let mut lcm_handle = thread::spawn(|| { });
        if demand.len() > cfg.max_solver_size && cabs.len() > cfg.max_solver_size {
            // too big to send to solver, it has to be cut by LCM
            // first just kill the default thread
            
            let start_lcm = Instant::now();
            lcm_handle.join().expect("LCM SQL thread being joined has panicked");
            let cabs_len = cabs.len();
            let ord_len = demand.len();
            let how_many = min(ord_len, cabs_len) as i16 - cfg.max_solver_size as i16;
            info!("LCM input: demand={}, supply={}, to be found: {}", ord_len, cabs_len, how_many);
            lcm_handle = lcm(host, &mut cabs, &mut demand, &mut max_route_id, &mut max_leg_id, 
                            how_many);
            update_max_and_avg_time(Stat::AvgLcmTime, Stat::MaxLcmTime, start_lcm);
            incr_val(Stat::TotalLcmUsed);
            (*cabs, demand) = shrink(&cabs, demand);
        }
        // SOLVER
        let start_solver = Instant::now();
        info!("Solver input - demand={}, supply={}", demand.len(), cabs.len());
        let sol = munkres(&cabs, &demand);
        let before_solver = max_route_id;

        let sql = repo::assign_cust_to_cab_munkres(sol, &cabs, &demand, &mut max_route_id, &mut max_leg_id);
        update_max_and_avg_time(Stat::AvgSolverTime, Stat::MaxSolverTime, start_solver);
        //write_sql_to_file(itr, &sql, "munkres");
        lcm_handle.join().expect("LCM SQL thread being joined has panicked");
        run_sql(conn, sql);
        info!("Dispatch completed, solver assigned: {}", max_route_id - before_solver);
    }
    run_sql(conn, repo::save_status());

    assign_requests_for_free_cabs(conn, &mut max_route_id, &mut max_leg_id); // someone went into and took this cab
    // let free_cabs = find_cab_by_status(conn, CabStatus::FREE);
    // let sql = relocate_free_cabs(&free_cabs, &stops, &mut max_route_id, &mut max_leg_id);
    // run_sql(conn, sql);
    return 0; // 0: all orders served
}


// remove orders and cabs allocated by the pool so that the vectors can be sent to solver
fn shrink(cabs: &Vec<Cab>, orders: Vec<Order>) -> (Vec<Cab>, Vec<Order>) {
    let mut new_cabs: Vec<Cab> = vec![];
    let mut new_orders: Vec<Order> = vec![];
    for c in cabs.iter() { 
        if c.id != -1 { new_cabs.push(*c); } // && dist=0 would get rid of those on last legs
    }
    for o in orders.iter() { 
        if o.id != -1 { new_orders.push(*o); }
    }
    return (new_cabs, new_orders);
}

fn get_old_orders(orders: &Vec<Order>, solver_delay: i32) -> Vec<Order> {
    let mut old_orders: Vec<Order> = vec![];
    for o in orders.iter() { 
        if get_elapsed(o.received) > solver_delay as i64 { 
            old_orders.push(*o); 
        }
    }
    return old_orders;
}

// count orders allocated by pool finder
// only for reporting
fn count_orders(pl: Vec<Branch>, orders: &Vec<Order>) -> i32 {
    let mut count_in_branches = 0;
    let mut count_in_orders = 0;
    for b in pl.iter() {
        for o in 0..b.ord_numb as usize {
            if b.ord_actions[o] == 'i' as i8 { // do not count twice
                if orders[b.ord_ids[o] as usize].id == -1 {
                    count_in_orders += 1;
                }
                count_in_branches += 1;
            }
        }
    }
    if count_in_branches != count_in_orders {
        panic!("Error! Number of orders marked as assigned ({}) does not equal orders in branches: {}",
            count_in_orders, count_in_branches);
    }
    return count_in_branches;
}

fn find_internal_pool(demand: &mut Vec<Order>, cabs: &mut Vec<Cab>, stops: &Vec<Stop>, 
                    max_route_id: &mut i64, max_leg_id: &mut i64, cfg: KernCfg) -> (Vec<Branch>, String) {
    let mut pl: Vec<Branch> = Vec::new();  
    let mut sql: String = String::from("");

    for p in (2..min(5, demand.len() + 1)).rev() { // 4,3,2
        if (p == 4 && demand.len() < (cfg.max_pool4_size) as usize ) ||
            (p == 3 && demand.len() < (cfg.max_pool3_size) as usize ) ||
            (p == 2 && demand.len() < (cfg.max_pool2_size) as usize ) {
            let now = Instant::now();
            let mut ret = find_pool(p as u8, cfg.thread_numb as i16,
                                                            demand,  cabs, &stops, max_route_id, max_leg_id,
                                                            cfg.max_angle, cfg.max_angle_dist, cfg.stop_wait);
            let el = now.elapsed().as_secs() as i64;
            match p {
                4 => update_max_and_avg_stats(Stat::AvgPool4Time, Stat::MaxPool4Time, el),
                3 => update_max_and_avg_stats(Stat::AvgPool3Time, Stat::MaxPool3Time, el),
                2 => update_max_and_avg_stats(Stat::AvgPool2Time, Stat::MaxPool2Time, el),
                _=>{},
            }
            //print_pool(&ret.0, demand, cabs);

            pl.append(&mut ret.0);
            sql += &ret.1;
        }
    }
    return (pl, sql);
}

/*
fn print_pool(list: &Vec<Branch>, demand: &Vec<Order>, cabs: &Vec<Cab>) {
    for b in list {
        let cab_cost = unsafe { DIST[cabs[b.cab as usize].location as usize][demand[b.ord_ids[0] as usize].from as usize] };
        print!("cost={}, cab={}, cab_cost={}: ", b.cost, b.cab, cab_cost);
        for c in 0..b.ord_numb as usize {
            if c < b.ord_numb as usize -1 {
                let from = if b.ord_actions[c as usize] == 105 { demand[b.ord_ids[c as usize] as usize].from }
                                else {demand[b.ord_ids[c as usize] as usize].to}; 
                let to = if b.ord_actions[(c+1) as usize] == 105 { demand[b.ord_ids[(c+1) as usize] as usize].from }
                                else {demand[b.ord_ids[c as usize] as usize].to}; 
                let cost = unsafe { DIST[from as usize][to as usize] };
                print!("{}{:?}[{}]({}), ", b.ord_ids[c], char::from_u32(b.ord_actions[c] as u32).unwrap(), from, cost);
            } else {
                let from = if b.ord_actions[c as usize -1] == 105 { demand[b.ord_ids[c as usize -1] as usize].from }
                                else {demand[b.ord_ids[c as usize -1] as usize].to}; 
                print!("{}{:?}[{}]({}), ", b.ord_ids[c], char::from_u32(b.ord_actions[c] as u32).unwrap(), 
                        demand[b.ord_ids[c as usize] as usize].to, 
                        unsafe { DIST[from as usize][demand[b.ord_ids[c as usize] as usize].to as usize] });
            }
        }
        println!("");
    }
}
*/

// if fail then dump input and output
fn validate_answer(br: &[Branch; MAXBRANCHNUMB], cnt: &i32, demand_size: usize, cabs: &Vec<Cab>) {
    let mut fail_found: bool = false;
    for i in 0 .. *cnt as usize {
        fail_found = false;
        if br[i].ord_numb > MAXINPOOL as i16 * 2 || br[i].ord_numb < 0 {
            warn!("ord_numb > {}", MAXINPOOL * 2);
            fail_found = true;
            break;
        }
        let mut in_count = 0;
        let mut out_count = 0;
        for j in 0 .. br[i].ord_numb as usize{
            if br[i].ord_ids[j] >= demand_size as i16 {
                warn!("ord_ids >= demandSize, id: {}, size: {}", br[i].ord_ids[j], demand_size);
                fail_found = true;
                break;
            }
            // last stop cannot be 'i'
            if (j == (br[i].ord_numb - 1) as usize) && br[i].ord_actions[j] == 'i' as i8 {
                warn!("last stop is 'i'");
                fail_found = true;
                break;
            }
            if br[i].ord_actions[j] == 'i' as i8 {
                in_count += 1;
                // every in has its out
                let mut found_out = false;
                for k in j + 1 .. br[i].ord_numb as usize {
                    if br[i].ord_actions[k] == 'o' as i8 
                        && br[i].ord_ids[k] == br[i].ord_ids[j] {
                            found_out = true;
                            break;
                        }
                }
                if !found_out {
                    warn!("OUT not found");
                    fail_found = true;
                    break;
                }
            }
            if br[i].ord_actions[j] == 'o' as i8 {
                out_count += 1;
            }
        }
        if fail_found { // go out of outer loop
            break;
        }
        if in_count != br[i].ord_numb / 2 {
            warn!("in_count: {}, ord_numb: {}", in_count, br[i].ord_numb);
            fail_found = true;
            break;
        }
        if out_count != br[i].ord_numb / 2 {
            warn!("out_count: {}, ord_numb: {}", out_count, br[i].ord_numb);
            fail_found = true;
            break;
        }
        // cab on the list
        if br[i].cab >= cabs.len() as i32 {
            warn!("Cab index {} higher than list.len {}", br[i].cab, cabs.len());
            fail_found = true;
            break;
        }
    }
    if fail_found { // dump input
        panic!("Pool corrupt");
    } 
}

pub fn extern_lcm(cabs: &Vec<Cab>, orders: &Vec<Order>, how_many: i16, _cfg: KernCfg) -> Vec<(i32,i32)> {
    let cabs_cpy = cabs.to_vec(); // clone
    let orders_cpy = orders.to_vec();
    let mut supply: [i32; MAXLCM] = [0; MAXLCM];
    let mut demand: [i32; MAXLCM] = [0; MAXLCM];
    let mut count: i32 = 0;

    unsafe { 
        /* if cabs.len() * orders.len() > cfg.max_solver_size * 20 {
            lcm_dummy(
                addr_of!(DIST),
                MAXSTOPSNUMB as i32,
                &orders_to_transfer_array(&orders_cpy),
                orders_cpy.len() as i32,
                &cabs_to_array(&cabs_cpy),
                cabs_cpy.len() as i32,
                how_many as i32,
                &mut supply, // returned values
                &mut demand,
                &mut count
                );
        }
        else {
        */
            slow_lcm(
                addr_of!(DIST),
                MAXSTOPSNUMB as i32,
                &orders_to_transfer_array(&orders_cpy),
                orders_cpy.len() as i32,
                &cabs_to_array(&cabs_cpy),
                cabs_cpy.len() as i32,
                how_many as i32,
                &mut supply, // returned values
                &mut demand,
                &mut count
            );
        //}
    }
    
    let mut pairs: Vec<(i32,i32)> = vec![];
    info!("LCM returned {} pairs", count);
    for i in 0..count as usize {
        pairs.push((supply[i], demand[i]));
    }
    return pairs;
}


// least/low cost method - shrinking the model so that it can be sent to solver
pub fn lcm(host: &String, mut cabs: &mut Vec<Cab>, mut orders: &mut Vec<Order>, 
            max_route_id: &mut i64, max_leg_id: &mut i64, how_many: i16/*, cfg: KernCfg*/) 
            -> thread::JoinHandle<()> {
    if how_many < 1 { // we would like to find at least one
        warn!("LCM asked to do nothing");
        return thread::spawn(|| { });
    }
    let pairs: Vec<(i32,i32)> = lcm_slow(cabs, orders, how_many);  // extern_lcm(cabs, orders, how_many, cfg);
    let sql = assign_order_to_cab_lcm(pairs, &mut cabs, &mut orders, max_route_id, max_leg_id);
    return get_handle(host.clone(), sql, "LCM".to_string());
}

// calling a C routine
fn find_external_pool(demand: &mut Vec<Order>, cabs: &mut Vec<Cab>, stops: &Vec<Stop>, threads: i32,
                      max_route_id: &mut i64, max_leg_id: &mut i64, cfg: KernCfg) -> (Vec<Branch>, String) {
    let mut ret: Vec<Branch> = Vec::new();  
    if demand.len() > MAXORDERSNUMB || cabs.len() > MAXCABSNUMB {
        error!("Demand or supply too big, accordingly {} and {}", demand.len(), cabs.len());
        return (ret, "".to_string());
    }
    let orders: [OrderTransfer; MAXORDERSNUMB] = orders_to_transfer_array(&demand);
    let mut br: [Branch; MAXBRANCHNUMB] = [Branch::new(); MAXBRANCHNUMB];
    let mut cnt: i32 = 0;
    let mut poolsize = [0; MAXINPOOL as usize - 1];
    let mut pooltime = [0; MAXINPOOL as usize - 1];

    unsafe {
        poolsize[0] = cfg.max_pool4_size;
        poolsize[1] = cfg.max_pool3_size;
        poolsize[2] = cfg.max_pool2_size;

        dynapool(
            threads,
            &poolsize,
            addr_of!(DIST),
            MAXSTOPSNUMB as i32,
            &stops_to_array(&stops),
            stops.len() as i32,
            &orders,
            demand.len() as i32,
            &cabs_to_array(&cabs),
            cabs.len() as i32,
            cfg.max_angle,
            cfg.max_angle_dist,
            cfg.stop_wait,
            0, // unsued
            &mut br, // returned values
            MAXBRANCHNUMB as i32,
            &mut cnt, // returned count of values
            &mut pooltime
        );
    }
    validate_answer(&br, &cnt, demand.len(), cabs);
    update_max_and_avg_stats(Stat::AvgPool4Time, Stat::MaxPool4Time, pooltime[0] as i64);
    update_max_and_avg_stats(Stat::AvgPool3Time, Stat::MaxPool3Time, pooltime[1] as i64);
    update_max_and_avg_stats(Stat::AvgPool2Time, Stat::MaxPool2Time, pooltime[2] as i64);

    //let cut = &br[0..cnt as usize];
    //print_pool(&cut.to_vec(), demand, cabs);

    // generate SQL
    let mut sql: String = String::from("");
    for i in 0 .. cnt as usize {
        // first two quality checks
        /*
        if br[i].cab == -1 || br[i].cab >= cabs.len() as i16 {
            error!("Wrong cab index: {}, array len: {}, array index: {}", br[i].cab, cnt, i);
            continue;
        }
        //print!("{}: ", br[i].ord_numb);
        for c in 0 .. br[i].ord_numb as usize {
            if br[i].ord_ids[c] < 0 || br[i].ord_ids[c] as usize > MAXORDERSNUMB {
                error!("Wrong order index: {}", br[i].ord_ids[c]);
                continue 'outer;
            }
            //print!("{}{:?},", br[i].ord_ids[c], char::from_u32(br[i].ord_actions[c] as u32).unwrap());
        }
        */
        //println!("");
        /*unsafe {
        if !wait_constraints_met(&br[i], 
                            DIST[cabs[br[i].cab as usize].location as usize][demand[br[i].ord_ids[0] as usize].from as usize],
                            &demand
                        ) {
            continue;
        }
        }*/
        ret.push(br[i]); // just convert to vec
        sql += &assign_pool_to_cab(cabs[br[i].cab as usize], demand, br[i], max_route_id, max_leg_id, cfg.stop_wait);
        // remove the cab from list so that it cannot be allocated twice, by LCM or Munkres
        cabs[br[i].cab as usize].id = -1;
        // mark orders as assigned too
        for o in 0..br[i].ord_numb as usize {
            demand[br[i].ord_ids[o] as usize].id = -1;
        }
    }
    //  RUN SQL
    return (ret, sql);
}

// checking only maxWait
/*
fn wait_constraints_met(el: &Branch, dist_cab: i16, orders: &Vec<Order>) -> bool {
    // TASK: distances in pool should be stored to speed-up this check
    let mut dist = dist_cab;
    unsafe {
    for i in 0..el.ord_numb as usize -1 {
        let o: Order = orders[el.ord_ids[i] as usize];
        if el.ord_actions[i] == 'i' as i8 && dist > o.wait as i16 {
            println!("WAIT: i:{}, ord_numb={} real={} > requested={}, id={}, order_id={}", 
                i, el.ord_numb, dist, o.wait, el.ord_ids[i], orders[el.ord_ids[i] as usize].id);
            for k in 0..el.ord_numb as usize {
                print!("{}{},", el.ord_ids[k], el.ord_actions[k])
            }
            println!("");
            return false;
        }
        let o2: Order = orders[el.ord_ids[i+1] as usize];
        let from = if el.ord_actions[i] == ('i' as i8) { o.from as usize } else { o.to as usize };
        let to = if el.ord_actions[i + 1] == 'i' as i8 { o2.from as usize } else { o2.to as usize};
        if from != to { 
            dist += DIST[from][to] + CNFG.stop_wait;
        }
    }}
    return true;
}
*/

// 1) get unassigned orders and free cabs, 
// 2) expire old orders
// 3) some orders and cabs are too distant, although som cabs may end their last legs soon
// TODO: cabs on last leg should be considered
fn prepare_data(conn: &mut PooledConn, max_assign_time: i64) -> Option<(Vec<Order>, Vec<Cab>)> {
    let mut orders = repo::find_orders_by_status_and_time(
                conn, OrderStatus::RECEIVED , (Local::now() - Duration::minutes(5)).naive_local());
    if orders.len() == 0 {
        info!("No demand");
        return None;
    }
    info!("Orders before expiry check, input: {}", orders.len());
    
    orders = expire_orders(conn, &orders, max_assign_time);
    if orders.len() == 0 {
        info!("No demand, expired");
        return None;
    }
    let mut cabs = find_free_cab_and_on_last_leg(conn); //repo::find_cab_by_status(conn, CabStatus::FREE);
    if orders.len() == 0 || cabs.len() == 0 {
        warn!("No cabs available");
        return None;
    }
    info!("Initial count, demand={}, supply={}", orders.len(), cabs.len());
    orders = get_rid_of_distant_customers(&orders, &cabs);
    if orders.len() == 0 {
      info!("No suitable demand, too distant");
      return None; 
    }
    cabs = get_rid_of_distant_cabs(&orders, &cabs);
    if cabs.len() == 0 {
      info!("No cabs available, too distant");
      return None; 
    }
    return Some((orders, cabs));
}

// TODO: bulk update
fn expire_orders(conn: &mut PooledConn, demand: &Vec<Order>, max_assign_time: i64) -> Vec<Order> {
    let mut ret: Vec<Order> = Vec::new();
    let mut ids: String = "".to_string();

    for o in demand.iter() {
      //if (o.getCustomer() == null) {
      //  continue; // TODO: how many such orders? the error comes from AddOrderAsync in API, update of Customer fails
      //}
        let minutes_rcvd = get_elapsed(o.received)/60;
        let minutes_at : i64 = get_elapsed(o.at_time);

        if (minutes_at == -1 && minutes_rcvd > max_assign_time)
                    || (minutes_at != -1 && minutes_at/60 > max_assign_time) {
            ids = ids + &o.id.to_string() + &",".to_string();
        } else {
            ret.push(*o);
        }
    }
    if ids.len() > 0 {
        let sql = ids[0..ids.len() - 1].to_string(); // remove last comma
        conn.query_iter("UPDATE taxi_order SET status=6 WHERE id IN (".to_string() + &sql + ")").unwrap();
        debug!("{} refused, max assignment time exceeded", &ids);
    }
    return ret;
}

// if we find just one cab nearby - continue with this order
fn get_rid_of_distant_customers(demand: &Vec<Order>, supply: &Vec<Cab>) -> Vec<Order> {
    let mut ret: Vec<Order> = Vec::new();
    for o in demand.iter() {
      for c in supply.iter() {
        unsafe {
            if distance::DIST[c.location as usize][o.from as usize] as i32 <= o.wait { 
                // great, we have at least one cab in range for this customer
                ret.push(*o);
                break;
            }
        }
      }
    }
    return ret;
}

fn get_rid_of_distant_cabs(demand: &Vec<Order>, supply: &Vec<Cab>) -> Vec<Cab> {
    let mut ret: Vec<Cab> = Vec::new();
    for c in supply.iter() {
        for o in demand.iter() {
            unsafe {
                if distance::DIST[c.location as usize][o.from as usize] as i32 <= o.wait {
                    // great, we have at least one customer in range for this cab
                    ret.push(*c);
                    break;
                }
            }
        }
    }
    return ret;
}

// cargo test -- --test-threads=1
#[cfg(test)]
mod tests {
  use std::vec;
  use rand::Rng;
  use super::*;
  //use chrono::format::InternalNumeric;
  use serial_test::serial;
  use crate::distance::init_distance;
  use distance::DIST;
  use solver::{relocate_free_cabs_glpk, lcm_slow};

  fn test_orders_invalid() -> Vec<Order> {
    return vec![
        Order{ id: 1, from: 1, to: 2, wait: 10, loss: 50, dist: 2, 
            shared: true, received: None,at_time: None, route_id: -1},
        Order{ id: -1, from: 1, to: 2, wait: 10, loss: 50, dist: 2, 
            shared: true, received: None,at_time: None,route_id: -1}
    ];
  }

  fn test_orders() -> Vec<Order> {
    return vec![
        Order{ id: 0, from: 0, to: 1, wait: 10, loss: 50, dist: 2, 
            shared: true, received: None,at_time: None, route_id: -1},
        Order{ id: 1, from: 1, to: 2, wait: 10, loss: 50, dist: 2, 
            shared: true, received: None,at_time: None, route_id: -1}
    ];
  }

  fn test_cabs() -> Vec<Cab> {
    return vec![
        Cab{ id: 0, location: 2, seats: 10, dist: 0},
        Cab{ id: 1, location: 3, seats: 10, dist: 0}
    ];
  }

  fn test_cabs_invalid() -> Vec<Cab> {
    return vec![
        Cab{ id: 1, location: 0, seats: 10, dist: 0},
        Cab{ id: -1, location: 1, seats: 10, dist: 0}
    ];
  }

  fn test_stops() -> Vec<Stop> {
    return vec![
      Stop{ id: 0, bearing: 0, latitude: 1.0, longitude: 1.0, capacity: 10},
      Stop{ id: 1, bearing: 0, latitude: 1.000000001, longitude: 1.000000001, capacity: 10},
      Stop{ id: 2, bearing: 0, latitude: 1.000000002, longitude: 1.000000002, capacity: 10},
      Stop{ id: 3, bearing: 0, latitude: 1.000000003, longitude: 1.000000003, capacity: 10},
      Stop{ id: 4, bearing: 0, latitude: 1.000000004, longitude: 1.000000004, capacity: 10},
      Stop{ id: 5, bearing: 0, latitude: 1.000000005, longitude: 1.000000005, capacity: 10}
    ];
  }

  #[test]
  #[serial]
  fn test_shrink() {
    let orders: Vec<Order> = test_orders_invalid();
    let cabs: Vec<Cab> = test_cabs_invalid();
    assert_eq!(cabs.len(), 2);
    assert_eq!(orders.len(), 2);
    let ret = shrink(&cabs, orders);
    assert_eq!(ret.0.len(), 1);
    assert_eq!(ret.1.len(), 1);
  }

  #[test]
  #[serial]
  fn test_munkres() {
    let orders: Vec<Order> = test_orders_invalid();
    let cabs: Vec<Cab> = test_cabs_invalid();
    let ret = munkres(&cabs, &orders);
    assert_eq!(ret.len(), 2);
  }

  #[test]
  #[serial]
  fn test_get_rid_of_distant_cabs() {
    let orders: Vec<Order> = test_orders_invalid();
    let cabs: Vec<Cab> = test_cabs_invalid();
    let ret = get_rid_of_distant_cabs(&orders, &cabs);
    assert_eq!(ret.len(), 2); // not distant
  }

  #[test]
  #[serial]
  fn test_get_rid_of_distant_orders() {
    let orders: Vec<Order> = test_orders_invalid();
    let cabs: Vec<Cab> = test_cabs_invalid();
    let ret = get_rid_of_distant_customers(&orders, &cabs);
    assert_eq!(ret.len(), 2); // not distant
  }

  #[test]
  #[serial]
  fn test_find_extern_pool() {
    let mut orders: Vec<Order> = test_orders();
    let mut cabs: Vec<Cab> = test_cabs();
    let stops = test_stops();
    init_distance(&stops, 30);
    unsafe { initMem(); }
    let ret = find_external_pool(&mut orders, &mut cabs, &stops, 1_i32,
                                                         &mut 0, &mut 0, KernCfg::new());
    unsafe { freeMem(); }
    assert_eq!(ret.0.len(), 1); 
    /*assert_eq!(ret.1, 
        "UPDATE cab SET status=0 WHERE id=0;\n\
        INSERT INTO route (id, status, cab_id) VALUES (0,1,0);\n\
        INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (0,2,1,0,0,1,8,0,0);\n\
        INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (1,1,2,1,0,1,2,0,1);\n\
        UPDATE taxi_order SET route_id=0, leg_id=1, cab_id=0, status=1, eta=0, in_pool=true WHERE id=1 AND status=0;\n\
        INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (2,2,0,2,0,1,8,0,0);\n\
        INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (3,0,1,3,0,1,2,0,1);\n\
        UPDATE taxi_order SET route_id=0, leg_id=3, cab_id=0, status=1, eta=0, in_pool=true WHERE id=0 AND status=0;\n"); 
        */
  }

  fn get_stops(step: f64, size: usize) -> Vec<Stop> {
    return get_stops_cap(step, size, 9, 10);
  }

  fn get_stops_cap(step: f64, size: usize, cap_from: i16, cap_to: i16) -> Vec<Stop> {
    let mut stops: Vec<Stop> = vec![];
    let mut c: i64 = 0;
    for i in 0..size {
      for j in 0..size {
        let cap = rand::thread_rng().gen_range(cap_from..cap_to);
        stops.push(
          Stop{ id: c, bearing: 0, latitude: 49.0 + step * i as f64, longitude: 19.000 + step * j as f64, capacity: cap}
        );
        c = c + 1;
      }
    }
    return stops;
  }

  fn get_orders(size: usize, stops: i32) -> Vec<Order> {
    let mut ret: Vec<Order> = vec![];
    for i in 0..size as i32 {     
        let from: i32 = i % stops;
        let to: i32 = if from + 5 >= stops { from - 5} else { from + 5} ;
        let dista = unsafe { DIST[from as usize][to as usize] as i32 };
        ret.push(Order{ id: i as i64, from, to, wait: 15, loss: 70, dist: dista, 
                    shared: true, received: Some(Local::now().naive_local()), at_time: None, 
                    route_id: -1 });
    }
    return ret;
  }

  fn get_cabs(size: usize) -> Vec<Cab> {
    let mut ret: Vec<Cab> = vec![];
    for i in 0..size {
        ret.push(Cab{ id: i as i64, location: (i % 2400) as i32, seats: 10, dist: 0});
    }
    return ret;
  }

  #[test]
  #[serial]
  fn test_performance_find_extern_pool() {
    let stops = get_stops(0.03, 49);
    init_distance(&stops, 30);
    let mut orders: Vec<Order> = get_orders(60, 49);
    //for o in &orders {
    //    println!("id: {}, from: {}, to: {}, dist: {}", o.id, o.from, o.to, o.dist);
    //}

    let mut cabs: Vec<Cab> = get_cabs(1000);
    unsafe { initMem(); }
    let start = Instant::now();
    let ret = find_external_pool(&mut orders, &mut cabs, &stops, 8_i32, 
                                                        &mut 0, &mut 0, KernCfg::new());
    let elapsed = start.elapsed();
    unsafe { freeMem(); }
    println!("Elapsed: {:?}", elapsed); 
    println!("Len: {}", ret.1.len()); 
    assert_eq!(ret.0.len(), 15); 
    assert_eq!(ret.1.len(), 19366); // TODO: Rust gives 18508
  }

  #[test]
  #[serial]
  fn test_performance_find_intern_pool() {
    let stops = get_stops(0.03, 49);
    init_distance(&stops, 30);
    let mut orders: Vec<Order> = get_orders(60, 49);
    //for o in &orders {
    //    println!("id: {}, from: {}, to: {}, dist: {}", o.id, o.from, o.to, o.dist);
    //}
    let mut cabs: Vec<Cab> = get_cabs(1000);
    let start = Instant::now();
    let ret = find_internal_pool(&mut orders, &mut cabs, &stops, 
                                                    &mut 0, &mut 0, KernCfg::new());
    let elapsed = start.elapsed();
    println!("Elapsed: {:?}", elapsed); 
    assert_eq!(ret.0.len(), 15); 
    assert_eq!(ret.1.len(), 19748);
  }

  #[test]
  #[serial]
  fn test_performance_find_intern_pool_4in() {
    let mut max_route_id : i64 = 0;
    let mut max_leg_id : i64 = 0;
    let stops = get_stops(0.05, 49);
    init_distance(&stops, 30);
    let mut demand: Vec<Order> = get_orders(60, 49);
    let mut cabs = get_cabs(1000);
    unsafe { initMem(); }
    let start = Instant::now();
    let cfg = KernCfg::new();
    let elapsed = start.elapsed();
    let ret = find_pool(4, 8, &mut demand,  &mut cabs, &stops, 
                                                &mut max_route_id, &mut max_leg_id, 
                                                cfg.max_angle, cfg.max_angle_dist, cfg.stop_wait);
                                                
    unsafe { freeMem(); }
    println!("Elapsed: {:?}", elapsed); 
    assert_eq!(ret.0.len() > 0, true); 
  }


  #[test]
  #[serial]
  fn test_performance_relocate_cabs() {
    let mut max_route_id : i64 = 0;
    let mut max_leg_id : i64 = 0;
    let stops = get_stops_cap(0.0008, 49, 0, 2);
    init_distance(&stops, 30);
    let cabs = get_cabs(1000);
    let sql = relocate_free_cabs(&cabs, &stops, &mut max_route_id, &mut max_leg_id);
    assert_eq!(sql.len() > 0, true); 
    let sql = relocate_free_cabs_glpk(&cabs, &stops, &mut max_route_id, &mut max_leg_id);
    assert_eq!(sql.len() > 0, true); 
  }

  #[test]
  #[serial]
  fn test_performance_relocate_cabs_glpk() {
    let mut max_route_id : i64 = 0;
    let mut max_leg_id : i64 = 0;
    let stops = get_stops_cap(0.008, 49, 0, 2);
    init_distance(&stops, 30);
    let cabs = get_cabs(1000);
    let sql = relocate_free_cabs_glpk(&cabs, &stops, &mut max_route_id, &mut max_leg_id);
    assert_eq!(sql.len() > 0, true); 
  }

  /*
  #[test]  
  #[serial]
  fn test_performance_find_extern_pool5() {
    let stops = get_stops(0.01, 49);
    init_distance(&stops, 30);
    let mut orders: Vec<Order> = get_orders(10, 49);
    let mut cabs: Vec<Cab> = get_cabs(1000);
    unsafe { initMem(); }
    let start = Instant::now();
    let ret = find_external_pool(&mut orders, &mut cabs, &stops, 8_i32, 
                                                        &mut 0, &mut 0, KernCfg::new());
    let elapsed = start.elapsed();
    unsafe { freeMem(); }
    println!("Elapsed: {:?}", elapsed); 
    assert_eq!(ret.0.len(), 2); 
    assert_eq!(ret.1.len(), 3322);
  }
  */

  #[test]  
  #[serial]
  fn test_performance_lcm_c() {
    let stops = get_stops(0.05, 49);
    init_distance(&stops, 30);
    let orders: Vec<Order> = get_orders(1900, 49);
    let cabs: Vec<Cab> = get_cabs(2000);
    let start = Instant::now();
    let cfg = KernCfg::new();
    let ret2 = extern_lcm(&cabs, &orders, 1000, cfg);
    println!("Elapsed: {}", start.elapsed().as_millis());
    assert_eq!(ret2.len(), 98);
    // assert_eq!(ret2[0].0, 901);
    // assert_eq!(ret2[0].1, 1499);
    // assert_eq!(ret2[1].0, 902);
    // assert_eq!(ret2[1].1, 1498);
  } 

  #[test]  
  fn test_performance_lcm() {
    let stops = get_stops(0.05, 49);
    init_distance(&stops, 30);
    let mut orders: Vec<Order> = get_orders(1900, 49);
    let mut cabs: Vec<Cab> = get_cabs(2000);
    let start = Instant::now();
    let ret2 = lcm_slow(&mut cabs, &mut orders, 1000);
    println!("Elapsed: {}", start.elapsed().as_millis());
    assert_eq!(ret2.len(), 98);
  } 

  #[test]
  fn a() {
    let mut cabs: Vec<i64> = vec![];
    cabs.push(1);
    cabs.push(2);
    for i in &mut cabs {
        if *i == 1 { *i = -1; }
    }
    print!("{:?}", cabs[0]);
  }
}
