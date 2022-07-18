use std::{thread};
use postgres::{Client, NoTls, Error};
use crate::model::{Order,Stop,Cab,Branch,MAXSTOPSNUMB,MAXCABSNUMB,MAXORDERSNUMB};
use crate::distance::{DIST};
use crate::extender::{max_angle};
use crate::repo::{assignPoolToCab};

static mut STOPS : [Stop; MAXSTOPSNUMB] = [Stop {id: 0, latitude: 0.0, longitude: 0.0, bearing: 0}; MAXSTOPSNUMB];
static mut STOPS_LEN: usize = 0;

static mut ORDERS: [Order; MAXORDERSNUMB] = [Order {
    id: 0, from: 0, to: 0, wait: 0,	loss: 0, dist: 0, shared: true, in_pool: true, 
    received: None, started: None, completed: None, at_time: None, eta: -1 }; MAXORDERSNUMB];
static mut ORDERS_LEN: usize = 0;

static mut CABS: [Cab; MAXCABSNUMB] = [Cab {id:0, location:0}; MAXCABSNUMB];
static mut CABS_LEN: usize = 0;

pub fn find_pool(in_pool: u8, threads: i16, demand: &Vec<Order>, supply: &mut Vec<Cab>,
                stands: &Vec<Stop>, mut max_route_id: &mut i32, max_leg_id: &mut i32) 
                -> (Vec<Branch>, thread::JoinHandle<()>) {
    unsafe {
        ORDERS = orders_to_array(demand);
        CABS = cabs_to_array(supply);
        STOPS = stops_to_array(stands);
        ORDERS_LEN = demand.len();
        CABS_LEN = supply.len();
        STOPS_LEN = stands.len();
    
        if ORDERS_LEN == 0 || CABS_LEN == 0 || STOPS_LEN == 0 {
            return (Vec::new(), thread::spawn(|| { }));
        }
    }
	let mut root = dive(0, in_pool, threads);
    //for (int i = 0; i < inPool + inPool - 1; i++)
    //    printf("node[%d].size: %d\n", i, countNodeSize(i));
    let ret = rm_final_duplicates(
            in_pool as usize, &mut root, &mut max_route_id, max_leg_id, supply);
    println!("FINAL: inPool: {}, found pools: {}\n", in_pool, ret.0.len());
    return ret;
}

fn dive(lev: u8, in_pool: u8, threads_numb: i16) -> Vec<Branch> {
	if lev > in_pool + in_pool - 3 { // lev >= 2*inPool-2, where -2 are last two levels
		return store_leaves();
		// last two levels are "leaves"
	}
	let mut t_numb = threads_numb;
	let prev_node = dive(lev + 1, in_pool, t_numb);
	
	let mut node : Vec<Branch> = Vec::with_capacity(3000000);
	let mut children = vec![];
	unsafe {
	let chunk: f32 = ORDERS_LEN as f32 / t_numb as f32;
	if ((t_numb as f32 * chunk).round() as i16) < ORDERS_LEN as i16 { t_numb += 1; } // last thread will be the reminder of division
	
    for i in 0..t_numb { // TASK: allocated orders might be spread unevenly -> count non-allocated and devide chunks ... evenly
		let node = prev_node.to_vec(); //clone();
		children.push(thread::spawn(move || {
			iterate(lev as usize, in_pool, i, chunk, &node)
        }));
    }
	for handle in children {
		let mut cpy : Vec<Branch> = handle.join().unwrap().to_vec();
		node.append(&mut cpy);
    }
	println!("Level: {}, size: {}", lev, node.len());
	}
	return node;
}

fn store_leaves() -> Vec<Branch> {
	let mut ret : Vec<Branch> = Vec::new();
	unsafe{
	for c in 0..ORDERS_LEN {
	  if ORDERS[c].id != -1 {
		for d in 0..ORDERS_LEN {
		  if ORDERS[d].id != -1 { // not allocated in previous search: inPool+1 (e.g. in_pool=4 and now we search in_pool=3)
		 	// to situations: <1in, 1out>, <1out, 2out>
		 	if c == d  {
			// IN and OUT of the same passenger, we don't check bearing as they are probably distant stops
		 		ret.push(add_branch(c as i32, d as i32, 'i', 'o', 1));
		 	} else if (DIST[ORDERS[c].to as usize][ORDERS[d].to as usize] as f32)
				< DIST[ORDERS[d].from as usize][ORDERS[d].to as usize] as f32
					* (100.0 + ORDERS[d].loss as f32) / 100.0
		 			&& bearing_diff(STOPS[ORDERS[c].to as usize].bearing, STOPS[ORDERS[d].to as usize].bearing) < max_angle as f32 {
		 		// TASK - this calculation above should be replaced by a redundant value in taxi_order - distance * loss
		 		ret.push(add_branch(c as i32, d as i32, 'o', 'o', 2));
		 	}
		  }
		}
	  }
	}
	}
	return ret;
}

fn bearing_diff(a: i16, b: i16 ) -> f32 {
    let mut r = (a as f32 - b as f32) % 360.0;
    if r < -180.0 {
      r += 360.0;
    } else if r >= 180.0 {
      r -= 360.0;
    }
    return r.abs();
}

fn add_branch(id1: i32, id2: i32, dir1: char, dir2: char, outs: u8) -> Branch {
    let mut br : Branch = Branch::new();
    if id1 < id2 || (id1==id2 && dir1 == 'i') {
		//br.key = sprintf!("%d%c%d%c", id1, dir1, id2, dir2).unwrap();
        br.ordIDsSorted[0] = id1;
        br.ordIDsSorted[1] = id2;
        br.ordActionsSorted[0] = dir1 as i8;
        br.ordActionsSorted[1] = dir2 as i8;
    }
    else if id1 > id2 || id1 == id2 {
        br.ordIDsSorted[0] = id2;
        br.ordIDsSorted[1] = id1;
        br.ordActionsSorted[0] = dir2 as i8;
        br.ordActionsSorted[1] = dir1 as i8;
    }
	unsafe {
    	br.cost = DIST[ORDERS[id1 as usize].to as usize][ORDERS[id2 as usize].to as usize];
	}
    br.outs = outs;
    br.ordIDs[0] = id1;
    br.ordIDs[1] = id2;
    br.ordActions[0] = dir1 as i8;
    br.ordActions[1] = dir2 as i8;
    br.ordNumb = 2;
	return br;
}

fn iterate(lev: usize, in_pool: u8, start: i16, size: f32, prev_node: &Vec<Branch>) -> Vec<Branch> {
	let ret: &mut Vec<Branch> = &mut Vec::with_capacity(100000);
	let next = ((start + 1) as f32 * size).round() as i32;
	unsafe{
	let stop: i32 = if next > ORDERS_LEN as i32 { ORDERS.len() as i32 } else { next };
	for ord_id in (start as f32 * size).round() as i32 .. stop {
		if ORDERS[ord_id as usize].id != -1 { // not allocated in previous search (inPool+1)
			for b in prev_node.iter() {
				if b.cost != -1 {  
					// we iterate over product of the stage further in the tree: +1
					store_branch_if_not_found(lev as u8, in_pool, ord_id, &b, ret);
				}
			}
		}
	}
	return ret.to_vec();
	}
}

// storeBranchIfNotFoundDeeperAndNotTooLong
// br is existing Branch in lev+1
fn store_branch_if_not_found(lev: u8, in_pool: u8, ord_id: i32, br: &Branch, ret: &mut Vec<Branch>) {
    // two situations: c IN and c OUT
    // c IN has to have c OUT in level+1, and c IN cannot exist in level + 1
    // c OUT cannot have c OUT in level +1
    let mut in_found : bool = false;
    let mut out_found : bool = false;
    for i in 0 .. (br.ordNumb as usize) {
      if br.ordIDs[i] == ord_id {
        if br.ordActions[i] == 'i' as i8 {
          in_found = true;
        } else {
          out_found = true;
        }
        // current passenger is in the branch below
      }
    }
    // now checking if anyone in the branch does not lose too much with the pool
    // c IN
	unsafe {
    let next_stop: usize = if br.ordActions[0] == 'i' as i8
                    	{ ORDERS[br.ordIDs[0] as usize].from as usize } 
						else { ORDERS[br.ordIDs[0] as usize].to as usize };
	let id = ord_id as usize;
	
    if !in_found
        && out_found
        && !is_too_long(DIST[ORDERS[id].from as usize][next_stop], br)
        // TASK? if the next stop is OUT of passenger 'c' - we might allow bigger angle
        && bearing_diff(STOPS[ORDERS[id].from as usize].bearing, STOPS[next_stop].bearing) < max_angle as f32
        { ret.push(store_branch('i', lev, ord_id, br, in_pool)); 
		}
    // c OUT
    if lev > 0 // the first stop cannot be OUT
        && br.outs < in_pool // numb OUT must be numb IN
        && !out_found // there is no such OUT later on
        && !is_too_long(DIST[ORDERS[id].to as usize][next_stop], br)
        && bearing_diff(STOPS[ORDERS[id].to as usize].bearing, STOPS[next_stop].bearing) < max_angle as f32
        { ret.push(store_branch('o', lev, ord_id, br, in_pool)); 
		}
	}
}

fn is_too_long(dist: i16, br: &Branch) -> bool {
	unsafe {
	let mut wait = dist;
    for i in 0..(br.ordNumb as usize) {
		let id = br.ordIDs[i] as usize;
        if wait as f32 >  //distance[orders[br.ordIDs[i]].fromStand][orders[br.ordIDs[i]].toStand] 
            ORDERS[id].dist as f32 * (100.0 + ORDERS[id].loss as f32) / 100.0 { return true; }
        if br.ordActions[i] == 'i' as i8 && wait > ORDERS[id].wait as i16 { return true; }
		
        if i + 1 < br.ordNumb as usize {
            wait += DIST[if br.ordActions[i] == 'i' as i8 { ORDERS[id].from as usize} 
							 else { ORDERS[id].to as usize }] 
							[if br.ordActions[i + 1] == 'i' as i8 { ORDERS[br.ordIDs[i + 1] as usize].from as usize }
							 else { ORDERS[br.ordIDs[i + 1] as usize].to as usize } ];
		}
    }
    return false;
	}
}

// b is existing Branch in lev+1
fn store_branch(action: char, lev: u8, ord_id: i32, b: &Branch, in_pool: u8) -> Branch  {
	let mut br : Branch = Branch::new();
	//br.key = "".to_string();

    br.ordNumb = (in_pool + in_pool - lev) as i16;
    br.ordIDs[0] = ord_id;
    br.ordActions[0] = action as i8;
    br.ordIDsSorted[0] = ord_id;
    br.ordActionsSorted[0] = action as i8;
    
    for j in 0.. (br.ordNumb as usize - 1) { // further stage has one passenger less: -1
      br.ordIDs[j + 1]      = b.ordIDs[j];
      br.ordActions[j + 1]  = b.ordActions[j];
      br.ordIDsSorted[j + 1]= b.ordIDs[j];
      br.ordActionsSorted[j + 1] = b.ordActions[j];
    }
	unsafe {
    br.cost = DIST[if action == 'i' { ORDERS[ord_id as usize].from as usize} 
						else { ORDERS[ord_id as usize].to as usize }]
                      [if b.ordActions[0] == 'i' as i8 { ORDERS[b.ordIDs[0] as usize].from as usize} 
					   else { ORDERS[b.ordIDs[0]as usize].to as usize} ] + b.cost;
	}
    br.outs = if action == 'o' { b.outs + 1 } else { b.outs };
    return br;
}

fn rm_final_duplicates(in_pool: usize, arr: &mut Vec<Branch>, mut max_route_id: &mut i32, 
                     mut max_leg_id: &mut i32, cabs: &mut Vec<Cab>) -> (Vec<Branch>, thread::JoinHandle<()>) {
	let mut ret : Vec<Branch> = Vec::new();
  let mut sql: String = String::from("");

  if arr.len() == 0 {
        return (ret, thread::spawn(|| { }));
  }
  arr.sort_by_key(|e| e.cost.clone());
    // removing duplicates
  unsafe {
    for i in 0..arr.len() {
        if arr[i].cost == -1 { // this -1 marker is set below
            continue;
        }
        // find nearest cab to first pickup and check if WAIT and LOSS constraints met - allocate
        let cabIdx = findNearestCab(arr[i].ordIDs[0]); // LCM
        if cabIdx == -1 { // no more cabs
            markPoolsAsDead(arr, i);
            break;
        }
        //Cab cab = supply[cabIdx];
        let distCab = DIST[CABS[cabIdx as usize].location as usize]
                          [ORDERS[arr[i].ordIDs[0] as usize].from as usize];
        if distCab == 0 // constraints inside pool are checked while "diving"
                || constraintsMet(arr[i], distCab as i32) {
            ret.push(arr[i]);
            // allocate
            sql += &assignAndRemove(arr, in_pool, i, cabIdx as usize, &mut max_route_id, &mut max_leg_id);
            // remove the cab from list so that it cannot be allocated twice in LCM or Munkres
            cabs[cabIdx as usize].id = -1;
        } else { // constraints not met, mark as unusable
            arr[i].cost = -1;
        }
    }
  }
  // RUN SQL
  let handle: thread::JoinHandle<_> = thread::spawn(move || {
    match Client::connect("postgresql://kabina:kaboot@localhost/kabina", NoTls)
    {
      Ok(mut c) => {
        c.batch_execute(&sql);
      }
      Err(err) => {
          panic!("Pool could not connect DB");
      }
    }
  });
  return (ret, handle);
}

fn assignAndRemove(arr: &mut Vec<Branch>, inPool: usize, i: usize, cabIdx: usize,
                    mut max_route_id: &mut i32, mut max_leg_id: &mut i32) -> String {
    // remove any further duplicates
    for j in i + 1 .. arr.len() {
        if arr[j].cost != -1 // not invalidated; this check is for performance reasons
                && isFoundV2(arr, i, j, inPool) {
            arr[j].cost = -1; // duplicated; we remove an element with greater costs
            // (list is pre-sorted)
        }
    }
    unsafe {
    return assignPoolToCab(CABS[cabIdx], &ORDERS, arr[i], &mut max_route_id, &mut max_leg_id);
    }
}

fn isFoundV2(arr: &Vec<Branch>, i: usize, j: usize, custInPool: usize) -> bool {
    for x in 0..custInPool + custInPool - 1 { // -1 the last is OUT
      if arr[i].ordActions[x] == 'i' as i8 {
        for y in 0..custInPool + custInPool - 1 {
          if arr[j].ordActions[y] == 'i' as i8 && arr[j].ordIDs[y] == arr[i].ordIDs[x] {
            return true;
          }
        }
      }
    }
    return false;
}

fn markPoolsAsDead(arr: &mut Vec<Branch>, i: usize) {
    for j in i+1 ..arr.len() {
      arr[j].cost = -1;
    }
}

fn findNearestCab(o_idx: i32) -> i32 {
    unsafe{
    let o: Order = ORDERS[o_idx as usize];
    let mut dist = 10000; // big
    let mut nearest = -1 as i32;
    for i in 0 .. CABS_LEN {
      let c: Cab = CABS[i]; 
      if c.id == -1 { // allocated earlier to a pool
        continue;
      }
      unsafe {
      if DIST[c.location as usize][o.from as usize] < dist {
        dist = DIST[c.location as usize][o.from as usize];
        nearest = i as i32;
      }}
    }
    return nearest;
    }
}

fn constraintsMet(el: Branch, distCab: i32) -> bool {
    // TASK: distances in pool should be stored to speed-up this check
    let mut dist = 0;
    unsafe {
    for i in 0..el.ordNumb as usize {
      let o: Order = ORDERS[el.ordIDs[i] as usize];
      if el.ordActions[i] == 'i' as i8 && dist + distCab > o.wait {
        return false;
      }
      if el.ordActions[i] == 'o' as i8 && dist as f32 > (1.0 + o.loss as f32 / 100.0) * o.dist as f32 {
        // TASK: remove this calculation above, it should be stored
        return false;
      }
      let o2: Order = ORDERS[el.ordIDs[i+1] as usize];
      if i < el.ordNumb as usize - 1 {
        dist += DIST[ if el.ordActions[i] == ('i' as i8) { o.from as usize } else { o.to as usize }]
                    [ if el.ordActions[i + 1] == 'i' as i8 { o2.from as usize } else { o2.to as usize}] as i32;
      }
    }
    }
    return true;
}


pub fn orders_to_array(vec: &Vec<Order>) -> [Order; MAXORDERSNUMB] {
    let mut arr : [Order; MAXORDERSNUMB] = [Order {
        id: 0, from: 0, to: 0, wait: 0,	loss: 0, dist: 0, shared: true, in_pool: true, 
        received: None, started: None, completed: None, at_time: None, eta: -1 }; MAXORDERSNUMB];
    for (i,v) in vec.iter().enumerate() { arr[i] = *v; }
    return arr;
}

pub fn cabs_to_array(vec: &Vec<Cab>) -> [Cab; MAXCABSNUMB] {
    let mut arr : [Cab; MAXCABSNUMB] = [Cab {id: 0, location: 0}; MAXCABSNUMB];
    for (i,v) in vec.iter().enumerate() { arr[i] = *v; }
    return arr;
}

pub fn stops_to_array(vec: &Vec<Stop>) -> [Stop; MAXSTOPSNUMB] {
    let mut arr : [Stop; MAXSTOPSNUMB] = [Stop {id: 0, bearing: 0, longitude:0.0, latitude: 0.0}; MAXSTOPSNUMB];
    for (i,v) in vec.iter().enumerate() { arr[i] = *v; }
    return arr;
}
