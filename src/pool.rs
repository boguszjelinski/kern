/// Kabina minibus/taxi dispatcher
/// Copyright (c) 2024 by Bogusz Jelinski bogusz.jelinski@gmail.com
/// 
/// Pool finder submodule.
/// A pool is a group of orders to be picked up by a cab in a prescribed sequence
/// 'Branch' structure describes one such group (saved as route in the database)
/// 
use std::thread;
use log::debug;
use crate::model::{Order,OrderTransfer,Stop,Cab,Branch,MAXSTOPSNUMB,MAXCABSNUMB,MAXORDERSNUMB};
use crate::distance::DIST;
use crate::repo::{assign_pool_to_cab, CNFG};

static mut STOPS : [Stop; MAXSTOPSNUMB] = [Stop {id: 0, latitude: 0.0, longitude: 0.0, bearing: 0}; MAXSTOPSNUMB];
static mut STOPS_LEN: usize = 0;

static mut ORDERS: [Order; MAXORDERSNUMB] = [Order {
    id: 0, from: 0, to: 0, wait: 0,	loss: 0, dist: 0, shared: true, in_pool: true, 
    received: None, started: None, completed: None, at_time: None, eta: -1, route_id: -1 }; MAXORDERSNUMB];
static mut ORDERS_LEN: usize = 0;

static mut CABS: [Cab; MAXCABSNUMB] = [Cab {id:0, location:0, seats: 0}; MAXCABSNUMB];
static mut CABS_LEN: usize = 0;

pub const NODE_INIT_SIZE:usize = 3000000;
pub const NODE_THREAD_INIT_SIZE:usize = 100000;

/// Returns a list of pools sorted by total trip length (sorting helps filter out worse plans)
/// in_pool: how many passengers
/// threads: how many
/// demand: orders (mutable: some marked as allocated)
/// supply: cabs with their location; TODO: busy cabs on their last leg (mutable: some marked as allocated)
/// stands: stops
/// max_route_id: primary key available (not used) for routes
/// max_leg_id:  primary key available (not used) for route legs
/// 
/// returns: vector of pools and SQL
pub fn find_pool(in_pool: u8, threads: i16, demand: &mut Vec<Order>, supply: &mut Vec<Cab>,
                stands: &Vec<Stop>, mut max_route_id: &mut i64, max_leg_id: &mut i64) 
                -> (Vec<Branch>, String) {
  unsafe {
      // static arrays are faster, I guess
      ORDERS = orders_to_array(demand);
      CABS = cabs_to_array(supply);
      STOPS = stops_to_array(stands);
      ORDERS_LEN = demand.len();
      CABS_LEN = supply.len();
      STOPS_LEN = stands.len();
  
      if ORDERS_LEN == 0 || CABS_LEN == 0 || STOPS_LEN == 0 {
          return (Vec::new(), String::from(""));
      }
  }
  // recursive dive until the leaves of the permutation tree
	let mut root = dive(0, in_pool, threads);

  // there might be pools with same passengers, with different length - sort and find the best ones
  let ret = rm_duplicates_assign_cab(
          in_pool as usize, &mut root, &mut max_route_id, max_leg_id, supply);

  // mark orders in pools as assigned so that next call to find_pool (with fewer 'in_pool') skips them
  for br in ret.0.iter() {
    for o in 0..br.ord_numb as usize {
      demand[br.ord_ids[o] as usize].id = -1;
    }
  }
  debug!("FINAL: inPool: {}, found pools: {}\n", in_pool, ret.0.len());
  return ret;
}

/// finding all feasible pools - sequences of passengers' pick-ups and drop-offs 
/// recursive dive in the permutation tree
/// level ZERO will have (in 'node' variable) all pickups and dropoffs, 
/// node ONE will miss the first IN marked with 'i' in 'ord_actions'
/// twice as much depths as passengers in pool (pickup and dropoff), 
/// minus leaves generated by a dedicated, simple routine  
/// 
/// lev: starting always with zero
/// in_pool: number of passengers going together
/// threads_numb: 
fn dive(lev: u8, in_pool: u8, threads_numb: i16) -> Vec<Branch> {
	if lev > in_pool + in_pool - 3 { // lev >= 2*inPool-2, where -2 are last two levels
		let ret = store_leaves();
    debug!("Level: {}, size: {}", lev, ret.len());
    return ret;
		// last two levels are "leaves"
	}
	// dive more
	let prev_node = dive(lev + 1, in_pool, threads_numb);

  let mut t_numb = threads_numb; // mut: there might be one more thread, rest of division
	
	let mut node : Vec<Branch> = Vec::with_capacity(NODE_INIT_SIZE);
	let mut children = vec![];
	unsafe {
    let chunk: f32 = ORDERS_LEN as f32 / t_numb as f32;
    if ((t_numb as f32 * chunk).round() as i16) < ORDERS_LEN as i16 { t_numb += 1; } // last thread will be the reminder of division
    
    // run the threads, each thread gets its own range of orders to iterate over - hence 'iterate'
    for i in 0..t_numb { // TASK: allocated orders might be spread unevenly -> count non-allocated and devide chunks ... evenly
      let node = prev_node.to_vec(); //clone();
      children.push(thread::spawn(move || {
        iterate(lev as usize, in_pool, i, chunk, &node)
      }));
    }

    // collect the data from threads, join their execution first
    // there might be 'duplicates', 1-2-3 and 1-3-2 and so on, they will be filtered out later
    for handle in children {
      let mut cpy : Vec<Branch> = handle.join().unwrap().to_vec();
      node.append(&mut cpy);
      }
    debug!("Level: {}, size: {}", lev, node.len()); // just for memory usage considerations
	}
	return node;
}

/// generate permutatations of leaves - last two stops (well, it might be one stop), we skip some checks here
/// just two nested loops
/// a leafe is e.g.: 1out-2out or 1in-1out, the last one must be OUT, 'o'
/// 
/// returns: leaves
fn store_leaves() -> Vec<Branch> {
	let mut ret : Vec<Branch> = Vec::new();
	unsafe{
	for c in 0..ORDERS_LEN {
	  if ORDERS[c].id != -1 { // not allocated in previous search: inPool+1 (e.g. in_pool=4 and now we search in_pool=3)
		for d in 0..ORDERS_LEN {
		  if ORDERS[d].id != -1 { 
		 	// to situations: <1in, 1out>, <1out, 2out>, the first here c==d, IN and OUT of the same passenger
		 	if c == d {
        // 'bearing' checks if stops are in line, it promotes straight paths to avoid unlife solutions
        // !! we might not check bearing here as they are probably distant stops
        if bearing_diff(STOPS[ORDERS[c].from as usize].bearing, STOPS[ORDERS[d].to as usize].bearing) < CNFG.max_angle as f32  {
		 		  ret.push(add_branch(c as i32, d as i32, 'i', 'o', 1));
        }
		 	} 
      // now <1out, 2out>
      else if (DIST[ORDERS[c].to as usize][ORDERS[d].to as usize] as f32)
				< DIST[ORDERS[d].from as usize][ORDERS[d].to as usize] as f32
					* (100.0 + ORDERS[d].loss as f32) / 100.0
		 			&& bearing_diff(STOPS[ORDERS[c].to as usize].bearing, STOPS[ORDERS[d].to as usize].bearing) < CNFG.max_angle as f32 {
		 		// TASK - this calculation above should be replaced by a redundant value in taxi_order - distance * loss
		 		ret.push(add_branch(c as i32, d as i32, 'o', 'o', 2));
        /*  debug!("c={} d={} c.id={} d.id={} c.to={} d.from={} d.to={} d.loss={} c.to.bearing={} d.to.bearing={} dist_c_d={} dist_d_d={}",
                  c, d, ORDERS[c].id, ORDERS[d].id, ORDERS[c].to, ORDERS[d].from, ORDERS[d].to,
                  ORDERS[d].loss, STOPS[ORDERS[c].to as usize].bearing, STOPS[ORDERS[d].to as usize].bearing,
                  DIST[ORDERS[c].to as usize][ORDERS[d].to as usize], DIST[ORDERS[d].from as usize][ORDERS[d].to as usize] 
                );
        */
		 	}
		  }
		}
	  }
	}
	}
	return ret;
}

pub fn bearing_diff(a: i32, b: i32 ) -> f32 {
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
	unsafe {
      let from = ORDERS[id1 as usize].to as usize;
      let to = ORDERS[id2 as usize].to as usize;
    	br.cost = DIST[from][to] + if from == to { 0 } else { CNFG.stop_wait };
	}
    br.outs = outs;
    br.ord_ids[0] = id1;
    br.ord_ids[1] = id2;
    br.ord_actions[0] = dir1 as i8;
    br.ord_actions[1] = dir2 as i8;
    br.ord_numb = 2;
	return br;
}

/// just a loop and calling store_branch...
/// returns: a chunk of all branches at that level
fn iterate(lev: usize, in_pool: u8, start: i16, size: f32, prev_node: &Vec<Branch>) -> Vec<Branch> {
	let ret: &mut Vec<Branch> = &mut Vec::with_capacity(NODE_THREAD_INIT_SIZE);
	let next = ((start + 1) as f32 * size).round() as i32;
	unsafe{
	let stop: i32 = if next > ORDERS_LEN as i32 { ORDERS.len() as i32 } else { next };
	for ord_id in (start as f32 * size).round() as i32 .. stop {
		if ORDERS[ord_id as usize].id != -1 { // not allocated in previous search (inPool+1)
			for b in prev_node.iter() {
				if b.cost != -1 {  // TODO: this check is probably not needed anymore, comes from duplicate check that we don't do anymore (time consuming, benefits did not cover the cost)
					// we iterate over product of the stage further in the tree: +1
					store_branch_if_not_found(lev as u8, in_pool, ord_id, &b, ret);
				}
			}
		}
	}
	return ret.to_vec();
	}
}

/// storeBranchIfNotFoundDeeperAndNotTooLong
/// check how an order fits into a pool
/// br is existing Branch in lev+1
/// returns: just pushes to a mutable vector
fn store_branch_if_not_found(lev: u8, in_pool: u8, ord_id: i32, br: &Branch, ret: &mut Vec<Branch>) {
  // two situations: c IN and c OUT
  // c IN has to have c OUT in level+1, and c IN cannot exist in level + 1
  // c OUT cannot have c OUT in level +1
  let mut out_found : bool = false;
  for i in 0 .. (br.ord_numb as usize) {
    if br.ord_ids[i] == ord_id {
      if br.ord_actions[i] == 'i' as i8 {
        //in_found = true; what was I thinking - if IN is found it must be also OUT, we cannot proceed
        return;
      } else {
        out_found = true;
        break; // we can break here as 'i' would have been found earlier
      }
    }
  }
  // now checking if anyone in the branch does not lose too much with the pool
  // c IN
	unsafe {
    let next_stop: usize = if br.ord_actions[0] == 'i' as i8
      { ORDERS[br.ord_ids[0] as usize].from as usize } else { ORDERS[br.ord_ids[0] as usize].to as usize };
	  let id = ord_id as usize;
	
    if out_found { // IN was not found, we would have called 'return'
      if !is_too_long('o', ord_id, DIST[ORDERS[id].from as usize][next_stop]
                        + if ORDERS[id].from == next_stop as i32 { 0 } else { CNFG.stop_wait }, br)
        // TASK? if the next stop is OUT of passenger 'c' - we might allow bigger angle
        && bearing_diff(STOPS[ORDERS[id].from as usize].bearing, STOPS[next_stop].bearing) < CNFG.max_angle as f32 { 

        ret.push(store_branch('i', lev, ord_id, br, in_pool)); 
      }
		} 
    // c OUT as neither IN or OUT was found
    else if lev > 0 // the first stop cannot be OUT
        && br.outs < in_pool // numb OUT must be numb IN
        && !is_too_long('o', ord_id, DIST[ORDERS[id].to as usize][next_stop]
                        + if ORDERS[id].to == next_stop as i32 { 0 } else { CNFG.stop_wait }, br)
        && bearing_diff(STOPS[ORDERS[id].to as usize].bearing, STOPS[next_stop].bearing) < CNFG.max_angle as f32 { 

      ret.push(store_branch('o', lev, ord_id, br, in_pool)); 
		}
	}
}

/// check 'max wait' and 'max loss' of all orders in the pool
/// dist: is the distance added to the pool
/// br: is the branch in level+1
/// returns if the order fits in
fn is_too_long(action: char, ord_id: i32, dist: i16, br: &Branch) -> bool {
	unsafe {
	  let mut wait = dist;
    // iterate over all previous orders, 
    for i in 0..(br.ord_numb as usize) -1 {
		  let id = br.ord_ids[i] as usize;
      if br.ord_actions[i] == 'o' as i8 && action == 'i' && ord_id == br.ord_ids[i] &&
        wait as f32 >  //distance[orders[br.ordIDs[i]].fromStand][orders[br.ordIDs[i]].toStand] 
          ORDERS[id].dist as f32 * (100.0 + ORDERS[id].loss as f32) / 100.0 { 
        // max loss of the new order (which we are trying to put in) is violated
        // max loss check of other orders have been checked earlier, here, in lev+1, of course only that with IN & OUT
        return true; 
      }
      if br.ord_actions[i] == 'i' as i8 && wait > ORDERS[id].wait as i16 { 
        // wait time of an already existing order (in the pool; lev+1) is violated
        return true; 
      }

      let from = if br.ord_actions[i] == 'i' as i8 { ORDERS[id].from as usize } 
                        else { ORDERS[id].to as usize };
      let to = if br.ord_actions[i + 1] == 'i' as i8 { ORDERS[br.ord_ids[i + 1] as usize].from as usize }
                      else { ORDERS[br.ord_ids[i + 1] as usize].to as usize };
      wait += DIST[from][to] + if from == to { 0 } else { CNFG.stop_wait };
    }
    // we have to repeat the check in the loop for the last element in array (max loss)
    if action == 'i' && ord_id == br.ord_ids[br.ord_numb as usize] &&
       wait as f32 > ORDERS[ord_id as usize].dist as f32 * (100.0 + ORDERS[ord_id as usize].loss as f32) / 100.0 { 
      return true; 
    }
    // no time constraint is violated
    return false;
	}
}

/// adding an order to a pool
///  b is existing Branch in lev+1
/// 
/// returns an extended pool
fn store_branch(action: char, lev: u8, ord_id: i32, b: &Branch, in_pool: u8) -> Branch  {
	let mut br : Branch = Branch::new();

  br.ord_numb = (in_pool + in_pool - lev) as i16;
  br.ord_ids[0] = ord_id;
  br.ord_actions[0] = action as i8;

  // make space for the new order - TODO: maybe we could have the last order at [0]? the other way round
  for j in 0.. (br.ord_numb as usize - 1) { // further stage has one passenger less: -1
    br.ord_ids[j + 1]     = b.ord_ids[j];
    br.ord_actions[j + 1] = b.ord_actions[j];
  }
	unsafe {
    let from = if action == 'i' { ORDERS[ord_id as usize].from as usize} 
                      else { ORDERS[ord_id as usize].to as usize };
    let to = if b.ord_actions[0] == 'i' as i8 { ORDERS[b.ord_ids[0] as usize].from as usize} 
                    else { ORDERS[b.ord_ids[0]as usize].to as usize};
    br.cost = DIST[from][to] + b.cost + if from == to { 0 } else { CNFG.stop_wait };
	}
  br.outs = if action == 'o' { b.outs + 1 } else { b.outs };
  return br;
}

/// there might be pools with same passengers (orders) but in different ... order (sequence of INs and OUTs) 
/// the list will be sorted by total length of the pool, worse pools with same passengers will be removed
/// cabs will be assigned with greedy method 
/// max_route_id: primary key available (not used) for routes
/// max_leg_id:  primary key available (not used) for route legs
/// 
/// returns allocated branches (to regenerate demand and supplu for the solver) and SQL to execute
fn rm_duplicates_assign_cab(in_pool: usize, arr: &mut Vec<Branch>, mut max_route_id: &mut i64, 
                       mut max_leg_id: &mut i64, cabs: &mut Vec<Cab>) -> (Vec<Branch>, String) {
	let mut ret : Vec<Branch> = Vec::new();
  let mut sql: String = String::from("");

  if arr.len() == 0 {
    return (ret, sql);
  }
  arr.sort_by_key(|e| e.cost.clone());
  
  // assigning and removing duplicates
  unsafe {
    for i in 0..arr.len() {
      if arr[i].cost == -1 { // this -1 marker is set below
        continue;
      }
      // find nearest cab to first pickup and check if WAIT and LOSS constraints met - allocate
      let cab_idx = find_nearest_cab(arr[i].ord_ids[0], count_passengers(arr[i])); // LCM
      if cab_idx == -1 { // no more cabs
        mark_pools_as_dead(arr, i);
        break;
      } else if cab_idx == -2 { // there is no cab for so many passengers
        arr[i].cost = -1;
        continue;
      }
      
      let dist_cab = DIST[CABS[cab_idx as usize].location as usize]
                              [ORDERS[arr[i].ord_ids[0] as usize].from as usize];
      if dist_cab == 0 // constraints inside pool are checked while "diving", and cab does not add up anything if == 0
              || constraints_met(arr[i], (dist_cab + CNFG.stop_wait) as i32 ) {
        ret.push(arr[i]);
        // assign to a cab and remove all next pools with these passengers (index 'i')
        sql += &assign_and_remove(arr, in_pool, i, cab_idx as usize, &mut max_route_id, &mut max_leg_id);
        // remove the cab from list so that it cannot be allocated twice in LCM or Munkres
        cabs[cab_idx as usize].id = -1;
        // the same with demand, but this static array ORDERS is a copy of Vec, so it is better to do it elsewhere
      } else { // constraints not met, mark as unusable
        arr[i].cost = -1;
      }
    }
  }
  return (ret, sql);
}

fn count_passengers(branch: Branch) -> i32 {
  let mut curr_count: i32 = 0;
  let mut max_count: i32 = 0;
  for i in 0 .. branch.ord_numb as usize {
    if branch.ord_actions[i] == 'i' as i8 {
      curr_count += 1;
      if curr_count > max_count {
        max_count = curr_count; // max_count++ would be the same; which one is faster?
      }
    } else { // 'o'
      curr_count -= 1;
    }
  }
  return max_count;
}

/// create a route with legs, assign orders to the cab (and legs, which is not that important)
/// remove all other pools with these passengers - 'i' index to arr
/// 
/// returns SQL
fn assign_and_remove(arr: &mut Vec<Branch>, in_pool: usize, i: usize, cab_idx: usize,
                     mut max_route_id: &mut i64, mut max_leg_id: &mut i64) -> String {
  // remove any further duplicates
  for j in i + 1 .. arr.len() {
      if arr[j].cost != -1 // not invalidated; this check is for performance reasons
              && is_found(arr, i, j, in_pool) {
          arr[j].cost = -1; // duplicated; we remove an element with greater costs
          // (list is pre-sorted)
      }
  }
  unsafe {
  return assign_pool_to_cab(CABS[cab_idx], &ORDERS, arr[i], &mut max_route_id, &mut max_leg_id);
  }
}

/// check if passengers in pool 'i' exist in pool 'j'
fn is_found(arr: &Vec<Branch>, i: usize, j: usize, cust_in_pool: usize) -> bool {
  for x in 0..cust_in_pool + cust_in_pool - 1 { // -1 the last is OUT
    if arr[i].ord_actions[x] == 'i' as i8 {
      for y in 0..cust_in_pool + cust_in_pool - 1 {
        if arr[j].ord_actions[y] == 'i' as i8 && arr[j].ord_ids[y] == arr[i].ord_ids[x] {
          return true;
        }
      }
    }
  }
  return false; // not found
}

/// mark all pools after 'i' as dead 
/// TODO: do we need this? there will be no cab for in_pool-1 anymore
fn mark_pools_as_dead(arr: &mut Vec<Branch>, i: usize) {
    for j in i+1 ..arr.len() {
      arr[j].cost = -1;
    }
}

/// LCM - find the nearest cab for this order ('from' of the first order in pool)
/// returns id of the cab
fn find_nearest_cab(o_idx: i32, pass_count: i32) -> i32 {
  unsafe{
    let o: Order = ORDERS[o_idx as usize];
    let mut dist = 10000; // big
    let mut nearest = -1 as i32;
    let mut found_any = false;
    for i in 0 .. CABS_LEN {
      let c: Cab = CABS[i]; 
      if c.id == -1 { // allocated earlier to a pool
        continue;
      }
      found_any = true;
      if DIST[c.location as usize][o.from as usize] < dist && c.seats <= pass_count {
        dist = DIST[c.location as usize][o.from as usize];
        nearest = i as i32;
      }
    }
    if !found_any { 
      return -1; // no cabs at all
    } else if nearest == -1 { // there are some cabs available but not with so many seats
      return -2;
    } 
    return nearest;
  }
}

/// checking max wait of all orders
///  
fn constraints_met(el: Branch, dist_cab: i32) -> bool {
    // TASK: distances in pool should be stored to speed-up this check
    let mut dist = dist_cab;
    unsafe {
    for i in 0..el.ord_numb as usize -1 {
      let o: Order = ORDERS[el.ord_ids[i] as usize];
      if el.ord_actions[i] == 'i' as i8 && dist > o.wait {
        return false;
      }
      let o2: Order = ORDERS[el.ord_ids[i+1] as usize];
      let from = if el.ord_actions[i] == ('i' as i8) { o.from as usize } else { o.to as usize };
      let to = if el.ord_actions[i + 1] == 'i' as i8 { o2.from as usize } else { o2.to as usize};
      if from != to {
        dist += (DIST[from][to] + CNFG.stop_wait) as i32;
      }
    }
    // we don't need to check the last leg as it does not concern "loss", this has been check earlier 
    }
    return true;
}

pub fn orders_to_array(vec: &Vec<Order>) -> [Order; MAXORDERSNUMB] {
  let mut arr : [Order; MAXORDERSNUMB] = [Order {
      id: 0, from: 0, to: 0, wait: 0,	loss: 0, dist: 0, shared: true,
      in_pool: true, received: None, started: None, completed: None, at_time: None, eta: 0, route_id: -1
    }; MAXORDERSNUMB];
  for (i, v) in vec.iter().enumerate() { 
    arr[i] = *v; 
  }
  return arr;
}

pub fn orders_to_transfer_array(vec: &Vec<Order>) -> [OrderTransfer; MAXORDERSNUMB] {
    let mut arr : [OrderTransfer; MAXORDERSNUMB] = [OrderTransfer {
        id: 0, from: 0, to: 0, wait: 0,	loss: 0, dist: 0}; MAXORDERSNUMB];
    for (i, v) in vec.iter().enumerate() { 
      arr[i].id = v.id; 
      arr[i].from = v.from; 
      arr[i].to = v.to; 
      arr[i].wait = v.wait; 
      arr[i].loss = v.loss; 
      arr[i].dist = v.dist; 
    }
    return arr;
}

pub fn cabs_to_array(vec: &Vec<Cab>) -> [Cab; MAXCABSNUMB] {
    let mut arr : [Cab; MAXCABSNUMB] = [Cab {id: 0, location: 0, seats: 0}; MAXCABSNUMB];
    for (i,v) in vec.iter().enumerate() { arr[i] = *v; }
    return arr;
}

pub fn stops_to_array(vec: &Vec<Stop>) -> [Stop; MAXSTOPSNUMB] {
    let mut arr : [Stop; MAXSTOPSNUMB] = [Stop {id: 0, bearing: 0, longitude:0.0, latitude: 0.0}; MAXSTOPSNUMB];
    for (i,v) in vec.iter().enumerate() { arr[i] = *v; }
    return arr;
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::distance::init_distance;
  use chrono::Local;
use serial_test::serial;
  use std::time::Instant;
  use std::time::SystemTime;

  fn get_stops() -> Vec<Stop> {
    let mut stops: Vec<Stop> = vec![];
    let mut c: i64 = 0;
    for i in 0..49 {
      for j in 0..49 {
        stops.push(
          Stop{ id: c, bearing: 0, latitude: 49.0 + 0.025 * i as f64, longitude: 19.000 + 0.025 * j as f64}
        );
        c = c + 1;
      }
    }
    return stops;
  }

  fn set_orders() {
    unsafe {
      for i in 0..10000 {     
        let from: i32 = i % 2400;
        let to: i32 = from + 1;
        ORDERS[i as usize] = Order{ id: i as i64, from, to, wait: 15, loss: 70, dist: DIST[from as usize][to as usize] as i32, 
          shared: true, in_pool: false, received: None, started: None, completed: None, at_time: None, 
          eta: 1, route_id: -1 };
      }
      ORDERS_LEN = 10000;
    }
  }

  fn test_init_orders_and_dist(dist: i16) {
    unsafe {
      for i in 0..4 {     
        ORDERS[i] = Order{ id: i as i64, from: i as i32, to: 7-i as i32, wait: 15, loss: 70, dist: 7-2*i as i32, 
          shared: true, in_pool: false, received: None, started: None, completed: None, at_time: None, eta: 1, route_id: -1 };
      }
      ORDERS_LEN = 4;
      for i in 0..7 { DIST[i][i+1] = dist; }    
      CABS_LEN =2;
      CABS[0] = Cab{ id: 0, location: 0, seats: 10 };
      CABS[1] = Cab{ id: 1, location: 1, seats: 10 };
    }
  }

  fn test_branches() -> Vec<Branch> {
    return vec![ 
      Branch{ cost: 1, outs: 4, ord_numb: 1, ord_ids: [1,2,3,4,5,5,4,3,2,1], ord_actions: [105,105,105,105,105,111,111,111,111,111], cab: 0 },
      Branch{ cost: 1, outs: 4, ord_numb: 1, ord_ids: [5,6,7,8,9,9,8,7,6,5], ord_actions: [105,105,105,105,105,111,111,111,111,111], cab: 0 },
      Branch{ cost: 1, outs: 4, ord_numb: 1, ord_ids: [1,6,7,8,9,9,8,7,6,1], ord_actions: [105,105,105,105,105,111,111,111,111,111], cab: 0 }];
  }

  fn test_leaves() -> Vec<Branch> {
    return vec![ 
      Branch{ cost: 1, outs: 2, ord_numb: 2, ord_ids: [1,2,0,0,0,0,0,0,0,0], ord_actions: [111,111,0,0,0,0,0,0,0,0], cab: 0 },
      Branch{ cost: 1, outs: 2, ord_numb: 2, ord_ids: [2,1,0,0,0,0,0,0,0,0], ord_actions: [111,111,0,0,0,0,0,0,0,0], cab: 0 },
      Branch{ cost: 1, outs: 2, ord_numb: 2, ord_ids: [0,1,0,0,0,0,0,0,0,0], ord_actions: [111,111,0,0,0,0,0,0,0,0], cab: 0 }];
  }

  #[test]
  #[serial]
  fn test_find_pool(){
    //test_init_orders_and_dist(1);
    let mut orders = unsafe { ORDERS.to_vec().drain(..ORDERS_LEN).collect() };
    let mut cabs = unsafe { CABS.to_vec().drain(..CABS_LEN).collect() };
    let stops = vec![ Stop{id:0,bearing:0, latitude: 0.0, longitude: 0.0 }];
    let mut max_route_id: i64 = 0;
    let mut max_leg_id: i64 = 0;
    let ret = find_pool(4, 4, &mut orders, &mut cabs, 
            &stops, &mut max_route_id, &mut max_leg_id);
    assert_eq!(ret.0.len(), 1);
  }

  fn get_pool_stops() -> Vec<Stop> {
    let mut stops: Vec<Stop> = vec![];
    let mut c: i64 = 0;
    for i in 0..49 {
      for j in 0..49 {
        stops.push(
          Stop{ id: c, bearing: 0, latitude: 49.0 + 0.05 * i as f64, longitude: 19.000 + 0.05 * j as f64}
        );
        c = c + 1;
      }
    }
    return stops;
  }

  fn get_pool_orders() -> Vec<Order> {
    let mut ret: Vec<Order> = vec![];
    for i in 0..50 {     
        let from: i32 = i % 2400;
        let to: i32 = from + 5;
        let dista = unsafe { DIST[from as usize][to as usize] as i32 };
        ret.push(Order{ id: i as i64, from, to, wait: 15, loss: 70, dist: dista, 
                    shared: true, in_pool: false, received: Some(Local::now().naive_local()), started: None, completed: None, at_time: None, 
                    eta: 1, route_id: -1 });
    }
    return ret;
  }

  fn get_pool_cabs() -> Vec<Cab> {
    let mut ret: Vec<Cab> = vec![];
    for i in 0..1000 {
        ret.push(Cab{ id: i, location: (i % 2400) as i32, seats: 10});
    }
    return ret;
  }


  /* 
      4 threads
     RELEASE: 5.532196233s
     DEV: 32.462779374s

     [profile.test]
      opt-level = 3
      debug = false
      debug-assertions = false
      overflow-checks = false
      lto = false
      panic = 'unwind'
      incremental = false
      codegen-units = 16
      rpath = false
   */
  #[test]
  #[serial]
  fn test_performance_find_pool(){
    let stops = get_pool_stops();
    init_distance(&stops);
    let mut orders: Vec<Order> = get_pool_orders();
    let mut cabs: Vec<Cab> = get_pool_cabs();
    let mut max_route_id: i64 = 0;
    let mut max_leg_id: i64 = 0;
    let start = Instant::now();
    let ret = find_pool(4, 4, &mut orders, &mut cabs, 
                                                &stops, &mut max_route_id, &mut max_leg_id);
    let elapsed = start.elapsed();
    println!("Elapsed: {:?}", elapsed); 
    assert_eq!(ret.0.len(), 12);
    assert_eq!(ret.1.len(), 17406);
  }

  #[test]
  #[serial]
  fn test_dive_leaves(){
    test_init_orders_and_dist(1);
    // 10 (high) => just the leaves
    let ret = dive(10, 4, 4);
    assert_eq!(ret.len(), 7);
  }
  
  #[test]
  #[ignore]
  #[serial]
  fn test_dive(){
    test_init_orders_and_dist(1);
    let ret = dive(0, 4, 4);
    assert_eq!(ret.len(), 900);
  }

  #[test]
  #[serial]
  fn test_store_leaves(){
    init_distance(&get_stops());
    set_orders();
    let slice = unsafe { &ORDERS[0 .. ORDERS_LEN] };
    let start = Instant::now();
    //let ret = store_leaves2(slice);
    let ret = store_leaves();
    let elapsed = start.elapsed();
    println!("Elapsed: {:?}", elapsed); 
    assert_eq!(ret.len(), 2118458);
  }

  #[test]
  #[serial]
  fn test_bearing_diff(){
    assert_eq!(bearing_diff(0,1), 1.0);
    assert_eq!(bearing_diff(-1,1), 2.0);
    assert_eq!(bearing_diff(-1,-2),1.0);
  }

  #[test]
  #[serial]
  fn test_add_branch(){
    test_init_orders_and_dist(1);
    let ret = add_branch(0,1,'i', 'i', 2);
    assert_eq!(ret.cost, 1);
  }

  #[test]
  #[serial]
  fn test_iterate() {
    let prev_node: Vec<Branch> = test_leaves();
    let ret = iterate(0, 4, 0, 1.0, &prev_node);
    assert_eq!(ret.len(), 1);
  }
  //lev: usize, in_pool: u8, start: i16, size: f32, prev_node: &Vec<Branch>) -> Vec<Branch>

  #[test]
  #[serial]
  fn test_store_branch_if_not_found(){
    let arr = 
      Branch{ cost: 1, outs: 4, ord_numb: 7, ord_ids: [1,2,3,3,4,4,2,1,0,0], ord_actions: [105,105,105,105,111,111,111,111,111,0], cab: 0 
    };
    test_init_orders_and_dist(1);
    let mut ret: Vec<Branch> = Vec::new();
    store_branch_if_not_found(0,4,0, &arr, &mut ret);
    assert_eq!(ret.len(), 1);
    assert_eq!(ret[0].ord_ids[0], 0); // was 1, should be 0
    assert_eq!(ret[0].ord_actions[7], 111); // was 0, should be 111
  }
  //lev: u8, in_pool: u8, ord_id: i32, br: &Branch, ret: &mut Vec<Branch>)

  #[test]
  #[serial]
  fn test_is_too_long() {
    test_init_orders_and_dist(1);
    let b =  Branch{ cost: 1, outs: 1, ord_numb: 7, ord_ids: [1,2,3,4,5,5,4,3,2,1], ord_actions: [105,105,105,105,105,111,111,111,111,111], cab: 0 };
    let ret = is_too_long('i', 0, 1, &b);
    assert_eq!(ret, true);
  }

  #[test]
  #[serial]
  fn test_store_branch() {
    test_init_orders_and_dist(1);
    let b =  Branch{ cost: 1, outs: 1, ord_numb: 1, ord_ids: [1,2,3,4,5,5,4,3,2,1], ord_actions: [105,105,105,105,105,111,111,111,111,111], cab: 0 };
    let ret = store_branch('i', 0, 0, &b, 4);
    assert_eq!(ret.cost, 3);
  }

  #[test]
  #[serial]
  fn test_rm_final_duplicates() {
    let mut arr: Vec<Branch> = test_branches();
    let mut max_route_id: i64 = 0;
    let mut max_leg_id: i64 = 0;
    let mut cabs: Vec<Cab> = vec![Cab{ id: 0, location: 0, seats: 10 },Cab{ id: 1, location: 1, seats: 10 }];
    test_init_orders_and_dist(1);
    let ret = rm_duplicates_assign_cab(4, &mut arr, &mut max_route_id, 
                                                          &mut max_leg_id, &mut cabs);
    assert_eq!(ret.1, "UPDATE cab SET status=0 WHERE id=1;\nINSERT INTO route (id, status, cab_id) VALUES (0,1,1);\n\
    UPDATE cab SET status=0 WHERE id=0;\nINSERT INTO route (id, status, cab_id) VALUES (1,1,0);\n");
  }

  #[test]
  #[serial]
  fn test_assign_and_remove() {
    test_init_orders_and_dist(1);
    let mut arr: Vec<Branch> = test_branches();
    let mut max_route_id: i64 = 0;
    let mut max_leg_id: i64 = 0;
    let ret = assign_and_remove(&mut arr, 4, 0, 0, &mut max_route_id, &mut max_leg_id);
    assert_eq!(ret, "UPDATE cab SET status=0 WHERE id=0;\nINSERT INTO route (id, status, cab_id) VALUES (0,1,0);\nINSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (0,0,1,0,1,1,16000,0,0);\n");
  }

  #[test]
  #[serial]
  fn test_is_found() {
    let arr = test_branches();
    assert_eq!(is_found(&arr, 0, 1, 4), false);
    assert_eq!(is_found(&arr, 0, 2, 4), true);
  }

  #[test]
  #[serial]
  fn test_mark_pools_as_dead() {
    let mut arr: Vec<Branch> = test_branches();
    mark_pools_as_dead(&mut arr, 0);
    assert_eq!(arr[0].cost, 1);
    assert_eq!(arr[1].cost, -1);
  }

  #[test]
  #[serial]
  fn test_find_nearest_cab() {
    test_init_orders_and_dist(1);
    assert_eq!(find_nearest_cab(0, 2), 0);
  }

  #[test]
  //#[ignore] // fails when run with others 
  #[serial]
  fn test_constraints_met() {
    test_init_orders_and_dist(1);
    let br = Branch{ cost: 1, outs: 4, ord_numb: 8, ord_ids: [0,1,2,3,4,4,3,2,1,0], 
            ord_actions: [105,105,105,105,105,111,111,111,111,111], cab: 0 };
    assert_eq!(constraints_met(br, 1), true);
  }

  #[test]
  #[serial]
  fn test_constraints_not_met() {
    test_init_orders_and_dist(10);
    let br = Branch{ cost: 1, outs: 4, ord_numb: 8, ord_ids: [0,1,2,3,4,4,3,2,1,0], 
            ord_actions: [105,105,105,105,105,111,111,111,111,111], cab: 0 };
    assert_eq!(constraints_met(br, 1), false);
  }

  #[test]
  #[serial]
  fn test_orders_to_array() {
    let vec: Vec<Order> = vec![Order{ id: 1, from: 1, to: 2, wait: 10, loss: 50, dist: 2, shared: true, in_pool: false,
          received: None,started: None,completed: None,at_time: None,eta: 0, route_id: -1
    }];
    let arr = orders_to_array(&vec);
    assert_eq!(arr.len(), MAXORDERSNUMB);
    assert_eq!(arr[0].id, 1);
  }
  
  #[test]
  #[serial]
  fn test_orders_to_transfer_array() {
    let vec: Vec<Order> = vec![Order{ id: 1, from: 1, to: 2, wait: 10, loss: 50, dist: 2, shared: true, in_pool: false,
          received: None,started: None,completed: None,at_time: None,eta: 0, route_id: -1
    }];
    let arr = orders_to_transfer_array(&vec);
    assert_eq!(arr.len(), MAXORDERSNUMB);
    assert_eq!(arr[0].id, 1);
  }

  #[test]
  #[serial]
  fn test_cabs_to_array() {
    let vec: Vec<Cab> = vec![Cab{id: 0, location: 0, seats: 0}];
    let arr = cabs_to_array(&vec);
    assert_eq!(arr.len(), MAXCABSNUMB);
    assert_eq!(arr[0].id, 0);
  }

  #[test]
  #[serial]
  fn test_stops_to_array() {
    let vec: Vec<Stop> = vec![ Stop{id:0,bearing:0, latitude: 0.0, longitude: 0.0 }];
    let arr = stops_to_array(&vec);
    assert_eq!(arr.len(), MAXSTOPSNUMB);
    assert_eq!(arr[0].id, 0);
  }
}