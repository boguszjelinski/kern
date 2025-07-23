/// Kabina minibus/taxi dispatcher
/// Copyright (c) 2024 by Bogusz Jelinski bogusz.jelinski@gmail.com
/// 
/// Extender submodule, extends passenger pools.
/// A pool is a group of orders to be picked up by a cab in a prescribed sequence
/// 

use std::collections::HashMap; //use std::io::Write;
use std::{thread, cmp, vec};
use chrono::{Local, Duration};
use log::{info, warn, debug};
//use postgres::{Client, NoTls};
use mysql::*;
use mysql::prelude::*;
use crate::model::{ KernCfg, Leg, Order, OrderStatus, RouteStatus, Stop};
use crate::repo::{find_legs, assign_order_find_cab, create_leg, update_leg_a_bit2, update_reserves_in_legs_before_and_including,
                  update_reserves_in_legs_before_and_including2,
                  update_place_in_legs_after, update_passengers_and_reserve_in_legs_between, update_reserve_after,
                  find_orders_by_status_and_time};
use crate::distance::DIST;
use crate::utils::get_elapsed;

pub const MAXCOST : i32 = 1000000;
pub const STOP_WAIT : i16 = 1;

#[derive(Copy, Clone)]
struct LegIndicesWithDistance2 {
  idx_from: usize,
  idx_to: usize,
  route_id: i64, 
  dist: i32,
  wait: i32,
  tour: i32,
  sum_reserve: i32, // sum in dropp-off stage
  order: Order
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

pub fn find_matching_routes(conn: &mut PooledConn, demand: &Vec<Order>, stops: &Vec<Stop>, max_leg_id: &mut i64, cfg: &KernCfg) 
                            -> Vec<Order> {
    if demand.len() == 0 {
        return Vec::new();
    }
    let mut demand_cpy = demand.clone();
    let mut ret: Vec<Order> = Vec::new();
    loop {
      let mut legs: Vec<Leg> = find_legs(conn); // TODO: legs that will soon start should not be taken into consideration !!!
      // as we will get customers not picked up ???
      if legs.len() == 0 {
          return demand.to_vec();
      }
      let ass_orders: Vec<Order> = find_orders_by_status_and_time(conn, OrderStatus::ASSIGNED,
         (Local::now() - Duration::minutes(30)).naive_local());
      info!("Extender START, new orders count={} assigned orders={} legs count={}", demand.len(), ass_orders.len(), legs.len());
      let ass_orders_map = assigned_orders(&ass_orders);

      let (mut ret_part, missed, sql)
        = extend_routes(&demand_cpy, &ass_orders_map, stops, &mut legs, max_leg_id, cfg);
        
      // EXECUTE SQL !!
      //write_sql_to_file(itr, &sql_bulk, "extender");
      //for s in split_sql(sql_bulk, 150) {
      //  client.batch_execute(&s).unwrap();
      //}
      if sql.len() > 0 {
        //debug!("{}", sql_bulk);
        match conn.query_iter(&sql) {
          Ok(_) => {} 
          Err(err) => {
            warn!("Extender SQL error: {}", err);
          }
        }
      }
      ret.append(&mut ret_part);
      if missed.len() == 0 { break; }
      demand_cpy = missed;
    }
    return ret;
}

/*pub fn write_sql_to_file(itr: i32, sql: &String, label: &str) {
  let file_name = format!("{}-{}.sql", label.to_string(), itr);
  let msg = format!("SQL for {} failed", file_name);
  let mut _file = std::fs::File::create(&file_name).expect(&("Create ".to_string() + &msg));
  //file.write_all(sql.as_bytes()).expect(&("Write ".to_string() + &msg));
}
*/

pub fn count_legs(legs: &Vec<Leg>) -> HashMap<i64, i8> {
  let mut counts = HashMap::new();
  if legs.len() == 0 {
    return counts;
  }
  let mut i:usize = 1;
  let mut count = 1;
  while i < legs.len() {
    if legs[i-1].route_id != legs[i].route_id {
      counts.insert(legs[i-1].route_id, count);
      count = 0;
    }
    count += 1;
    i += 1;
  }
  counts.insert(legs[i-1].route_id, count);
  return counts;
}

// orders must be ordered by route_id !!!
pub fn assigned_orders(assigned_orders: &Vec<Order>) -> HashMap<i64, Vec<Order>> {
  let mut ret = HashMap::new();
  if assigned_orders.len() == 0 {
    return ret;
  }
  let mut i: usize = 1;
  let mut assigned_to_route: Vec<Order> = vec![];
  assigned_to_route.push(assigned_orders[0]);

  while i < assigned_orders.len() {
    if assigned_orders[i-1].route_id != assigned_orders[i].route_id {
      ret.insert(assigned_orders[i-1].route_id, assigned_to_route);
      assigned_to_route = vec![];
    }
    assigned_to_route.push(assigned_orders[i]);
    i += 1;
  }
  ret.insert(assigned_orders[i-1].route_id, assigned_to_route);
  return ret;
}

// this method returns:
// 1) orders that can go to pool finder because they did not match anything in routes 
// 2) orders than cannot go to pool finder because they perfectly match a route - a duplicate that needs another iteration
// 3) sql to be run 
fn extend_routes(orders: &Vec<Order>, assigned_orders: &HashMap<i64, Vec<Order>>, stops: &Vec<Stop>, legs: &mut Vec<Leg>, 
                max_leg_id: &mut i64, cfg: &KernCfg) -> (Vec<Order>, Vec<Order>, String) {
  let mut t_numb = 10; // mut: there might be one more thread, rest of division
	let leg_count: HashMap<i64, i8> = count_legs(legs);
  
	let mut children = vec![];

  let mut chunk: i32 = (orders.len() as f32 / t_numb as f32) as i32;
  if chunk == 0 { chunk = 1; } // few orders
  if t_numb * chunk < orders.len() as i32 { t_numb += 1; } // last thread will be the reminder of division
  
  // run the threads, each thread gets its own range of orders to iterate over - hence 'iterate'
  for i in 0..t_numb {
    if i * chunk >= orders.len() as i32 { break; }
    let part =
      Vec::from_iter(orders[(i*chunk) as usize..if (i+1)*chunk > orders.len() as i32 { orders.len() } else {((i+1)*chunk) as usize}].iter().cloned());
    let legs_cpy = legs.to_vec();
    let stops_cpy = stops.to_vec();
    let leg_count_cpy = leg_count.clone();
    let old_orders = assigned_orders.clone();
    let c = cfg.clone();
    children.push(thread::spawn(move || {
      iterate(part, &legs_cpy, &stops_cpy, &leg_count_cpy, &old_orders, &c)
    }));
  }
  // collect the data from threads, join their execution first
  let mut indices : Vec<LegIndicesWithDistance2> = Vec::new();
  for handle in children {
    let mut cpy : Vec<LegIndicesWithDistance2> = handle.join().unwrap().to_vec();
    indices.append(&mut cpy);
  }
  // sort - there might be extensions of the same route, we will choose the better one, the worse one will go to next iteration
  indices.sort_by_key(|e| e.dist.clone());
  // get SQL
  let mut sql: String = String::from("");
  let mut assigned_orders: Vec<i64> = Vec::new();
  let mut missed_orders_for_pool: Vec<Order> = Vec::new();
  let mut missed_orders: Vec<Order> = Vec::new();
  let mut extended_routes: Vec<i64> = Vec::new(); // IDs of extended routes so that we do not extend twice - some will be sent to the next iteration
  let mut missed_matches: Vec<i64> = Vec::new();
  let mut missed_matches_no_dups: Vec<i64> = Vec::new();

  for ind in indices {
    if extended_routes.contains(&ind.route_id) {
      missed_matches.push(ind.order.id);
      continue;
    }
    assigned_orders.push(ind.order.id);
    extended_routes.push(ind.route_id);
    sql += &get_sql(&ind, max_leg_id, &legs);
  }

  for o in orders {
    if assigned_orders.contains(&o.id) {
      continue;
    }
    if missed_matches.contains(&o.id) && !missed_matches_no_dups.contains(&o.id) { // don't send missed matches to pool or solver
      missed_orders.push(*o);
      missed_matches_no_dups.push(o.id);
      continue;
    }
    missed_orders_for_pool.push(*o);
  }
  return (missed_orders_for_pool, missed_orders, sql);
}

fn iterate(orders: Vec<Order>, legs: &Vec<Leg>, stops: &Vec<Stop>, leg_count: &HashMap<i64, i8>, 
            assigned_orders: &HashMap<i64, Vec<Order>>, cfg: &KernCfg) -> Vec<LegIndicesWithDistance2> {
  let mut ret : Vec<LegIndicesWithDistance2> = Vec::new();
  for o in orders {
    match find_route(&o, legs, stops, leg_count, assigned_orders, cfg) {
      Some(x) => { ret.push(x); },
      None => {}
    }
  }
  return ret;
}

fn leg_is_short(val: Option<&i8>, max_legs: i8) -> bool {
  match val {
    Some(x) => { *x <= max_legs },
    None => { true }
  }
}

// iterate over all existing routes and find the one that will be least distracted (additional path is shortest)
fn find_route(order: &Order, legs: &Vec<Leg>, stops: &Vec<Stop>, leg_count: &HashMap<i64, i8>,
              assigned_orders: &HashMap<i64, Vec<Order>>, cfg: &KernCfg) -> Option<LegIndicesWithDistance2> {
  unsafe {
  if legs.len() == 0 { return None; }
  let mut ret: Option<LegIndicesWithDistance2> = None;
  let mut i: usize = 1; // index of pickup TODO: i=0 has to be considered one day
  let mut total_dist: i32;
  if legs[0].status == RouteStatus::STARTED {
    let mut on_the_way = get_elapsed(legs[0].started) as i32;
    if on_the_way == -1 { on_the_way = 0; }
    total_dist = cmp::max(0, legs[0].dist - on_the_way/60) + STOP_WAIT as i32;
  } else {
    total_dist = legs[0].dist + 2*STOP_WAIT as i32; // distance from the begining of a route; well, only the remaining legs
  }
  let mut min_cost: i32 = MAXCOST; // added cost of the winner, we are starting with a looser
  let mut is_short = leg_is_short(leg_count.get(&legs[i].route_id), cfg.max_legs);
  let order_from = order.from as usize;
  let max_angle = cfg.max_angle as f32;
  let max_angle_dist = cfg.max_angle_dist as i32;
  let mut wait_legs: i16 = 0; // each leg takes 15secs more, TODO: check why
  let mut first_leg: usize = i; // index of the first leg in a route, needed by 'wait_exceeded'

  while i < legs.len() { // this is the pick-up loop
    let leg = legs[i];
    if leg.route_id != legs[i-1].route_id { // new route -> check the previous one
      first_leg = i;
      let prev_leg_to = legs[i-1].to as usize;
      let dist1 = DIST[prev_leg_to][order_from] as i32;
      is_short = leg_is_short(leg_count.get(&leg.route_id), cfg.max_legs);
      // check beyond route
      if total_dist + dist1 + extra_wait(wait_legs + 1) < order.wait
         && dist1 < min_cost
         && (dist1 > max_angle_dist || bearing_diff(stops[prev_leg_to].bearing, stops[order_from].bearing) <  max_angle) { // well, we have to compare to something; there still might be a better plan with lesser wait time
        min_cost = dist1;
        ret = get_some(i, i, legs[i-1].route_id, STOP_WAIT as i32 + dist1, 
                      total_dist + dist1+ extra_wait(wait_legs), 
                      order.dist, 0, order);
        debug!("DEBUG3C find_route: order_id={}, route_id={}, total_dist={}, dist1={}, wait_legs={}", 
                      order.id, legs[i-1].route_id, total_dist, dist1, wait_legs +1);
        // i beyond the route, just to mark a leg in the next route
      }
      wait_legs = 0;
      total_dist = 0;
      if leg.status == RouteStatus::STARTED { // but we need such legs to avoid assigning legs that very soon will start (little chance to let know the driver)
        let mut on_the_way = get_elapsed(leg.started) as i32;
        if on_the_way == -1 { on_the_way = 0; }
        total_dist += cmp::max(0, leg.dist - on_the_way/60) + STOP_WAIT as i32;
        wait_legs += 1;
        i += 1;
        continue; 
      }
      // if there is too many non-pickedup customers, uncomment the below, which mean do not assign a leg which is about to start soon
      if leg.status == RouteStatus::ASSIGNED {
        total_dist += leg.dist + 2*STOP_WAIT as i32;
        wait_legs += 1;
        i += 1;
        continue; 
      }
    }
    if leg.status == RouteStatus::STARTED { // this should never happen, the same check is above when new route is found 
      let mut on_the_way = get_elapsed(leg.started) as i32;
      if on_the_way == -1 { on_the_way = 0; }
      total_dist += cmp::max(0, leg.dist - on_the_way/60) + STOP_WAIT as i32;
      i += 1;
      continue; 
    }
    let mut add_cost: i32 = (DIST[leg.from as usize][order_from] + STOP_WAIT + DIST[order_from][leg.to as usize]) as i32
                            - leg.dist;
    if leg.to != order.from // direct hit in next leg
      && leg.passengers < leg.seats // 'seats' come from 'cab' table; < means at least one seat available, = would mean all occupied 
      && (total_dist + (DIST[leg.from as usize][order_from]) as i32) + extra_wait(wait_legs) <= order.wait 
      && (leg.from == order.from // direct hit
            || (is_short // we don't want to extend long routes
                && 
                add_cost <= leg.reserve // or a non-perfect match
                // TODO: !! BEARING CONTROLL
                && add_cost < min_cost)) { // no use in checking a plan if we have a better one
      // we have a pickup, check drop-off now, 
      // 3 possibilities - in the same leg or in next ones, or direct hit at leg.to
      // firstly null cost if direct hit
      if leg.from == order.from {
        add_cost = 0;
      }
      if leg.to == order.to { // direct hit for drop-off in the same leg, and no detour
        if leg.from == order.from { // bingo, no point looking for any other route (TODO: check number of seats!)
          //info!("Extension proposal, perfect match, order_id={}, route_id={}", order.id, leg.route_id);
          return get_some( i, i, leg.route_id, 0, total_dist + extra_wait(wait_legs), 
                          order.dist, 0, order);
          // 0: pickup and dropoff are direct hits, best solution TODO: there can be more such solution with shorter wait time!!
          // SAVE1 no leg at all, both are direct hits // check to-to & from-from
        } else if is_short {
          // SAVE2 // pickup was not a direct hit  // two legs affected   // from-from will fail, pickup is expanded
          if !wait_exceeded(order, wait_legs, first_leg, i, i, total_dist, add_cost, 0, legs, assigned_orders)
             && (DIST[leg.from as usize][order_from] > max_angle_dist as i16 || bearing_diff(stops[leg.from as usize].bearing, stops[order_from].bearing) <  max_angle) {
            min_cost = add_cost;
            ret = get_some(i, i, leg.route_id, add_cost, 
                         total_dist + STOP_WAIT as i32 + (DIST[leg.from as usize][order_from] as i32) + extra_wait(wait_legs),
                          order.dist, 0, order);
          }
        }
      } else { // find in next legs
        match find_droppoff(order, legs, first_leg, i, add_cost, min_cost, 
                      total_dist + (DIST[leg.from as usize][order_from] as i32) + extra_wait(wait_legs), wait_legs,
                            is_short, assigned_orders, stops, &cfg) {
          Some(x) => { 
            min_cost = x.dist; 
            ret = Some(x); 
          },
          None => {}
        }
      }
    } 
    total_dist += leg.dist + STOP_WAIT as i32;
    wait_legs += 1;
    if total_dist + extra_wait(wait_legs) > order.wait { // nothing to look for here, find next route
      let mut i2 = i + 1;
      while i2 < legs.len() && legs[i2].route_id == leg.route_id { i2 += 1; }
      i = i2;
    } else {
      i += 1; 
    }
  }
  // beyond the last route
  let last_dist = total_dist + STOP_WAIT as i32 + (DIST[legs[i-1].to as usize][order_from] as i32) + extra_wait(wait_legs);
  if last_dist < order.wait
    && (DIST[legs[i-1].to as usize][order_from] as i32) < min_cost { // well, we have to compare to something; there still might be a better plan with lesser wait time
    // SAVE6
    //info!("Extension proposal, beyond route, order_id={}, route_id={}", order.id, legs[i-1].route_id);
    debug!("DEBUG6 find_route: order_id={}, route_id={}, leg_id={}, leg_dist={}, leg_reserve={}, from={}, to={}, dist={},", 
          order.id, legs[i-1].route_id, legs[i-1].id, legs[i-1].dist, legs[i-1].reserve, legs[i-1].to, order.from, 
          DIST[legs[i-1].to as usize][order_from]);
    return get_some( i, i, legs[i-1].route_id, DIST[legs[i-1].to as usize][order_from] as i32,
                    last_dist, order.dist, 0, order);
  }
  if min_cost == MAXCOST {
    return None;
  }
  //info!("Extension proposal, order_id={}, route_id={}, cost={}", order.id, ret.unwrap().route_id, ret.unwrap().dist);
  return ret;
}
}

// for unknown reason cabs wait about 30s more than defined one minute
fn extra_wait(count: i16) -> i32 { 
  return (count as f32 * 0.5) as i32;
}

fn wait_exceeded(ord: &Order, wait_legs: i16, first_leg: usize, i: usize, j:usize, wait: i32, add_cost: i32, add_cost2: i32, legs: &Vec<Leg>, ass_orders: &HashMap<i64, Vec<Order>>) -> bool {
  if i>= legs.len() { return false; }
  let add2_cost = if add_cost2 < 0 { 0 } else { add_cost2 };
  let route_id = legs[i].route_id;
  let orders: Vec<Order>;
  match ass_orders.get(&route_id) { Some(x) => { 
    orders = x.to_vec(); 
  } , None => { 
    return false; 
  } };
  let mut log = format!("wait_debug: order_id={}, leg_from={}, leg_to={}, route_id={}, wait={}, wait_legs={}, add_cost={}, add_cost2={} legs={}, orders={}; ", 
                                ord.id, if i< legs.len() { legs[i].id } else { -1 }, if j<legs.len() { legs[j].id } else { -1 }, 
                                route_id, wait, wait_legs, add_cost, add_cost2, legs.len(), orders.len());
  let mut total_dist = wait + legs[i].dist + STOP_WAIT as i32 + if add_cost < 0 { 0 } else { add_cost }  ;
  if j == i { total_dist += add2_cost; }
  let mut idx = i + 1; // we will check the impact on wait of the other customers, beyond the extended leg (i)
  let mut passed_log: String = String::new();
  let xtra_wait = extra_wait(wait_legs);
  while idx < legs.len() {
    if legs[idx].route_id != route_id {
      break; // go to max_loss check below
    }
    for o in &orders {
      if o.id == ord.id { // wait time in this order is checked before this function is called
        continue;
      }
      let time_passed = get_elapsed(o.received)/60; // TODO: we do not have assignment timestamp
      if time_passed == -1 { // TODO it should never happen!!
        warn!("Assigned order but received is NULL");
        continue;
      }
      passed_log += &format!("[order_id={}, passed={}, total_dist={}, extra_wait={}], ", o.id, time_passed, total_dist, xtra_wait);
      if o.from == legs[idx].from && time_passed as i32 + total_dist + xtra_wait >= o.wait - STOP_WAIT as i32 { // - STOP_WAIT due to some rounding errors - eg. while subtracting elapsed time
        return true;
      }
    }
    total_dist += legs[idx].dist + STOP_WAIT as i32;
    if idx == j { total_dist += add2_cost; } 
    log += &format!("[dist after leg={}, dist={}], ", legs[idx].id, total_dist);
    idx += 1;
  }

  // maybe we do not need such a strong controll, like the lines below
  //return false;
  
  //info!("{} {}", log, passed_log);
  // let's check max_loss in other orders
  passed_log += &format!("LOSS CHECK: first_leg={}, i={}, j={}, legs[first_leg].id={}, numb of orders={}", first_leg, i, j, legs[first_leg].id, orders.len());
  for o in &orders {
    passed_log += &format!("({})", o.id);
    if o.id == ord.id { // wait time in this order is checked before this function is called
      continue;
    }
    let dist_with_loss: i32 = ((1.0 + o.loss as f32 / 100.0) * o.dist as f32).round() as i32 + 3*STOP_WAIT as i32; //+ stop so that we are not so strict
    let mut legs_count = 0; // to count extra_wait
    total_dist = add_cost; // just in case the order has allready started (no "from"), so pickup extension will affect this order 
    idx = first_leg; // where the route starts
    while (idx == first_leg || legs[idx].route_id == legs[idx - 1].route_id) && idx < legs.len() {
      if o.from == legs[idx].from { // it may never happen for an order that is in progress at this
        if idx > j { // the older order is not affected by droppoff, so not by the whole extension
          break; // check next order
        }
        if idx > i {
          total_dist = 0;
        } else {
          total_dist = add_cost; // we have to repeat the initial assignment as some distances could have been added
        } 
        legs_count = 0;
      }
      if o.to == legs[idx].to {
        if idx < i { // 'to' is before any extension, this order will not be affected
          break;
        }
        if idx >= j { // 'to' is after extension, this order is affected by drop-off extension
          total_dist += add2_cost;
        }
        if total_dist + legs[idx].dist /*+ extra_wait(legs_count)*/ > dist_with_loss {
          // some orders began before legs read from database, so we don't even sum up the whole trip, we are not that strict
          // we would have to check 'started'of that order
          return true; // one of old orders would not like it
        }
        passed_log += &format!("[order_id={}, total_dist={}, extra_wait={}, dist_with_loss={}], ", o.id, total_dist, extra_wait(legs_count), dist_with_loss - 3*STOP_WAIT as i32);
        break; // check next order
      }
      total_dist += legs[idx].dist + STOP_WAIT as i32;
      legs_count += 1;
      idx += 1;
    }
  }
  info!("{} {}", log, passed_log);
  return false;
}

#[inline]
fn get_some(from: usize, to: usize, route_id: i64, cost: i32, wait: i32, tour: i32, sum_reserve: i32, order: &Order) -> Option<LegIndicesWithDistance2> {
  return Some(LegIndicesWithDistance2{
    idx_from: from, 
    idx_to: to,  
    route_id,
    dist: cost,
    wait,
    tour,
    sum_reserve,
    order: *order
  });
}

fn find_droppoff(order: &Order, legs: &Vec<Leg>, first_leg: usize, i: usize, add_cost: i32, mincost: i32, wait: i32, wait_legs: i16, 
                is_short: bool, assigned_orders: &HashMap<i64, Vec<Order>>, stops: &Vec<Stop>, cfg: &KernCfg) -> Option<LegIndicesWithDistance2> {
  unsafe {
  let mut ret: Option<LegIndicesWithDistance2> = None;
  let max_angle = cfg.max_angle as f32;
  let max_angle_dist = cfg.max_angle_dist as i32;
  let mut j: usize = i + 1;
  let mut min: i32 = mincost;
  let dist_with_loss: i32 = ((1.0 + order.loss as f32 / 100.0) * order.dist as f32).round() as i32;
  let order_to = order.to as usize;
  let mut add2_cost = (DIST[legs[i].from as usize][order.from as usize] + STOP_WAIT + DIST[order.from as usize][order_to] 
                            + STOP_WAIT + DIST[order_to][legs[i].to as usize]) as i32 - legs[i].dist;
  // first check the same leg as pickup                        
  if is_short && 
      add2_cost <= legs[i].reserve && add_cost + add2_cost < min 
      && !wait_exceeded(order, wait_legs, first_leg, i, i, wait, add_cost, add2_cost, legs, assigned_orders) { // still no detour loss

    min = add_cost + add2_cost;
    ret = get_some(i, i, legs[i].route_id, add_cost + add2_cost, wait, order.dist, legs[i].reserve, order);
    // SAVE3 within the same expanded leg
    //if legs[i].from == order.from { // pickup direct hit
    // two legs for drop-off
    // to-to will fail
    //} else {
    // three legs
    // to-to & from-from will fail, still the same leg
    //}
  }
  // but it might be a better plan in next legs of the route
  let mut tour: i32 = DIST[order.from as usize][legs[i].to as usize] as i32 + STOP_WAIT as i32; // it is valid even if direct hit
  let mut sum_reserve: i32 = cmp::max(0, legs[i].reserve - add_cost);

  while j < legs.len() && legs[j].route_id == legs[j-1].route_id {
    let leg = legs[j];
    let leg_from = leg.from as usize;
    if leg.passengers >= leg.seats { // a leg in between pickup and dropoff is unacceptable
      return ret;
    }
    add2_cost = (DIST[leg_from][order_to] + STOP_WAIT + DIST[order_to][leg.to as usize]) as i32 - leg.dist;
    if (((leg.to == order.to && tour + leg.dist <= dist_with_loss)) // direct hit, no extra cost
        || (is_short
            && 
            add2_cost < leg.reserve
            && tour + (DIST[leg_from][order_to] as i32) + extra_wait((j-i) as i16) <= dist_with_loss // TODO: (j-i) is a misterious delay each leg, to be analysed, some delay in Kim?
            && add_cost + add2_cost < min)
            && (DIST[leg_from][order_to] > max_angle_dist as i16 || bearing_diff(stops[leg_from].bearing, stops[order_to].bearing) <  max_angle) )
        && !wait_exceeded(order, wait_legs + ((j - i) as i16), first_leg, i, j, wait, add_cost, add2_cost, legs, assigned_orders) {
      min = add_cost + add2_cost;
      ret = get_some( i, j, legs[i].route_id, add_cost + add2_cost, wait, 
                      tour + (DIST[leg_from][order_to] as i32) + extra_wait((j-i) as i16), sum_reserve, order); // tour is later used to count reserve (reserve=dist*loss-tour), but beware, if not used in some other way!
      debug!("DEBUG4 dropp-off: order_id={}, route_id={}, leg_id={}, leg_dist={}, wait={}, leg_reserve={}, from={}, to={}, add2_cost={}, a={}, b={}, 1={}, 2={}, 3={}", 
          order.id, legs[i].route_id, leg.id, leg.dist, wait, leg.reserve, leg.from, leg.to, add2_cost, 
          DIST[leg_from][order_to], DIST[order_to][leg.to as usize],
          leg.from, order.to, leg.to);
      // SAVE4
      //if legs[i].from == order.from { // pickup direct hit
      // two legs for drop-off
      // different legs but still to-to and from-from might be true
      //} else {
      // four legs
      //}
    }
    tour += leg.dist + STOP_WAIT as i32;
    sum_reserve += leg.reserve;
    if tour + extra_wait((j-i) as i16) <= dist_with_loss { 
      return ret; // nothing to look after any more
    }
    j += 1;
  }
  // what if dropoff extends beyond the route?
  if //j > 1 && legs[j-2].route_id == legs[j-1].route_id 
    tour + (DIST[legs[j-1].to as usize][order_to] as i32) + extra_wait((j-i) as i16) < dist_with_loss 
        && add_cost < min 
        && !wait_exceeded(order, wait_legs + ((j - i) as i16), first_leg, i, j, wait, add_cost, add2_cost, legs, assigned_orders) 
        && (DIST[legs[j-1].to as usize][order_to] > max_angle_dist as i16 || bearing_diff(stops[legs[j-1].to as usize].bearing, stops[order_to].bearing) <  max_angle) { // we don't ruin the current route so we just take the pickup cost, but you might think otherwise
    ret = get_some(i, j, legs[i].route_id, add_cost, wait, 
                  tour + (DIST[legs[j-1].to as usize][order_to] as i32) + extra_wait((j-i) as i16), sum_reserve, order);
    debug!("DEBUG dropp-off beyond: order_id={}, leg_id={}, to={}", order.id, legs[j-1].id, legs[j-1].to);             
    // SAVE5
    // !!! necessary check j>legs.len() || route_id != route_id, which means beyond route
    //if legs[i].from == order.from { // pickup direct hit
      // two legs for drop-off
    //} else {
        // four legs
    //}
  }
  return ret;
}
}

fn get_sql(f: &LegIndicesWithDistance2, max_leg_id: &mut i64, legs: &Vec<Leg>) -> String {
  unsafe {
  let mut prev_leg: Leg = legs[f.idx_from - 1];
  let reserve = cmp::max(0, f.order.wait - f.wait);
  // reserves before changed leg have to satisfy the current order and (!) the added cost will affect wait time of orders that start after the extension
  let detour_reserve = cmp::max(0, (((100.0 + f.order.loss as f32) / 100.0) * f.order.dist as f32) as i32 - f.tour);
  let mut sql: String = String::from("");
  sql += &assign_order_find_cab(f.order.id,
                        if f.idx_from >= legs.len() || f.route_id != legs[f.idx_from].route_id { -1 } else { legs[f.idx_from].id }, 
                                f.route_id, f.wait, "true", "expander");
  // extension totally BEYOND a route, including pickup
  if f.idx_from >= legs.len() // beyond the last route in the list, here we do not have route_id
     || f.route_id != legs[f.idx_from].route_id {  // beyond a route inside the list
    // SAVE0, SAVE6
    // it will be both pick-up and drop-off
    if prev_leg.to == f.order.from { // direct hit
      sql += &update_reserves_in_legs_before_and_including(prev_leg.route_id, prev_leg.place, reserve); 
      sql += &create_leg(f.order.id, 
        f.order.from,
        f.order.to,
        prev_leg.place + 1,
        RouteStatus::ASSIGNED,
        f.order.dist as i16,
        detour_reserve,
        prev_leg.route_id as i64, 
        max_leg_id, // incremented inside
        1, 
        &("route extender SAVE0A".to_string()));
    } else { // not a direct hit
      sql += &update_reserves_in_legs_before_and_including(prev_leg.route_id, prev_leg.place, reserve); 
      sql += &create_leg(-1,  // ??
        prev_leg.to,
        f.order.from,
        prev_leg.place + 1,
        RouteStatus::ASSIGNED,
        DIST[prev_leg.to as usize][f.order.from as usize],
        reserve,
        prev_leg.route_id as i64, 
        max_leg_id, // incremented inside
        0, 
        &("route extender SAVE0B".to_string()));
      sql += &create_leg(f.order.id, 
        f.order.from,
        f.order.to,
        prev_leg.place + 2,
        RouteStatus::ASSIGNED,
        f.order.dist as i16,
        detour_reserve,
        prev_leg.route_id as i64, 
        max_leg_id,
        1, 
        &("route extender SAVE0C".to_string()));      
        debug!("SAVE0C: route_id={}, res: {}, wait: {}", f.route_id, reserve, f.wait);
    }
  } else { // inside, at least pickup
    let leg_pick = legs[f.idx_from];
    sql += &update_reserves_in_legs_before_and_including2(leg_pick.route_id, leg_pick.place -1, reserve, f.dist);
    
    if f.idx_from == f.idx_to  { // one leg will be extended, 4 situations here
      let resrv = cmp::max(0, cmp::min(leg_pick.reserve, detour_reserve) - STOP_WAIT as i32);
      // first adjust reserves after the leg as extension (3 of 4 cases below) will affect wait time
      sql += &update_reserve_after(leg_pick.route_id, f.dist, leg_pick.place+1);

      if leg_pick.from == f.order.from && legs[f.idx_to].to == f.order.to { // matches perfectly
        // SAVE 1
        sql += &update_passengers_and_reserve_in_legs_between(leg_pick.route_id, detour_reserve, leg_pick.place, leg_pick.place); // one leg to be updated                                                    
      } else if leg_pick.from == f.order.from { // only pickup matches
        // SAVE 3
        //sql += &update_passengers_and_reserve_in_legs_between(leg_pick.route_id, resrv, leg_pick.place + 1, 100); // 100: all after +1
        sql += &update_place_in_legs_after(leg_pick.route_id, leg_pick.place + 1);
        let len_diff: i32 = (DIST[f.order.to as usize][leg_pick.to as usize] + f.order.dist as i16 + STOP_WAIT) as i32 - leg_pick.dist;
        sql += &create_leg(f.order.id, 
          f.order.to, // well, a leg after drop-off will be assigned to the order, not quite awesome
          leg_pick.to,
          leg_pick.place + 1,
          RouteStatus::ASSIGNED,
          DIST[f.order.to as usize][leg_pick.to as usize],
          cmp::max(0, leg_pick.reserve - len_diff), // 'dist' contains added cost/length
          leg_pick.route_id as i64, 
          max_leg_id,
          leg_pick.passengers as i8, 
          &("route extender SAVE3".to_string()));
        // the extended leg should point at the new leg added above
        sql += &update_leg_a_bit2(leg_pick.route_id, leg_pick.id, f.order.to, 
                f.order.dist as i16, cmp::min(resrv, len_diff), leg_pick.passengers as i8 +1); // MIN because reserve in 2 legs <= reserve in one leg; len_diff = reserv - (reserv - len_diff)
      } else if legs[f.idx_to].to == f.order.to { // only drop-off matches
        // SAVE 3
        sql += &update_passengers_and_reserve_in_legs_between(leg_pick.route_id, resrv, leg_pick.place + 1, 100); // 100: all after +1
        sql += &update_place_in_legs_after(leg_pick.route_id, leg_pick.place + 1);
        sql += &create_leg(f.order.id, 
          f.order.from,
          leg_pick.to, // == order.to
          leg_pick.place + 1,
          RouteStatus::ASSIGNED,
          f.order.dist as i16,
          resrv,
          leg_pick.route_id as i64, 
          max_leg_id,
          leg_pick.passengers as i8 + 1, 
          &("route extender SAVE3B".to_string()));
        // the new leg above will have a smaller reserv than the extended leg, but how much smaller - what will be the reserve of the existing leg?
        // it will be cmp::min(leg_pick.reserve - resrv, leg_pick.reserve - len_diff)
        // but to spare one subtraction let's find the max first
        let len_diff: i32 = (f.order.dist + DIST[leg_pick.from as usize][f.order.from as usize] as i32 + STOP_WAIT as i32) - leg_pick.dist;
        let reserve_subtr = cmp::max(resrv, len_diff);

        // the extended leg should point at the new leg added above
        sql += &update_leg_a_bit2(leg_pick.route_id, leg_pick.id, f.order.from, 
                 DIST[leg_pick.from as usize][f.order.from as usize], 
                 cmp::max(0, cmp::min(leg_pick.reserve - reserve_subtr, f.order.wait - f.wait)), // yes, wait time has to be taken into acount too
                 leg_pick.passengers as i8);
      } else { // no match, the order will extend one leg
        sql += &update_place_in_legs_after(leg_pick.route_id, leg_pick.place + 1); // TODO: one call, not two
        sql += &update_place_in_legs_after(leg_pick.route_id, leg_pick.place + 1);
        let added_cost = (DIST[leg_pick.from as usize][f.order.from as usize] + STOP_WAIT + DIST[f.order.from as usize][f.order.to as usize] 
                              + STOP_WAIT + DIST[f.order.to as usize][leg_pick.to as usize]) as i32 + extra_wait(2) - leg_pick.dist;
        sql += &create_leg(f.order.id, 
          f.order.from,
          f.order.to,
          leg_pick.place + 1,
          RouteStatus::ASSIGNED,
          f.order.dist as i16,
          // 3 things - current reserve (other orders), reserve for the new order and how the new order affects the old ones
          cmp::max(0, cmp::min(leg_pick.reserve - added_cost, detour_reserve)),
          leg_pick.route_id as i64, 
          max_leg_id,
          leg_pick.passengers as i8 + 1, 
          &("route extender SAVE3C".to_string()));
        // like in SAVE3B, here the reserve has to be split in 3 (!) legs
        let len_diff: i32 = (f.order.dist + DIST[f.order.to as usize][leg_pick.to as usize] as i32 + STOP_WAIT as i32) - leg_pick.dist;
        let reserve_subtr = cmp::max(resrv, len_diff);
        let reserve2 = cmp::min(leg_pick.reserve - reserve_subtr, f.order.wait - f.wait - f.sum_reserve);
        // beyond the new order, detour of this order is not needed
        sql += &create_leg(f.order.id, 
          f.order.to,
          leg_pick.to, // == order.to
          leg_pick.place + 2,
          RouteStatus::ASSIGNED,
          DIST[f.order.to as usize][leg_pick.to as usize],
          cmp::min(cmp::min(resrv, f.sum_reserve),  reserve2),
          leg_pick.route_id as i64, 
          max_leg_id,
          leg_pick.passengers as i8, 
          &("route extender SAVE3C".to_string()));  
        // the extended leg should point at the new leg added above
        sql += &update_leg_a_bit2(leg_pick.route_id, leg_pick.id, f.order.from, 
                 DIST[leg_pick.from as usize][f.order.from as usize], 
                 cmp::max(0, cmp::min(leg_pick.reserve - added_cost, f.order.wait - f.wait)), // leg_pick.reserve - reserve_subtr - reserve2
                 leg_pick.passengers as i8);
      }
    } else { // more legs to be extended, possibly
      // !!! we have to update reserves in bettwen pickup and drop-off (see at the end) and after, before are updated above
      if f.idx_to < legs.len() // beyond the last route in the list, here we do not have route_id
          && f.route_id == legs[f.idx_to].route_id {
        sql += &update_reserve_after(leg_pick.route_id, f.dist, legs[f.idx_to].place+1);
      }
      let place_start: i32;
      // PICK-UP first
      if leg_pick.from == f.order.from {
        // SAVE 4
        place_start = leg_pick.place;
      } else {
        // SAVE 4B
        place_start = leg_pick.place + 1;
             // we have to increment 'place' before drop-off INSERTs  
        sql += &update_place_in_legs_after(leg_pick.route_id, leg_pick.place + 1);
        let len_diff: i32 = (DIST[leg_pick.from as usize][f.order.from as usize] + STOP_WAIT + DIST[f.order.from as usize][leg_pick.to as usize]) as i32 - leg_pick.dist;
        let res = cmp::max(0, cmp::min(detour_reserve, leg_pick.reserve - len_diff));
        sql += &create_leg(f.order.id, 
          f.order.from,
          leg_pick.to,
          leg_pick.place + 1,
          RouteStatus::ASSIGNED,
          DIST[f.order.from as usize][leg_pick.to as usize] as i16,
          res,
          leg_pick.route_id as i64, 
          max_leg_id,
          leg_pick.passengers as i8 + 1, 
          &("route extender SAVE4B".to_string()));
        debug!("SAVE4B: route_id={}, wait:{}, detour_res:{}, res:{}", f.route_id, f.wait, detour_reserve, res);
        // the extended leg should point at the new leg added above
        let res = cmp::max(0, cmp::min(res, leg_pick.reserve - res)); // sum of the two legs (reserve) cannot be bigger than the original leg 
        sql += &update_leg_a_bit2(leg_pick.route_id, leg_pick.id, f.order.from, 
                            DIST[leg_pick.from as usize][f.order.from as usize],
                            // previous version: leg_pick.reserve - len_diff
                            cmp::max(0, cmp::min(f.order.wait - f.wait, leg_pick.reserve - res)), // -res, to subtract reserve ffrom the leg above
                            leg_pick.passengers as i8);
      }
      // DROP-OFF
      let place_incr = if place_start > leg_pick.place { 2 } else { 1 };
      // first check if not beyond
      if f.idx_to >= legs.len() // beyond the last route in the list, here we do not have route_id
          || f.route_id != legs[f.idx_to].route_id { // we know that there is no perfect match for 'to', it would be the last leg in a route
        prev_leg = legs[f.idx_to -1];
        debug!("SAVE5: route_id: {}, detour_res: {}, sum_reserve: {}", f.route_id, detour_reserve, f.sum_reserve);
        sql += &create_leg(-1,  // ??
          prev_leg.to,
          f.order.to,
          prev_leg.place + place_incr, 
          RouteStatus::ASSIGNED,
          DIST[prev_leg.to as usize][f.order.to as usize],
          cmp::max(0, detour_reserve - f.sum_reserve), // reserve in all legs cannot be bigger than dist-tour 
          prev_leg.route_id as i64, 
          max_leg_id,
          1, 
          &("route extender SAVE5".to_string()));
      } else { // one of existing legs
        let leg = legs[f.idx_to];
        if leg.to == f.order.to {
          // SAVE 4
          // reserve have been updated in pickup part, but passengers need to increment; notice that 'place'might be incremented by pickup phase!
          // about place_from - if leg_pick.from == f.order.from then there was no INSERT/UPDATE, we have to increment the starting leg (passengers) too
          // but if there was INSERT, then both extended legs are updated already, we have to start from +2
        } else {
          // SAVE 4
          sql += &update_place_in_legs_after(leg_pick.route_id, leg.place + place_incr);
          let len_diff: i32 = (DIST[leg.from as usize][f.order.to as usize] + STOP_WAIT + DIST[f.order.to as usize][leg.to as usize]) as i32 - leg.dist;
          let reserve1 = cmp::max(0, cmp::min(detour_reserve, leg.reserve - len_diff - f.sum_reserve));
          sql += &create_leg(-1, 
            f.order.to,
            leg.to,
            leg.place + place_incr,
            RouteStatus::ASSIGNED,
            DIST[f.order.to as usize][leg.to as usize],
            reserve1,
            leg_pick.route_id as i64, 
            max_leg_id,
            leg.passengers as i8, 
            &("route extender SAVE4C".to_string()));
          // the extended leg should point at the new leg added above
          let reserve_subtr = cmp::min(leg.reserve - detour_reserve, leg.reserve - reserve1); // how much reserve is left for the other leg
          sql += &update_leg_a_bit2(leg.route_id, leg.id, f.order.to, 
            DIST[leg.from as usize][f.order.to as usize],
            cmp::max(0, cmp::min(reserve_subtr, detour_reserve)), // reserve - detour: sum of reserver in 2 legs cannot be bigger than leg.reserve
            leg.passengers as i8 + 1);
        }
      }
      if f.route_id == leg_pick.route_id { // at least pickup is in an existing leg 
        let mut place_start = leg_pick.place;
        if leg_pick.from != f.order.from { place_start += 2; }
        let mut place_stop;
        if f.idx_to < legs.len() && f.route_id == legs[f.idx_to].route_id { 
          place_stop = legs[f.idx_to].place + place_incr - 1;
          if legs[f.idx_to].to != f.order.to { place_stop -= 1; }
        } else { 
          place_stop = legs[f.idx_to -1 ].place;
        }
        sql += &update_passengers_and_reserve_in_legs_between(leg_pick.route_id, cmp::max(0, detour_reserve),
                                                  place_start, place_stop); // TODO: SQL without BETWEEN!
      }
    }
  }
  return sql.to_string();
}
}

pub fn get_handle(conn_str: String, sql: String, label: String)  -> thread::JoinHandle<()> {
  return thread::spawn(move || {
    if sql.len() > 0 {
      let pool = Pool::new(conn_str.as_str());
      match pool {
        Ok(p) => {
          let conn = p.get_conn();
          match conn {
            Ok(mut c) => {
              let res = c.query_iter(sql);
              match res {
                Ok(_) => {}
                Err(err) => {
                  panic!("Could not run SQL batch: {}, err:{}", &label, err);
                }
              }
            }
            Err(err) => {
              panic!("Could not connect to MySQL: {}, err:{}", &label, err);
            }
          }
        }
        Err(err) => {
          panic!("Could not get pool to MySQL: {}, err:{}", &label, err);
        }
      }
    }
  });
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::distance::init_distance;
  use serial_test::serial;

  fn get_test_legs() -> Vec<Leg> {
    unsafe {
    return vec![
      Leg{ id: 0, route_id: 123, from: 0, to: 1, place: 0, dist: DIST[0][1] as i32, reserve:1, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
      Leg{ id: 1, route_id: 123, from: 1, to: 2, place: 1, dist: DIST[1][2] as i32, reserve:2, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
      Leg{ id: 2, route_id: 123, from: 2, to: 3, place: 2, dist: DIST[2][3] as i32, reserve:3, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    ];
    }
  }
  /*
  INSERT INTO leg (id, route_id, from_stand, to_stand, place, distance, reserve, passengers) VALUES (0,123,0,1,0,1,1,1);
  INSERT INTO leg (id, route_id, from_stand, to_stand, place, distance, reserve, passengers) VALUES (1,123,1,2,1,1,2,1);
  INSERT INTO leg (id, route_id, from_stand, to_stand, place, distance, reserve, passengers) VALUES (2,123,2,3,2,1,3,1);
   */

  fn get_test_legs2() -> Vec<Leg> {
    unsafe {
    return vec![
      Leg{ id: 3, route_id: 124, from: 4, to: 5, place: 0, dist: DIST[4][5] as i32, reserve:1, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
      Leg{ id: 0, route_id: 123, from: 0, to: 2, place: 0, dist: DIST[0][2] as i32, reserve:3, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
      Leg{ id: 1, route_id: 123, from: 2, to: 4, place: 1, dist: DIST[2][4] as i32, reserve:5, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
      Leg{ id: 2, route_id: 123, from: 4, to: 5, place: 2, dist: DIST[4][5] as i32, reserve:6, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    ];
    }
  }
  /*
  INSERT INTO leg (id, route_id, from_stand, to_stand, place, distance, reserve, passengers) VALUES (3,124,4,5,0,1,1,1);
  INSERT INTO leg (id, route_id, from_stand, to_stand, place, distance, reserve, passengers) VALUES (0,123,0,2,0,3,3,1);
  INSERT INTO leg (id, route_id, from_stand, to_stand, place, distance, reserve, passengers) VALUES (1,123,2,4,1,3,5,1);
  INSERT INTO leg (id, route_id, from_stand, to_stand, place, distance, reserve, passengers) VALUES (2,123,4,5,2,3,6,1);
   */


  fn get_stops() -> Vec<Stop> {
    return vec![
      Stop{ id: 0, bearing: 0, latitude: 49.0, longitude: 19.000, capacity: 10},
      Stop{ id: 1, bearing: 0, latitude: 49.0, longitude: 19.025, capacity: 10},
      Stop{ id: 2, bearing: 0, latitude: 49.0, longitude: 19.050, capacity: 10},
      Stop{ id: 3, bearing: 0, latitude: 49.0, longitude: 19.075, capacity: 10},
      Stop{ id: 4, bearing: 0, latitude: 49.0, longitude: 19.100, capacity: 10},
      Stop{ id: 5, bearing: 0, latitude: 49.0, longitude: 19.125, capacity: 10}
    ];
  }

  fn test_find_route(from_stand: i32, to_stand: i32, from_idx: usize, to_idx: usize) {
    init_distance(&get_stops(), 30);
    let order1: Order = Order { id: 1, from: from_stand, to: to_stand, wait: 10, loss:90, 
                              dist:unsafe{DIST[from_stand as usize][to_stand as usize] as i32}, 
                              shared: true, received: None, at_time: None, route_id: -1 };
    match find_route(&order1, &mut get_test_legs(), &mut get_stops(), 
                    &HashMap::new(), &HashMap::new(), &KernCfg::new()) {
      Some(x) => {
        assert_eq!(x.route_id, 123);
        assert_eq!(x.idx_from, from_idx);
        assert_eq!(x.idx_to, to_idx);
      },
      None => {
        assert_eq!(1, 2); // fail
      }
    };
  }

  fn test_find_route2(from_stand: i32, to_stand: i32, from_idx: usize, to_idx: usize, route_id: i64) {
    init_distance(&get_stops(), 30);
    let order1: Order = Order { id: 1, from: from_stand, to: to_stand, wait: 10, loss:90, 
                              dist:unsafe{DIST[from_stand as usize][to_stand as usize] as i32},
                              shared: true, received: None, at_time: None, route_id: -1  };
    let mut legs = get_test_legs2();
    match find_route(&order1, &mut legs, &mut get_stops(), &HashMap::new(), 
                &HashMap::new(), &KernCfg::new()) {
      Some(x) => {
        assert_eq!(x.route_id, route_id);
        assert_eq!(x.idx_from, from_idx);
        assert_eq!(x.idx_to, to_idx);
      },
      None => {
        assert_eq!(1, 2); // fail
      }
    };
  }

  fn test_extend_legs_sql(from_stand: i32, to_stand: i32, expected_sql: &str) {
    let mut max_leg_id: &mut i64 = &mut 10;
    init_distance(&get_stops(), 30);
    let orders = vec![Order { id: 1, from: from_stand, to: to_stand, wait: 10, loss:90, 
                                      dist:unsafe{DIST[from_stand as usize][to_stand as usize] as i32}, 
                                      shared: true, received: None, at_time: None, route_id: -1 }];
    let (_ret, _, sql) = extend_routes(&orders, &HashMap::new(),  &get_stops(),
                                                       &mut get_test_legs(), &mut max_leg_id, &KernCfg::new());
    assert_eq!(sql, expected_sql);
  }

  fn test_extend_legs_sql2(from_stand: i32, to_stand: i32, expected_sql: &str) {
    let mut max_leg_id: &mut i64 = &mut 10;
    init_distance(&get_stops(), 30);
    let orders = vec![Order { id: 1, from: from_stand, to: to_stand, wait: 10, loss:90, 
                                      dist:unsafe{DIST[from_stand as usize][to_stand as usize] as i32},
                                      shared: true, received: None, at_time: None, route_id: -1 }];
    let (_ret, _, sql) = extend_routes(&orders, &HashMap::new(), &get_stops(),
                                                         &mut get_test_legs2(), &mut max_leg_id, &KernCfg::new());
    assert_eq!(sql, expected_sql);
  }

  // PERFECT MATCH
  #[test]
  #[serial]
  fn test_find_route_perfect_match() {
    test_find_route(1,3,1,2);
  }

  #[test]
  #[serial]
  fn test_extend_legs_in_db_returns_sql() {
    test_extend_legs_sql(1,3, 
      "UPDATE taxi_order SET route_id=123, leg_id=1, cab_id=(SELECT cab_id FROM route where id=123), status=1, eta=5, in_pool=true WHERE id=1 AND status=0;\nUPDATE leg SET reserve=GREATEST(0, reserve-1) WHERE route_id=123 AND place <= 0;\nUPDATE leg SET reserve=LEAST(reserve, 5) WHERE route_id=123 AND place <= 0;\nUPDATE leg SET reserve=GREATEST(0, reserve-1) WHERE route_id=123 AND place >= 3;\nUPDATE leg SET passengers=passengers+1, reserve=LEAST(reserve, 6) WHERE route_id=123 AND place BETWEEN 1 AND 2;\n");
  }

  // request from stops in between, both 'from' and 'to'
  #[test]
  #[serial]
  #[ignore]
  fn test_find_route_nonperfect_match() {
    test_find_route2(1,3,1,1, 124);
  } 

  #[test]
  #[serial]
  fn test_extend_legs_in_db_returns_sql2() {
    test_extend_legs_sql2(1,3,
      "");
  }
  
  // -----------------------------------------------------------------------------
  // only drop-off is perfect match
  #[test]
  #[serial]
  #[ignore]
  fn test_find_route_nonperfect_match2() {
    test_find_route2(1,4,2,2, 123);
  }

  #[test]
  #[serial]
  fn test_extend_legs_in_db_returns_sql3() {
    test_extend_legs_sql2(1,4, "");
  }

 // only pickup is perfect match - different legs
 #[test]
 #[serial]
 fn test_find_route_nonperfect_match3() {
  test_find_route2(2,3,2,2, 123); // indices of legs table, not IDs i it
 }

// a trip requested from a leg which is not started but is the first leg, which can start any second
 #[test]
 #[serial]
 fn test_extend_legs_in_db_returns_sql4() {
  test_extend_legs_sql2(0,3, "");
 }

// only pickup is perfect match - same legs
#[test]
#[serial]
fn test_find_route_nonperfect_match4() {
  test_find_route2(2,3,2,2, 123);
}

#[test]
#[serial]
fn test_extend_legs_in_db_returns_sql5() {
  // leg_id=1 ???
  test_extend_legs_sql2(2,3, 
    "UPDATE taxi_order SET route_id=123, leg_id=1, cab_id=(SELECT cab_id FROM route where id=123), status=1, eta=9, in_pool=true WHERE id=1 AND status=0;\nUPDATE leg SET reserve=GREATEST(0, reserve-1) WHERE route_id=123 AND place <= 0;\nUPDATE leg SET reserve=LEAST(reserve, 1) WHERE route_id=123 AND place <= 0;\nUPDATE leg SET reserve=GREATEST(0, reserve-1) WHERE route_id=123 AND place >= 2;\nUPDATE leg SET place=place+1 WHERE route_id=123 AND place >= 2;\nINSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (10,3,4,2,3,1,5,123,1);\nUPDATE leg SET to_stand=3, distance=3, reserve=0, passengers=2 WHERE id=1;\n");
}

// only drop-off is perfect match - same legs
#[test]
#[serial]
#[ignore]
fn test_find_route_nonperfect_match_4_b() {
  test_find_route2(1,2,1,1, 124);
}

#[test]
#[serial]
fn test_extend_legs_in_db_returns_sql_5_b() {
  // leg_id=1 ???
  test_extend_legs_sql2(1,2, 
    "");
}

// pick-up & drop-off extending the same leg

fn get_test_legs4() -> Vec<Leg> {
  unsafe {
  return vec![
    Leg{ id: 0, route_id: 123, from: 0, to: 2, place: 0, dist: DIST[0][2] as i32, reserve:3, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 1, route_id: 123, from: 2, to: 5, place: 1, dist: DIST[2][5] as i32, reserve:5, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
  ];
  }
}

fn test_find_route4(route_id: i64, from_stand: i32, to_stand: i32, from_idx: usize, to_idx: usize) {
  init_distance(&get_stops(), 30);
  let order1: Order = Order { id: 1, from: from_stand, to: to_stand, wait: 10, loss:90, 
                            dist:unsafe{DIST[from_stand as usize][to_stand as usize] as i32}, 
                            shared: true, received: None, at_time: None, route_id: -1 };
  match find_route(&order1, &mut get_test_legs4(), &mut get_stops(), 
                    &HashMap::new(), &HashMap::new(), &KernCfg::new()) {
    Some(x) => {
      assert_eq!(x.route_id, route_id);
      assert_eq!(x.idx_from, from_idx);
      assert_eq!(x.idx_to, to_idx);
    },
    None => {
      assert_eq!(1, 1); // fail
    }
  };
}

#[test]
fn test_find_route_extending_one_leg() {
  test_find_route4(123, 3,4,1,1);
}

fn test_extend_legs_sql4(from_stand: i32, to_stand: i32, expected_sql: &str) {
  let mut max_leg_id: &mut i64 = &mut 10;
  init_distance(&get_stops(), 30);
  let orders = vec![Order { id: 1, from: from_stand, to: to_stand, wait: 10, loss:90, 
                                    dist:unsafe{DIST[from_stand as usize][to_stand as usize] as i32}, 
                                    shared: true, received: None, at_time: None, route_id: -1 }];
  let (_ret, _, sql) = extend_routes(&orders, &HashMap::new(), &get_stops(),
                                                       &mut get_test_legs4(), &mut max_leg_id, &KernCfg::new());
  assert_eq!(sql, expected_sql);
}

#[test]
#[serial]
fn test_extend_legs_in_db_returns_sql_5_c() {
  test_extend_legs_sql4(3,4, 
    "");
}

// only drop-off beyond current legs
#[test]
#[serial]
fn test_find_route_nonperfect_match5() {
  test_find_route(2,4,2,3);
}

#[test]
#[serial]
fn test_extend_legs_in_db_returns_sql6() {
  test_extend_legs_sql(2,4,  
    "UPDATE taxi_order SET route_id=123, leg_id=2, cab_id=(SELECT cab_id FROM route where id=123), status=1, eta=9, in_pool=true WHERE id=1 AND status=0;\nUPDATE leg SET reserve=GREATEST(0, reserve-0) WHERE route_id=123 AND place <= 1;\nUPDATE leg SET reserve=LEAST(reserve, 1) WHERE route_id=123 AND place <= 1;\nINSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (10,3,4,3,3,1,3,123,1);\nUPDATE leg SET passengers=passengers+1, reserve=LEAST(reserve, 6) WHERE route_id=123 AND place BETWEEN 2 AND 2;\n");
}

// both pickup and drop-off beyond current legs
#[test]
#[serial]
#[ignore]
fn test_find_route_nonperfect_match6() {
  test_find_route(4,5,3,3);
}

#[test]
#[serial]
fn test_extend_legs_in_db_returns_sql7() {
  test_extend_legs_sql(4,5, 
    "");
}

// now two matching routes, one is better
fn get_test_legs3() -> Vec<Leg> {
  unsafe {
  return vec![
    Leg{ id: 100, route_id: 124, from: 4, to: 5, place: 0, dist: DIST[4][5] as i32, reserve:1, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 0, route_id: 123, from: 0, to: 2, place: 0, dist: DIST[0][2] as i32, reserve:3, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 1, route_id: 123, from: 2, to: 4, place: 1, dist: DIST[2][4] as i32, reserve:5, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 2, route_id: 123, from: 4, to: 5, place: 2, dist: DIST[4][5] as i32, reserve:6, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 3, route_id: 125, from: 0, to: 1, place: 0, dist: DIST[0][1] as i32, reserve:3, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 4, route_id: 125, from: 1, to: 2, place: 1, dist: DIST[1][2] as i32, reserve:5, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 5, route_id: 125, from: 2, to: 3, place: 2, dist: DIST[2][3] as i32, reserve:6, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 6, route_id: 126, from: 0, to: 1, place: 0, dist: DIST[0][1] as i32, reserve:5, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 7, route_id: 126, from: 1, to: 4, place: 1, dist: DIST[1][4] as i32, reserve:6, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 8, route_id: 126, from: 4, to: 5, place: 2, dist: DIST[4][5] as i32, reserve:3, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
  ];
  }
}

fn test_find_route3(route_id: i64, from_stand: i32, to_stand: i32, from_idx: usize, to_idx: usize) {
  init_distance(&get_stops(), 30);
  let order1: Order = Order { id: 1, from: from_stand, to: to_stand, wait: 10, loss:90, 
                            dist:unsafe{DIST[from_stand as usize][to_stand as usize] as i32}, 
                            shared: true, received: None, at_time: None, route_id: -1 };
  match find_route(&order1, &mut get_test_legs3(), &mut get_stops(), 
                    &HashMap::new(), &HashMap::new(), &KernCfg::new()) {
    Some(x) => {
      assert_eq!(x.route_id, route_id);
      assert_eq!(x.idx_from, from_idx);
      assert_eq!(x.idx_to, to_idx);
    },
    None => {
      assert_eq!(1, 2); // fail
    }
  };
}

#[test]
#[serial]
fn test_find_route_more_matching_routes() {
  test_find_route3(125, 1,3,5,6);
}
// test no match

fn test_extend_legs_no_match(from_stand: i32, to_stand: i32) {
  let mut max_leg_id: &mut i64 = &mut 10;
  init_distance(&get_stops(), 30);
  let orders = vec![Order { id: 1, from: from_stand, to: to_stand, wait: 1, loss:1, 
                                    dist:unsafe{DIST[from_stand as usize][to_stand as usize] as i32}, 
                                    shared: true, received: None,  at_time: None, route_id: -1 }];
  let (ret, _, sql) = extend_routes(&orders, &HashMap::new(), &get_stops(),
                                                       &mut get_test_legs2(), &mut max_leg_id, &KernCfg::new());
  assert_eq!(sql, "");
  assert_eq!(ret.len(), 1);
}

#[test]
#[serial]
fn test_extend_legs_in_db_returns_no_sql() {
  test_extend_legs_no_match(5,0);
}

// test of two identical orders - only one should go thru
fn test_extend_legs_identical_orders(from_stand: i32, to_stand: i32) {
  let mut max_leg_id: &mut i64 = &mut 10;
  init_distance(&get_stops(), 30);
  let orders = vec![
    Order { id: 1, from: from_stand, to: to_stand, wait: 10, loss:90, dist:unsafe{DIST[from_stand as usize][to_stand as usize] as i32},
            shared: true, received: None,  at_time: None,  route_id: -1 },
    Order { id: 2, from: from_stand, to: to_stand, wait: 10, loss:90, dist:unsafe{DIST[from_stand as usize][to_stand as usize] as i32},
            shared: true, received: None,  at_time: None,  route_id: -1 }];
  let (ret, _, sql) = extend_routes(&orders, &HashMap::new(), &get_stops(),
                                                       &mut get_test_legs2(), &mut max_leg_id, &KernCfg::new());
  assert_eq!(sql, "UPDATE taxi_order SET route_id=123, leg_id=1, cab_id=(SELECT cab_id FROM route where id=123), status=1, eta=9, in_pool=true WHERE id=1 AND status=0;\nUPDATE leg SET reserve=GREATEST(0, reserve-0) WHERE route_id=123 AND place <= 0;\nUPDATE leg SET reserve=LEAST(reserve, 1) WHERE route_id=123 AND place <= 0;\nUPDATE leg SET reserve=GREATEST(0, reserve-0) WHERE route_id=123 AND place >= 2;\nUPDATE leg SET passengers=passengers+1, reserve=LEAST(reserve, 6) WHERE route_id=123 AND place BETWEEN 1 AND 1;\n");
  assert_eq!(ret.len(), 0); // nothing should go to pool finder, one order should be allocated by extender at next iteration 
}

#[test]
fn test_extend_legs_two_identical_orders() {
  test_extend_legs_identical_orders(2,4);
}

// testing wait time
fn get_test_legs5() -> Vec<Leg> {
  unsafe {
  return vec![
    Leg{ id: 0, route_id: 123, from: 0, to: 1, place: 0, dist: DIST[0][1] as i32, reserve:3, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 1, route_id: 123, from: 1, to: 2, place: 1, dist: DIST[1][2] as i32, reserve:5, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 2, route_id: 123, from: 2, to: 3, place: 2, dist: DIST[2][3] as i32, reserve:6, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 3, route_id: 123, from: 3, to: 4, place: 0, dist: DIST[3][4] as i32, reserve:3, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
    Leg{ id: 4, route_id: 123, from: 4, to: 5, place: 1, dist: DIST[4][5] as i32, reserve:5, started: None, status: RouteStatus::ASSIGNED, passengers:1, seats: 10},
  ];
  }
}

fn test_find_route_wait_time_exceeded(from_stand: i32, to_stand: i32) {
  init_distance(&get_stops(), 30);
  let order1: Order = Order { id: 1, from: from_stand, to: to_stand, wait: 5, loss:90, 
                            dist:unsafe{DIST[from_stand as usize][to_stand as usize] as i32},  
                            shared: true, received: None, at_time: None,  route_id: -1 };
    assert!(find_route(&order1, &mut get_test_legs5(), &mut get_stops(), &HashMap::new(), &HashMap::new(), &KernCfg::new()).is_none());
}

#[test]
#[serial]
fn test_find_route_wait_time() {
  test_find_route_wait_time_exceeded(4,5);
}

#[test]
#[serial]
fn test_wait_exceed_no_assigned_orders_then_false() {
  let o = Order { id: 1, from: 4, to: 5, wait: 5, loss:90, 
                        dist:unsafe{DIST[4][5] as i32}, 
                        shared: true, received: None, at_time: None,  route_id: 12 };
  let ass_orders = vec![o];
  let ass_orders_map = assigned_orders(&ass_orders);  
  let ret = wait_exceeded(&o, 0, 0, 1, 2, unsafe{DIST[4][5] as i32}, 1, 1, &get_test_legs5(), &ass_orders_map);
  assert!(!ret);
}

#[test]
#[serial]
fn test_wait_exceed_assigned_order_and_too_long_then_true() {
  init_distance(&get_stops(), 30);
  let o = Order { id: 1, from: 4, to: 5, wait: 5, loss:90, 
              dist:unsafe{DIST[4][5] as i32}, 
              shared: true, 
              received: Local::now().naive_local().checked_sub_signed(chrono::Duration::seconds(3*60)), // ! three minutes are enough to exceed the wait time
     at_time: None,  route_id: 123 };
  let o2 = Order { id: 12345, from: 4, to: 5, wait: 5, loss:90, 
                          dist:unsafe{DIST[4][5] as i32}, 
                          shared: true, 
                          received: Some(Local::now().naive_local()),
                          at_time: None, route_id: 123 };
  let ass_orders = vec![o];
  let ass_orders_map = assigned_orders(&ass_orders);  
  let ret = wait_exceeded(&o2, 0, 0, 1, 2, unsafe{DIST[4][5] as i32}, 1, 1, &get_test_legs5(), &ass_orders_map);
  assert!(ret);
}

#[test]
#[serial]
fn test_wait_exceed_assigned_order_and_not_too_long_then_false() {
  init_distance(&get_stops(), 30);
  let o = Order { id: 1, from: 4, to: 10, wait: 10, loss:90, 
                        dist:unsafe{DIST[4][5] as i32}, shared: true, 
                        received: Local::now().naive_local().checked_sub_signed(chrono::Duration::seconds(60)), // one minute only
                        at_time: None,  route_id: 123 };
  let ass_orders = vec![o];
  let ass_orders_map = assigned_orders(&ass_orders);  
  let ret = wait_exceeded(&o, 0, 0, 1, 2, unsafe{DIST[4][5] as i32}, 1, 1, &get_test_legs5(), &ass_orders_map);
  //each leg = 1min distance + 1min at the stop
  // four legs = 4*1 + 3*1 = 7 min. + 1min of waittime since 'received'. Should be OK
  assert!(!ret);
}

}