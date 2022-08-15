use std::io::Write;
use std::{thread, cmp};
use log::{debug,info,warn};
//use std::io::Write;
use postgres::{Client, NoTls};
use crate::model::{ Order, Stop, Leg, RouteStatus };
use crate::repo::{CNFG,find_legs_by_status,assign_order_find_cab, update_places_in_legs,
                  create_leg,update_leg_a_bit,update_leg_with_route_id};
use crate::distance::DIST;
use crate::pool::bearing_diff;
use crate::stats::{add_avg_element, Stat};
use crate::utils::get_elapsed;

#[derive(Clone, Copy)]
struct LegIndicesWithDistance {
    idx_from: i32,
    idx_to: i32, 
    dist: i32,
    order: Order
}

pub fn find_matching_routes(thr_numb: i32, itr: i32, host: &String, client: &mut Client, demand: &Vec<Order>, stops: &Vec<Stop>, 
                            max_leg_id: &mut i64) -> (Vec<Order>, thread::JoinHandle<()>) {

    //return (demand.to_vec(), thread::spawn(|| { }));

    let mut t_numb = thr_numb;
    if demand.len() == 0 {
        return (Vec::new(), thread::spawn(|| { }));
    }
    let mut legs: Vec<Leg> = find_legs_by_status(client, RouteStatus::ASSIGNED);
    if legs.len() == 0 {
        return (demand.to_vec(), thread::spawn(|| { }));
    }
    info!("Extender START, orders count={} legs count={}", demand.len(), legs.len());
    let mut ret: Vec<Order> = Vec::new();
    let mut sql_bulk: String = String::from("");
    let mut feasible: Vec<LegIndicesWithDistance> = Vec::new();
    let mut extended: Vec<i32> = vec![]; // to help skip legs indices, feasible extensions which match other extensions 
    let mut children = vec![];

    // divide the task into threads
    let chunk_size: f32 = demand.len() as f32 / t_numb as f32;
    if ((t_numb as f32 * chunk_size).round() as i16) < demand.len() as i16 { 
      t_numb += 1; 
    } // last thread will be the reminder of division
    
    for i in 0..t_numb { 
      let orders = demand.to_vec();
      let bus_stops = stops.to_vec();
      let legs_cpy = legs.to_vec();

      children.push(thread::spawn(move || {
        let mut part_feas: Vec<LegIndicesWithDistance> = Vec::new();
        let mut part_order: Vec<Order> = Vec::new();
        let start = (i as f32 * chunk_size).round() as i32;
        let mut stop = ((i + 1) as f32 * chunk_size).round() as i32;
        stop = if stop > orders.len() as i32 { orders.len() as i32 } else { stop };

        for o in start..stop {
            let taxi_order = orders[o as usize];
            let feas = try_to_extend_route(&taxi_order, &legs_cpy, &bus_stops);
            match feas {
              Some(x) => part_feas.push(x),
              None => part_order.push(taxi_order) // it will go to pool
            }
        }
        return (part_feas, part_order);
      }));
    }

    for handle in children {
      let mut cpy : (Vec<LegIndicesWithDistance>, Vec<Order>) = handle.join().unwrap();
      feasible.append(&mut cpy.0);
      ret.append(&mut cpy.1);
    }

    let mut count_skipped = 0;
    for f in feasible.iter() {
      // TASK: MAX LOSS check
      if extended.contains(&f.idx_from) || extended.contains(&f.idx_to) {
        ret.push(f.order); // this feasible case colides with other, which modified a leg
        count_skipped += 1;
        continue;
      }

      let from_leg: Leg = legs[f.idx_from as usize];
      let to_leg: Leg = legs[f.idx_to as usize];
      
      if f.order.from != from_leg.from { // leg will be added
        extended.push(f.idx_from);
      }
      if f.order.to != to_leg.to { 
        extended.push(f.idx_to);
      } 
      sql_bulk += &modify_legs(f, max_leg_id, &mut legs);
    }
    info!("Extender STOP, rest orders count={}", ret.len());
    if count_skipped > 0 {
      info!("Matching but skipped by extender: {}", count_skipped);
    }
    write_sql_to_file(itr, &sql_bulk, "route_extender");
    // EXECUTE SQL !!
    let handle = get_handle(host.clone(), sql_bulk, "extender".to_string());
    return (ret, handle);
}

pub fn write_sql_to_file(itr: i32, sql: &String, label: &str) {
  let file_name = format!("{}-{}.sql", label.to_string(), itr);
  let msg = format!("SQL for {} failed", file_name);
  let mut file = std::fs::File::create(&file_name).expect(&("Create ".to_string() + &msg));
  file.write_all(sql.as_bytes()).expect(&("Write ".to_string() + &msg));
}

fn check_if_perfect_match_from(from: i32, legs: &Vec<Leg>, start: usize) -> usize {
  let mut i = start;
  while i<legs.len() && legs[i].route_id == legs[start].route_id {
    if legs[i].from == from {
      return i;
    }
    i += 1;
  }
  return start;
}

fn check_if_perfect_match_to(to: i32, legs: &Vec<Leg>, start: usize) -> usize {
  let mut i = start;
  while i<legs.len() && legs[i].route_id == legs[start].route_id {
    if legs[i].to == to {
      return i;
    }
    i += 1;
  }
  return start;
}

// this algo is not ideal, finds just first satisfying solution, but not the best

fn try_to_extend_route(demand: &Order, legs: &Vec<Leg>, stops: &Vec<Stop>) -> Option<LegIndicesWithDistance> {
  unsafe {
    let mut feasible: Vec<LegIndicesWithDistance> = Vec::new();
    let mut i = check_if_perfect_match_from(demand.from, legs, 1); // we do not start at 0 as there must be at least one leg before 'not started', assigned, so that cab has time to get an extended route
    let mut initial_distance: i16 = 0;
    while i < legs.len() {
      // not from 0 as each leg we are looking for must have a non-completed predecessor
      // routes from the same stand which have NOT started will surely be seen by passengers,
      // they can get aboard
      let leg: Leg = legs[i];
      // we should get only not-completed legs, but ...
      if leg.status == RouteStatus::COMPLETED as i32 || leg.id == -1 {// some bug in modify_leg / extends_legs_in_vec ?
        i += 1;
        continue; 
      }
      // firts find perfect match when new route to check
      if leg.route_id != legs[i-1].route_id {
        initial_distance = 0; 
        i = check_if_perfect_match_from(demand.from, legs, i);
      } else if initial_distance as i32 > demand.wait {
        i += 1;
        continue; // this means iterate to the next route
      }
      let not_too_long: bool = count_legs(leg.route_id, legs) <= CNFG.max_legs;
      
      let mut distance_diff = DIST[leg.from as usize][demand.from as usize] + CNFG.stop_wait +
                                    DIST[demand.from as usize][leg.to as usize] - leg.dist as i16;
      // with dist_allowed we get a reserve in the route, here we check maxloss of other orders

      if demand.from != leg.to // == means direct hit in the next leg/iteration, leg.from; here we miss one possibility, where leg is the last leg in that route
          // previous leg is from the same route
          && legs[i - 1].route_id == leg.route_id
          // the previous leg cannot be completed TASK!! in the future consider other statuses here
          && legs[i - 1].status != RouteStatus::COMPLETED as i32
          // check MAXLOSS
          && ((demand.from == leg.from && demand.wait >= initial_distance as i32 )// direct hit and maxwait check
            || (not_too_long
                && leg.reserve >= distance_diff as i32 // new path - old path length; integers so it can even be negative :) we will check it
                && demand.wait >= (initial_distance + DIST[leg.from as usize][demand.from as usize]) as i32 // MAXWAIT
                //&& ((distance_diff + CNFG.stop_wait) as f32) < (leg.dist as f32) * CNFG.extend_margin
                && bearing_diff(stops[leg.from as usize].bearing, stops[demand.from as usize].bearing) < CNFG.max_angle
                && bearing_diff(stops[demand.from as usize].bearing, stops[leg.to as usize].bearing) < CNFG.max_angle
                ) // 5% TASK - global config, wait at stop?
            )
      // we want the previous leg to be active
      // to give some time for both parties to get the assignment
      {
        // OK, so we found the first 'pickup' leg, either direct hit or can be extended
        let mut to_found: bool = false;
        let mut distance_in_pool: i16 = 0;
        // we have found "from", now let's find "to"
        let mut k = check_if_perfect_match_to(demand.to, legs, i); // "to might be in the same leg as "from", we have to start from 'i'
        while k < legs.len() {
          if i != k { // 'i' countet already
            distance_in_pool += legs[k].dist as i16 + CNFG.stop_wait;
          }
          if legs[k].route_id != leg.route_id {
            initial_distance = 0; // new route
            // won't find; this leg is the first leg in the next route and won't be checked as i++
            break;
          }
          if demand.to == legs[k].to { // direct hit
            to_found = true;
            break;
          }
          if k == i { // we extend the same leg as in "from phase" 
            distance_diff = DIST[legs[k].from as usize][demand.from as usize] + CNFG.stop_wait + 
                            DIST[demand.from as usize][demand.to as usize] + CNFG.stop_wait +
                            DIST[demand.to as usize][legs[k].to as usize] - legs[k].dist as i16;
          } else {
            distance_diff = DIST[legs[k].from as usize][demand.to as usize] + CNFG.stop_wait +
                             DIST[demand.to as usize][legs[k].to as usize] - legs[k].dist as i16;
          }

          if not_too_long
              && legs[k].reserve >= distance_diff as i32
              // && ((distance_diff + CNFG.stop_wait) as f32) < (legs[k].dist as f32) * CNFG.extend_margin
              && bearing_diff(stops[legs[k].from as usize].bearing, stops[demand.to as usize].bearing) < CNFG.max_angle
              && bearing_diff(stops[demand.to as usize].bearing, stops[legs[k].to as usize].bearing) < CNFG.max_angle {
            // passenger is dropped before "getToStand", but the whole distance is counted above
            distance_in_pool -= DIST[demand.to as usize][legs[k].to as usize];
            to_found = true;
            break;
          }
          k += 1;
        }
        if to_found 
            && demand.wait as i16 >= initial_distance
            // TASK: maybe distance*maxloss is a performance bug,
            // distanceWithLoss should be stored and used
            && (1.0 + demand.loss as f32 / 100.0) * demand.dist as f32 >= distance_in_pool as f32 {
            feasible.push(LegIndicesWithDistance{
                idx_from: i as i32, 
                idx_to: k as i32, 
                dist: (initial_distance + distance_in_pool) as i32,
                order: *demand
            });
        }
        i = k;
      }
      if leg.status == RouteStatus::ASSIGNED as i32 || leg.status == RouteStatus::ACCEPTED as i32 {
        initial_distance += leg.dist as i16 + CNFG.stop_wait;
      }
      i += 1;
    }
    // TASK: sjekk if demand.from == last leg.toStand - this might be feasible
    if feasible.len() == 0 { // empty
        return None;
    }
    feasible.sort_by_key(|e| e.dist.clone());
    return Some(feasible[0]); 
  }
}
  
fn modify_legs(f: &LegIndicesWithDistance, max_leg_id: &mut i64, legs: &mut Vec<Leg>) -> String {
    let demand = f.order;
    let from_leg: Leg = legs[f.idx_from as usize];

    // pickup phase
    let mut sql: String = String::from("");
    debug!("Order {} assigned to existing route: {}", demand.id, from_leg.route_id);
    debug!("Modifying legs, idx_from={}, idx_to={}, from_leg_id={}, to_leg_id={}", 
          f.idx_from, f.idx_to, from_leg.id, legs[f.idx_to as usize].id);
    if demand.from == from_leg.from { // direct hit, we don't modify that leg
      // TODO: eta should be calculated !!!!!!!!!!!!!!!!!!!!!
      sql += &assign_order_find_cab(demand.id, from_leg.id, from_leg.route_id, 0, "matchRoute IN");
    } else { 
      sql += &extend_legs_in_db(&demand, -1, f.idx_from, legs, demand.from, from_leg.place, 
                                from_leg.id, max_leg_id, "IN");
      // now assign the order to the new leg
      sql += &assign_order_find_cab(demand.id, *max_leg_id -1 , from_leg.route_id, 0, "extendRoute IN");
      //sql += &assign_order_find_leg_cab(demand.id, 
      //                            from_leg.place + 1, from_leg.route_id, 0, "extendRoute IN");
      // modify 'legs' vector
      update_places((f.idx_from + 1) as usize, legs);
      // now do it all in the copy of the database - "legs" vector
      // legs will be used for another order, the list must be updated
      //extends_legs_in_vec(from_leg, demand.from, idxs.idx_from, legs, *max_leg_id - 1); 
      // extend_legs executes create_leg, which increments id
    }
    add_avg_element(Stat::AvgOrderAssignTime, get_elapsed(demand.received));
    // drop-off phase
    let to_leg: Leg = legs[f.idx_to as usize];
    if demand.to != to_leg.to { // one leg more, ignore situation with ==
      // if from_leg == to_leg, which means we put a customer's 'from' and 'to'between to stops of a route,
      // then two things change - one more leg there, 'place'+1, and we will modify the new leg inserted above, 
      // not the leg stored in 'legs', the ID will differ
      if f.idx_from == f.idx_to {
        sql += &extend_legs_in_db(&demand, f.idx_from, f.idx_to, legs, demand.to, to_leg.place + 1,
                                 *max_leg_id -1, max_leg_id, "OUT");
      } else {
        sql += &extend_legs_in_db(&demand, f.idx_from, f.idx_to, legs, demand.to, to_leg.place, 
                                  to_leg.id, max_leg_id, "OUT");
      }
      update_places((f.idx_to + 1) as usize, legs);
      //extends_legs_in_vec(to_leg, demand.to, idxs.idx_to, legs, *max_leg_id - 1);
    }
    return sql;
}

fn update_places(idx: usize, legs: &mut Vec<Leg>) {
  if idx >= legs.len() {
    return; //nothing to correct
  }
  let mut i = idx;
  while i < legs.len() && legs[i].place != 0  { // 0 means the next route, legs should be sorted
    legs[i].place += 1; 
    i += 1;
  }
}
/*
fn extends_legs_in_vec(leg: Leg, from: i32, idx:i32, legs: &mut Vec<Leg>, leg_id: i64) {
  unsafe {
    // new leg
    let new_leg = Leg { 
        id: leg_id, // we don't know what it will be during insert
        route_id: leg.route_id,
        from: from,
        to: leg.to,
        place: leg.place + 1,
        dist:  DIST[from as usize][leg.to as usize] as i32,
        status: RouteStatus::ASSIGNED as i32,
        completed: None,
        started: None
    };
    legs.insert(idx as usize + 1, new_leg);
    // old leg
    legs[idx as usize].to = from;
    legs[idx as usize].dist = 
            DIST[leg.from as usize][from as usize] as i32;

    // now "place" in route for next legs has to be incremented
    let mut i = idx as usize + 2;
    while i < legs.len() && legs[i].route_id == leg.route_id {
        legs[i].place += 1;
        i += 1;
    }
  }
}
  */
fn extend_legs_in_db(order: &Order, from_leg_idx: i32, leg_idx: i32, legs: &mut Vec<Leg>, from: i32,
                     place: i32, leg_id: i64, max_leg_id: &mut i64, label: &str) -> String {
  unsafe {
    let leg: Leg = legs[leg_idx as usize];
    let mut sql: String = String::from("");
    let mut distance_diff: i32 = 0;
    let mut explain: String = String::from("");
    if from_leg_idx == -1 || from_leg_idx != leg_idx {
      distance_diff = (DIST[leg.from as usize][from as usize] + CNFG.stop_wait 
                      + DIST[from as usize][leg.to as usize]) as i32 - leg.dist;
    }
    else { // OUT leg extends the same leg as IN
      distance_diff = (DIST[leg.from as usize][order.from as usize] + CNFG.stop_wait 
                      + DIST[order.from as usize][order.to as usize]+ CNFG.stop_wait
                      + DIST[order.to as usize][leg.to as usize]) as i32 - leg.dist;
    }
    let mut reserve: i32 = leg.reserve - distance_diff; // both legs will get the reserve
    if reserve < 0 {
      warn!("Negative reserve while extending leg {} leg_id={}, route_id={}, place={}", 
            label, leg.id, leg.route_id, place + 1);
      reserve = 0;
    }
    if from_leg_idx == -1 || from_leg_idx != leg_idx {
      explain = format!("prev_reserve={}-distance_diff={}=new_reserve={}, diff={}+{}+{}-{}",
                        leg.reserve, distance_diff, reserve, 
                        DIST[leg.from as usize][from as usize], CNFG.stop_wait,
                        DIST[from as usize][leg.to as usize], leg.dist)   
    } else {
      explain = format!("prev_reserve={}-distance_diff={}=new_reserve={}, diff={}+{}+{}+{}+{}-{}",
                        leg.reserve, distance_diff, reserve, 
                        DIST[leg.from as usize][order.from as usize], CNFG.stop_wait,
                        DIST[order.from as usize][order.to as usize], CNFG.stop_wait,
                        DIST[order.to as usize][leg.to as usize], leg.dist)   
    }
    debug!("new, extended {} leg_id={}, route_id={}, place={}, {}", label, leg.id, leg.route_id, place + 1, explain);
    // we will add a new leg on "place"
    // we have to increment places in all subsequent legs
    // +
    //we have to modify reserves in DB and in Vec:
    //- all requests starting after added leg will get worse WAIT reserve, by 'distance_diff'
    //- all requests with 'i' before and 'o' after will get worse LOSS reserve, by 'distance_diff'
    //- the total reserve of a leg is MIN of both (this is not relevant here, maybe)
    // the trouble is - we do not have information about 'i'/'o', so just decrease the all
    sql += &update_places_in_legs(leg.route_id, place + 1, distance_diff);
    
    //decrease reserve also in Vec, not only in DB
    for idx in leg_idx as usize +1 .. legs.len() {
      if legs[idx-1].route_id != legs[idx].route_id { // new route
        break; 
      }
      legs[idx].reserve = cmp::max(0, legs[idx].reserve - distance_diff);
    }
    // one leg more in that free place
    sql += &create_leg( order.id, 
                        from,
                        leg.to,
                        place + 1,
                        RouteStatus::ASSIGNED,
                        DIST[from as usize][leg.to as usize],
                        reserve,
                        leg.route_id as i64, 
                        max_leg_id, // TODO: all IDs should be i64
                        &("route extender ".to_string() + &label.to_string()));

    // modify existing leg (to_stand, dist, reserve) so that it goes to a new waypoint in-between
    // when extender puts both IN and OUT into one 
    // but somehow we managed to extend many time - a bug to be fixed ... now
    legs[leg_idx as usize].reserve = reserve;
    if leg_id != -1 {
      sql += &update_leg_a_bit(leg.route_id, leg_id, from, 
                DIST[leg.from as usize][from as usize], reserve);
    } else { // less efficient & more risky (there can always be a bug in "placing")
      sql += &update_leg_with_route_id(leg.route_id, place, from, 
                DIST[leg.from as usize][from as usize], reserve);
    }
    return sql;
  }
}

fn count_legs(id: i64, legs: &Vec<Leg>) -> i8 {
    let mut count: i8 = 0;
    for l in legs.iter() {
        if l.route_id == id {
            count += 1;
        }
    }
    return count;
}

pub fn get_handle(host: String, sql: String, label: String)  -> thread::JoinHandle<()> {
  return thread::spawn(move || {
      match Client::connect(&host, NoTls) {
          Ok(mut c) => {
              if sql.len() > 0 {
                  match c.batch_execute(&sql) {
                    Ok(_) => {}
                    Err(err) => {
                      panic!("Could not run SQL batch: {}, err:{}", &label, err);
                    }
                  }
              }
          }
          Err(err) => {
              panic!("Could not connect DB in: {}, err:{}", &label, err);
          }
      }
  });
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_add() {
      let mut legs: Vec<Leg> = vec![];
      legs.push(Leg{ id: 1, route_id: 123, from: 1, to: 2, place: 0, dist: 1, reserve: 0, started: None, completed: None, status: 0});
      assert_eq!(count_legs(123, &legs), 1);
  }

  /*
  #[test]
  fn test_extend_legs_in_db_returns_sql() {
    let leg = Leg{ id: 1, route_id: 123, from: 1, to: 2, place: 0, dist: 1, reserve: 0, started: None, completed: None, status: 0};
    let max_leg_id: &mut i64 = &mut 1;
    let sql = extend_legs_in_db(&leg, 2, 1, 123, max_leg_id, "label");
    assert_eq!(sql, "UPDATE leg SET place=place+1 WHERE route_id=123 AND place >= 2;\n\
        INSERT INTO leg (id, from_stand, to_stand, place, distance, status, route_id) VALUES (1,2,2,2,0,1,123);\n\
        UPDATE leg SET to_stand=2, distance=0 WHERE id=123;\n");
  }
*/
  #[test]
  fn test_update_places() {
    let mut legs: Vec<Leg> = vec![
      Leg{ id: 1, route_id: 123, from: 1, to: 2, place: 0, dist: 1, reserve:0, started: None, completed: None, status: 0},
      Leg{ id: 1, route_id: 123, from: 1, to: 2, place: 1, dist: 1, reserve:0, started: None, completed: None, status: 0}
    ];
    update_places(1, &mut legs);
    assert_eq!(legs[1].place, 2);
  }

  #[test]
  fn test_try_to_extend_route_when_perfect_match() {
    let legs: Vec<Leg> = vec![
      Leg{ id: 0, route_id: 123, from: 0, to: 1, place: 0, dist: 1, reserve:0, started: None, completed: None, status: 0},
      Leg{ id: 1, route_id: 123, from: 1, to: 2, place: 1, dist: 1, reserve:0, started: None, completed: None, status: 0},
      Leg{ id: 2, route_id: 123, from: 2, to: 3, place: 0, dist: 1, reserve:0, started: None, completed: None, status: 0},
    ];
    let order1= Order{ id: 1, 
      from: 1,
      to: 2,
      wait: 10,loss: 50,dist: 2,shared: true,in_pool: false,received: None,started: None,completed: None,at_time: None,eta: 0,
    };
    let order2= Order{ id: 1, 
      from: 1,
      to: 3, // !!!
      wait: 10,loss: 50,dist: 2,shared: true,in_pool: false,received: None,started: None,completed: None,at_time: None,eta: 0,
    };
    let stops: Vec<Stop> = vec![
      Stop{ id: 0, bearing: 0, latitude: 1.0, longitude: 1.0},
      Stop{ id: 1, bearing: 0, latitude: 1.000000001, longitude: 1.000000001},
      Stop{ id: 2, bearing: 0, latitude: 1.000000002, longitude: 1.000000002},
      Stop{ id: 3, bearing: 0, latitude: 1.000000003, longitude: 1.000000003}
    ];
    let mut indices: LegIndicesWithDistance = LegIndicesWithDistance {
      idx_from: -1, idx_to: -1, dist: 0, order: order1
    };
    /*
    match try_to_extend_route(&order1, &legs, &stops) {
      Some(x) => indices = x,
      None => {}
    }
    assert_eq!(indices.idx_from, 1);
    assert_eq!(indices.idx_to, 1);
   
    match try_to_extend_route(&order2, &legs, &stops) {
      Some(x) => indices = x,
      None => {}
    }
    assert_eq!(indices.idx_from, 1);
    assert_eq!(indices.idx_to, 2);
    */
  }
  
  
}