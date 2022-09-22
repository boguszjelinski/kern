use std::io::Write;
use std::{thread, cmp};
use log::{debug,info,warn};
//use std::io::Write;
use postgres::{Client, NoTls};
use crate::model::{ Order, Stop, Leg, RouteStatus };
use crate::repo::{CNFG,find_legs_by_status,assign_order_find_cab, update_place_and_reserve_in_legs_after,
                  create_leg,update_leg_a_bit,update_leg_with_route_id,update_reserves_in_legs_before_and_including,
                  update_reserve_in_legs_between, update_passengers, read_stops};
use crate::distance::{DIST, init_distance};
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

pub fn find_matching_routes(_thr_numb: i32, itr: i32, host: &String, client: &mut Client, demand: &Vec<Order>, stops: &Vec<Stop>, 
                            max_leg_id: &mut i64) -> (Vec<Order>, thread::JoinHandle<()>) {

    if demand.len() > unsafe { CNFG.max_extender_size } as usize {
      return (demand.to_vec(), thread::spawn(|| { }));
    }
    if demand.len() == 0 {
        return (Vec::new(), thread::spawn(|| { }));
    }
    let mut legs: Vec<Leg> = find_legs_by_status(client, RouteStatus::ASSIGNED);
    if legs.len() == 0 {
        return (demand.to_vec(), thread::spawn(|| { }));
    }
    info!("Extender START, orders count={} legs count={}", demand.len(), legs.len());
    
    let (ret, sql_bulk) = extend_routes(itr, demand, stops, &mut legs, max_leg_id);

    // EXECUTE SQL !!
    let handle = get_handle(host.clone(), sql_bulk, "extender".to_string());
    return (ret, handle);
}

fn extend_routes(itr: i32, demand: &Vec<Order>, stops: &Vec<Stop>, legs: &mut Vec<Leg>, max_leg_id: &mut i64) -> (Vec<Order>, String) {
  let mut ret: Vec<Order> = Vec::new();
  let mut sql_bulk: String = String::from("");

  for o in demand { 
    let feasible = try_to_extend_route(&o, &legs, &stops);

    match feasible {
      Some(mut f) => { 
        sql_bulk += &modify_legs(&mut f, max_leg_id, legs);
      },
      None => ret.push(*o) // it will go to pool
    }
  }
  info!("Extender STOP, rest orders count={}", ret.len());
  write_sql_to_file(itr, &sql_bulk, "route_extender");
  return (ret, sql_bulk);
}

pub fn write_sql_to_file(itr: i32, sql: &String, label: &str) {
  let file_name = format!("{}-{}.sql", label.to_string(), itr);
  let msg = format!("SQL for {} failed", file_name);
  let mut file = std::fs::File::create(&file_name).expect(&("Create ".to_string() + &msg));
  file.write_all(sql.as_bytes()).expect(&("Write ".to_string() + &msg));
  
}

// try to find a matching leg, if not found - return the starting index so that we could look for non-prefect matches
fn check_if_perfect_match_from(from: i32, route_id: i64, legs: &Vec<Leg>, start: usize) -> usize {
  let mut i = start;
  while i<legs.len() && route_id == legs[start].route_id {
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
  if legs.len() < 2 {
    return None;
  }
  unsafe {
    let mut feasible: Vec<LegIndicesWithDistance> = Vec::new();
    let mut i = check_if_perfect_match_from(demand.from, legs[1].route_id, legs, 1); // we do not start at 0 as there must be at least one leg before 'not started', assigned, so that cab has time to get an extended route
    let mut initial_distance: i16 = 0;
    let mut leg_count: i8 = -1;
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
      // first find perfect match when new route to check
      if leg.route_id != legs[i-1].route_id {
        initial_distance = 0; 
        leg_count = -1;
        i = check_if_perfect_match_from(demand.from, legs[i].route_id, legs, i + 1); // this sets the index, we will check "direct hit" later on too
        // +1 above as we cannot modify the first leg, a cab might not be able to read the changes before starting the leg
      } 
      else if DIST[leg.from as usize][demand.from as usize] as i32 > demand.wait // just a quick filter
              || initial_distance as i32 > demand.wait { // not new route, check WAIT constraint
        i = find_next_route(i, legs);
        continue; // this means iterate to the next route
        // TODO/TASK - demand.from == leg.to && legs[i+1].route_id != leg.route_id
        // possible extension beyond current legs
      }
      if leg_count == -1 { // just to count once for a route, TODO: a hashmap before we go thru all orders?
        leg_count = count_legs(leg.route_id, legs);
      }
      let not_too_long: bool = leg_count <= CNFG.max_legs;
      
      let mut distance_diff = DIST[leg.from as usize][demand.from as usize] + CNFG.stop_wait +
                                    DIST[demand.from as usize][leg.to as usize] - leg.dist as i16;
      // -1 might be just INTEGER rounding error, to exceed reserve by more than 1 is not acceptable, max wait or max loss would not be met for other customers
// DEBUGGING 
if DIST[leg.from as usize][demand.from as usize] == 0 || DIST[demand.from as usize][leg.to as usize] == 0 {
        warn!("----------------------------------- ZERO -----------------------------------");
}
      if (leg.reserve - distance_diff as i32) < -1 {
        i += 1;
        continue;
      }

      if demand.from != leg.to // == means direct hit in the next leg/iteration, leg.from; here we miss one possibility, where leg is the last leg in that route
          // previous leg is from the same route
          && legs[i - 1].route_id == leg.route_id // still in the same route
          // the previous leg cannot be completed TASK!! in the future consider other statuses here
          // we want the previous leg to be active
          // to give some time for both parties to get the assignment
          && legs[i - 1].status != RouteStatus::COMPLETED as i32
          // check MAXLOSS
          && ((demand.from == leg.from && demand.wait >= initial_distance as i32)// direct hit and maxwait check
              || (not_too_long
                && demand.wait >= (initial_distance + DIST[leg.from as usize][demand.from as usize]) as i32 // MAXWAIT
                //&& leg.reserve >= distance_diff as i32 // new path - old path length; integers so it can even be negative :) we will check it
                //&& ((distance_diff + CNFG.stop_wait) as f32) < (leg.dist as f32) * CNFG.extend_margin
                && bearing_diff(stops[leg.from as usize].bearing, stops[demand.from as usize].bearing) < CNFG.max_angle
                && bearing_diff(stops[demand.from as usize].bearing, stops[leg.to as usize].bearing) < CNFG.max_angle
                ) // 5% TASK - global config, wait at stop?
             )
      {
        // OK, so we found the first 'pickup' leg, either direct hit or can be extended
        let mut to_found: bool = false;
        let mut distance_in_pool: i16 = 0;
        // we have found "from", now let's find "to"
        let mut k = check_if_perfect_match_to(demand.to, legs, i); // "to might be in the same leg as "from", we have to start from 'i'
        while k < legs.len() {
          if i != k { // 'i' counted already
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
          if (legs[k].reserve - distance_diff as i32) < -1 {
            k += 1;
            continue;
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
            //&& demand.wait as i16 >= initial_distance     why check this, it has been checked above 
            // TASK: maybe distance*maxloss is a performance bug,
            // distanceWithLoss should be stored in DB and used
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
    // one of many possible goal functions
    // initial_distance + distance_in_pool
    // choose the one best for the new; alternatives:
    // a) the version with least extension 
    // b) the least sum of prolonged trips of other orders
    feasible.sort_by_key(|e| e.dist.clone());
    return Some(feasible[0]); 
  }
}
  
fn find_next_route(i: usize, legs: &Vec<Leg>) -> usize {
  let mut k = i;
  while k < legs.len() {
    if legs[k].route_id != legs[i].route_id {
      return k;
    }
    k += 1;
  }
  return k;
}

fn modify_legs(f: &mut LegIndicesWithDistance, max_leg_id: &mut i64, legs: &mut Vec<Leg>) -> String {
    let demand = f.order;
    let from_leg: Leg = legs[f.idx_from as usize];

    // pickup phase
    let mut sql: String = String::from("");
    debug!("Order {} assigned to existing route: {}", demand.id, from_leg.route_id);
    debug!("Modifying legs, idx_from={}, idx_to={}, from_leg_id={}, to_leg_id={}", 
          f.idx_from, f.idx_to, from_leg.id, legs[f.idx_to as usize].id);
    if demand.from == from_leg.from { // direct hit, we don't modify that leg
      // TODO: eta should be calculated !!!!!!!!!!!!!!!!!!!!!
      sql += &assign_order_find_cab(demand.id, from_leg.id, from_leg.route_id, 0, "true", "matchRoute IN");
    } else { 
      // pick-up phase
      sql += &extend_legs_in_db_and_vec(&demand, -1, f.idx_from, legs, demand.from, from_leg.place, 
                                from_leg.id, max_leg_id, "IN");
      f.idx_to += 1; // one more leg there
      // now assign the order to the new leg
      sql += &assign_order_find_cab(demand.id, *max_leg_id -1 , from_leg.route_id, 0, "true", "extendRoute IN");
    }
    add_avg_element(Stat::AvgOrderAssignTime, get_elapsed(demand.received));

    // drop-off phase
    let to_leg: Leg = legs[f.idx_to as usize];
    if demand.to != to_leg.to { // one leg more, ignore situation with ==
      // if from_leg == to_leg && demand.from != from_leg.from, which means we put a customer's 'from' and 'to' between two stops of a route,
      // then two things change - one more leg there, 'place'+1, and we will modify the new leg inserted above (we have to refer to a newly generated ID), 
      // not the leg stored in 'legs'
      if f.idx_from == f.idx_to && demand.from != from_leg.from {
        sql += &extend_legs_in_db_and_vec(&demand, f.idx_from, f.idx_to, legs, demand.to, to_leg.place + 1,
                                 *max_leg_id -1, max_leg_id, "OUT");
      } else {
        sql += &extend_legs_in_db_and_vec(&demand, f.idx_from, f.idx_to, legs, demand.to, to_leg.place, 
                                  to_leg.id, max_leg_id, "OUT");
      }
    }
    // now update passengers, if this is OUT
    
    if demand.from == legs[f.idx_from as usize].from { // IN was a perfect match -> the first leg does not concern the 'order' here: +1
      sql += &update_passengers(from_leg.route_id, f.idx_from as usize, f.idx_to as usize, legs);
    } else {
      sql += &update_passengers(from_leg.route_id, f.idx_from as usize + 1, f.idx_to as usize, legs);
    }
    return sql;
}

/*
fn update_places(idx: usize, legs: &mut Vec<Leg>, route_id: i64) {
  if idx >= legs.len() {
    return; //nothing to correct
  }
  let mut i = idx;
  while i < legs.len() && legs[i].route_id == route_id  { 
    legs[i].place += 1; 
    i += 1;
  }
}
*/

fn extends_legs_in_vec(leg: Leg, from: i32, idx:i32, legs: &mut Vec<Leg>, leg_id: i64, res: i32, pass: i32, place: i32) {
  unsafe {
    // new leg
    let new_leg = Leg { 
        id: leg_id, // we don't know what it will be during insert
        route_id: leg.route_id,
        from: from,
        to: leg.to,
        place: place,
        dist:  DIST[from as usize][leg.to as usize] as i32,
        status: RouteStatus::ASSIGNED as i32,
        completed: None,
        started: None,
        reserve: res,
        passengers: pass
    };
    legs.insert(idx as usize + 1, new_leg);
  }
}
  
fn extend_legs_in_db_and_vec(order: &Order, from_leg_idx: i32, leg_idx: i32, legs: &mut Vec<Leg>, from: i32,
                     place: i32, leg_id: i64, max_leg_id: &mut i64, label: &str) -> String {
  unsafe {
    let leg: Leg = legs[leg_idx as usize];
    let mut sql: String = String::from("");
    let (distance_diff, reserve) = count_reserve(order, from_leg_idx, leg_idx, leg, from, place, label);
    
    // we will add a new leg on "place"
    // we have to increment places in all subsequent legs
    // +
    //we have to modify reserves in DB and in Vec:
    //- all requests starting after added leg will get worse WAIT reserve, by 'distance_diff'
    //- all requests with 'i' before and 'o' after will get worse LOSS reserve, by 'distance_diff'
    //- the total reserve of a leg is MIN of both (this is not relevant here, maybe)
    // the trouble is - we do not have information about 'i'/'o', so just decrease the all
    // And we have to take of of MAX_WAIT of the new order, decrease reserves of previous legs
    // these reserves cannot exceed max_wait- sum of distances of previous legs
    // previous legs are only non-started legs, TODO: the time-to-completion of the leg being executed should be added
    // with other words: MIN (reserve, max_wait-duration)
    let mut order_loss_reserve: i32 = 10000; // just big, will be MINimized, counted only for OUT leg, of course

    if from_leg_idx == -1 { // leg_idx is "IN leg" 
      let mut wait_diff: i32 = order.wait - sum_distances(legs, leg_idx);
      if wait_diff < 0 { 
        warn!("Max wait not met {} leg_id={}, route_id={}, place={}", label, leg.id, leg.route_id, place + 1);
        wait_diff =0; 
      }
      sql += &update_reserves_in_legs_before_and_including(leg.route_id, place, wait_diff); 
      // and in Vec
      decrease_reserve_before(leg_idx, legs, wait_diff);
    } else { // OUT - now we can count loss, correct reserve value and actual legs "after" 
      order_loss_reserve = ((1.0 + order.loss as f32 / 100.0) * order.dist as f32) as i32 - 
                                    count_actual_distance(from_leg_idx as usize, leg_idx as usize, legs, order);
      if order_loss_reserve < 0 {
        warn!("Order loss reserve is negative (route extension): route_id={}, order_id={},", leg.route_id, order.id);
        order_loss_reserve = 0;
      }
      // we correct all "after" legs below, but here we just have to correct legs for that particular order
      // 
      // if the IN part was a precise hit, then we have to encrease 'passengers' count on that leg too, 
      // otherwise only the new leg will have more passengers
      let from_idx_increment = if order.from == legs[from_leg_idx as usize].from { 0 } else { 1 };
      sql+= &update_reserve_in_legs_between(leg.route_id, 
                                                legs[from_leg_idx as usize].place + from_idx_increment, 
                                                legs[leg_idx as usize].place, 
                                                order_loss_reserve);
      // and in Vec, update number of passengers too
      // TODO: this below should not be called when both legs (FROM & TO) extend the same, one previous leg
      // because passengers will be incremented by 1 too many
      update_reserve_between(from_leg_idx + from_idx_increment, leg_idx, legs, order_loss_reserve);
    }
    // update legs after
    sql += &update_place_and_reserve_in_legs_after(leg.route_id, place + 1, distance_diff);
    
    //decrease reserve also in Vec, not only in DB
    update_place_and_reserve_after(leg_idx, legs, distance_diff);

    let reserve_new = cmp::min(reserve, order_loss_reserve);
    // one leg more in that free place

    extends_legs_in_vec(leg, from, leg_idx, legs, *max_leg_id, reserve_new, leg.passengers, place + 1);

    sql += &create_leg( order.id, 
                        from,
                        leg.to,
                        place + 1,
                        RouteStatus::ASSIGNED,
                        DIST[from as usize][leg.to as usize],
                        reserve_new,
                        leg.route_id as i64, 
                        max_leg_id, // TODO: all IDs should be i64
                        leg.passengers as i8, // it will be updated when OUT is created - see above 
                        &("route extender ".to_string() + &label.to_string()));

    // modify existing leg (to_stand, dist, reserve) so that it goes to a new waypoint in-between
    // when extender puts both IN and OUT into one 
    // but somehow we managed to extend many time - a bug to be fixed ... now
    if leg_id != -1 {
      sql += &update_leg_a_bit(leg.route_id, leg_id, from, 
                DIST[leg.from as usize][from as usize], reserve);
    } else { // less efficient & more risky (there can always be a bug in "placing")
      sql += &update_leg_with_route_id(leg.route_id, place, from, 
                DIST[leg.from as usize][from as usize], reserve);
    }
    legs[leg_idx as usize].reserve = reserve;
    legs[leg_idx as usize].dist = DIST[leg.from as usize][from as usize] as i32;
    legs[leg_idx as usize].to = from;
  
    return sql;
  }
}

fn count_reserve(order: &Order, from_leg_idx: i32, leg_idx: i32, leg: Leg, from: i32, place: i32, label: &str) 
                -> (i32, i32) {
  unsafe {
    let mut distance_diff: i32;
    let explain: String;
    if from_leg_idx == -1 || from_leg_idx != leg_idx {
      distance_diff = (DIST[leg.from as usize][from as usize] + CNFG.stop_wait 
                      + DIST[from as usize][leg.to as usize]) as i32 - leg.dist;
    }
    else { // OUT leg extends the same leg as IN
      distance_diff = (DIST[leg.from as usize][order.from as usize] + CNFG.stop_wait 
                      + DIST[order.from as usize][order.to as usize]+ CNFG.stop_wait
                      + DIST[order.to as usize][leg.to as usize]) as i32 - leg.dist;
    }
    if distance_diff < 0 {
      warn!("Negative distance_diff while extending leg {} leg_id={}, route_id={}, place={}", 
            label, leg.id, leg.route_id, place + 1);
      distance_diff = 0;
    }
    // there will be two reserves as there will be two legs (not IN/OUT, but each leg, both IN/OUT, can be extended into two)
    // the second leg can have bigger reserve (here only MAX_LOSS matters)
    // here we count reserve for the first leg
    let mut reserve: i32 = cmp::min(leg.reserve - distance_diff, leg.reserve 
                    - DIST[leg.from as usize][from as usize] as i32) - CNFG.stop_wait as i32;
    if reserve < 0 {
      //warn!("Negative reserve while extending leg {} leg_id={}, route_id={}, place={}", 
      //      label, leg.id, leg.route_id, place + 1);
      reserve = 0;
    }
    if from_leg_idx == -1 || from_leg_idx != leg_idx {
      explain = format!("prev_reserve={}, distance_diff={}, new_reserve={}, diff={}+{}+{}-{}",
                        leg.reserve, distance_diff, reserve, 
                        DIST[leg.from as usize][from as usize], CNFG.stop_wait,
                        DIST[from as usize][leg.to as usize], leg.dist)   
    } else {
      explain = format!("prev_reserve={}, distance_diff={}, new_reserve={}, diff={}+{}+{}+{}+{}-{}",
                        leg.reserve, distance_diff, reserve, 
                        DIST[leg.from as usize][order.from as usize], CNFG.stop_wait,
                        DIST[order.from as usize][order.to as usize], CNFG.stop_wait,
                        DIST[order.to as usize][leg.to as usize], leg.dist)   
    }
    debug!("new, extended {} leg_id={}, route_id={}, place={}, {}", label, leg.id, leg.route_id, place + 1, explain);
    return (distance_diff, reserve);
  }
}

fn count_actual_distance(from: usize, to: usize, legs: &Vec<Leg>, order: &Order) -> i32 {
  let from_leg: Leg = legs[from];
  let to_leg: Leg = legs[to];
  let mut sum: i32 =0;
  if order.from == from_leg.from {
    sum += from_leg.dist;
  } else {
    sum += unsafe { DIST[order.from as usize][from_leg.to as usize] as i32 + CNFG.stop_wait as i32 } ;
  }
  for i in from+1..to {
    sum += legs[i].dist + unsafe { CNFG.stop_wait as i32 };
  }
  if order.to != to_leg.from {
    sum += unsafe { DIST[to_leg.from as usize][order.to as usize] as i32  + CNFG.stop_wait as i32 };
  }
  return sum;
}

fn update_place_and_reserve_after(leg_idx: i32, legs: &mut Vec<Leg>, distance_diff: i32) {
  for idx in leg_idx as usize +1 .. legs.len() {
    if legs[idx-1].route_id != legs[idx].route_id { // new route
      break; 
    }
    legs[idx].reserve = cmp::max(0, legs[idx].reserve - distance_diff); // a least zero, not negative
    legs[idx].place += 1; 
  }
}

fn decrease_reserve_before(leg_idx: i32, legs: &mut Vec<Leg>, wait_diff: i32) {
  for i in (0..leg_idx as usize + 1).rev() { // +1 = including leg_idx
    if legs[i].route_id != legs[leg_idx as usize].route_id {
      return;
    }
    legs[i].reserve = cmp::min( legs[i].reserve, wait_diff);
  }
}

fn update_reserve_between(from_leg_idx: i32, leg_idx: i32, legs: &mut Vec<Leg>, loss_reserve: i32) {
  for i in from_leg_idx as usize .. leg_idx as usize +1 { // +1 = including leg_idx
    legs[i].reserve = cmp::min( legs[i].reserve, loss_reserve);
  }
}

fn update_reserve_and_pass_between(from_leg_idx: i32, leg_idx: i32, legs: &mut Vec<Leg>, loss_reserve: i32) {
  for i in from_leg_idx as usize .. leg_idx as usize +1 { // +1 = including leg_idx
    legs[i].reserve = cmp::min( legs[i].reserve, loss_reserve);
  }
  for i in from_leg_idx as usize .. leg_idx as usize {
    legs[i].passengers += 1;
  }
}

fn sum_distances(legs: &Vec<Leg>, leg_idx: i32) -> i32 {
  let mut sum: i32 = 0;
  for i in (0..leg_idx as usize + 1).rev() {
    if legs[i].route_id != legs[leg_idx as usize].route_id {
      return sum;
    }
    sum += legs[i].dist + unsafe { CNFG.stop_wait as i32 };
  }
  return sum;
}

fn count_legs(id: i64, legs: &Vec<Leg>) -> i8 {
    let mut count: i8 = 0;
    for l in legs.iter() {
        if l.route_id == id {
            count += 1;
        } else {
          break;
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

  fn get_test_legs() -> Vec<Leg> {
    return vec![
      Leg{ id: 0, route_id: 123, from: 0, to: 1, place: 0, dist: 1, reserve:1, started: None, completed: None, status: 0, passengers:1},
      Leg{ id: 1, route_id: 123, from: 1, to: 2, place: 1, dist: 1, reserve:2, started: None, completed: None, status: 0, passengers:1},
      Leg{ id: 2, route_id: 123, from: 2, to: 3, place: 2, dist: 1, reserve:3, started: None, completed: None, status: 0, passengers:1},
    ];
  }

  fn get_stops() -> Vec<Stop> {
    return vec![
      Stop{ id: 0, bearing: 0, latitude: 1.0, longitude: 1.0},
      Stop{ id: 1, bearing: 0, latitude: 1.000000001, longitude: 1.000000001},
      Stop{ id: 2, bearing: 0, latitude: 1.000000002, longitude: 1.000000002},
      Stop{ id: 3, bearing: 0, latitude: 1.000000003, longitude: 1.000000003},
      Stop{ id: 4, bearing: 0, latitude: 1.000000004, longitude: 1.000000004},
      Stop{ id: 5, bearing: 0, latitude: 1.000000005, longitude: 1.000000005}
    ];
  }

  #[test]
  fn test_add() {
      assert_eq!(count_legs(123, &get_test_legs()), 3);
  }

  #[test]
  fn test_extend_legs_in_db_returns_sql() {
    let order = Order { id: 1, from: 1, to: 3, wait: 10, loss:90, dist:1, shared: true, in_pool: false, 
                              received: None, started: None, completed: None, at_time: None, eta: 1 };
    let max_leg_id: &mut i64 = &mut 1;
    let sql = extend_legs_in_db_and_vec(&order, 0, 0, &mut get_test_legs(), 2, 1, 0,        max_leg_id, "label");

    assert_eq!(sql, "UPDATE leg SET reserve=LEAST(reserve, 0), passengers=passengers+1 WHERE route_id=123 AND place BETWEEN 1 AND 0;\n\
        UPDATE leg SET place=place+1, reserve=GREATEST(0,reserve-1) WHERE route_id=123 AND place >= 2;\n\
        INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (1,2,1,2,0,1,0,123,1);\n\
        UPDATE leg SET to_stand=2, distance=0, reserve=0 WHERE id=0;\n");
  }

  #[test]
  fn test_try_to_extend_route_when_perfect_match() {
    let mut legs = get_test_legs();
    let order1= Order{ id: 0, 
      from: 1,
      to: 2,
      wait: 10,loss: 50,dist: 2,shared: true,in_pool: false,received: None,started: None,completed: None,at_time: None,eta: 0,
    };
    let order2= Order{ id: 1, 
      from: 1,
      to: 3, // !!!
      wait: 10,loss: 50,dist: 2,shared: true,in_pool: false,received: None,started: None,completed: None,at_time: None,eta: 0,
    };
    let stops: Vec<Stop> = get_stops();
    let mut indices: LegIndicesWithDistance = LegIndicesWithDistance {
      idx_from: -1, idx_to: -1, dist: 0, order: order1
    };
    // route is 0->1->2->3
    // order is 1->2
    assert_eq!(legs.len(), 3);

    match try_to_extend_route(&order1, &legs, &stops) {
      Some(x) => indices = x,
      None => {}
    }
    assert_eq!(indices.idx_from, 1);
    assert_eq!(indices.idx_to, 1);
    assert_eq!(legs.len(), 3);

    // order is 1->3
    match try_to_extend_route(&order2, &legs, &stops) {
      Some(x) => indices = x,
      None => {}
    }
    assert_eq!(indices.idx_from, 1);
    assert_eq!(indices.idx_to, 2);
    assert_eq!(legs.len(), 3);

    let demand: Vec<Order> = vec![order1];

    let ret = extend_routes(0, &demand, &stops, &mut legs, &mut 0);
    assert_eq!(ret.0.len(), 0);
    assert_eq!(legs.len(), 3);
    assert_eq!(ret.1, "UPDATE taxi_order AS o SET route_id=123, leg_id=1, cab_id=r.cab_id, status=1, eta=0 FROM route AS r WHERE r.id=123 AND o.id=0 AND o.status=0;\n");

    let demand: Vec<Order> = vec![order2];

    let ret = extend_routes(0, &demand, &stops, &mut legs, &mut 0);
    assert_eq!(ret.0.len(), 0);
    assert_eq!(legs.len(), 3);
    assert_eq!(ret.1, "UPDATE taxi_order AS o SET route_id=123, leg_id=1, cab_id=r.cab_id, status=1, eta=0 FROM route AS r WHERE r.id=123 AND o.id=1 AND o.status=0;\n");

    let demand: Vec<Order> = vec![order1, order2];

    let ret = extend_routes(0, &demand, &stops, &mut legs, &mut 0);
    assert_eq!(ret.0.len(), 0);
    assert_eq!(legs.len(), 3);
    assert_eq!(ret.1, "UPDATE taxi_order AS o SET route_id=123, leg_id=1, cab_id=r.cab_id, status=1, eta=0 \
              FROM route AS r WHERE r.id=123 AND o.id=0 AND o.status=0;\n\
              UPDATE taxi_order AS o SET route_id=123, leg_id=1, cab_id=r.cab_id, status=1, eta=0 \
              FROM route AS r WHERE r.id=123 AND o.id=1 AND o.status=0;\n");
  }
  
  #[test]
  fn test_try_to_extend_route_when_nonperfect_match() {
    let order1= Order{ id: 1, from: 0, to: 1,
      wait: 10,loss: 50,dist: 2,shared: true,in_pool: false,received: None,started: None,completed: None,at_time: None,eta: 0,
    };
    let order2= Order{ id: 2, from: 1, to: 2,
      wait: 10,loss: 50,dist: 2,shared: true,in_pool: false,received: None,started: None,completed: None,at_time: None,eta: 0,
    };
    let order3= Order{ id: 3, from: 1, to: 3,
      wait: 10,loss: 50,dist: 2,shared: true,in_pool: false,received: None,started: None,completed: None,at_time: None,eta: 0,
    };
    let stops: Vec<Stop> = get_stops();

    let mut legs: Vec<Leg> = vec![
      Leg{ id: 0, route_id: 123, from: 0, to: 2, place: 0, dist: 1, reserve:1, started: None, completed: None, status: 0, passengers:1},
      Leg{ id: 1, route_id: 123, from: 2, to: 3, place: 1, dist: 1, reserve:3, started: None, completed: None, status: 0, passengers:1},
    ];
    let demand: Vec<Order> = vec![order1];
    // route is 0->2->3
    // order is 0->1
    assert_eq!(legs.len(), 2);
    let ret = extend_routes(0, &demand, &stops, &mut legs, &mut 2);
    assert_eq!(ret.0.len(), 0); // order fits the route
    assert_eq!(legs.len(), 4); // one more leg
    // route is 0->2->0->1->3  because 0->2 is not taken into account, the previous leg must be ASSIGNED
    // one passenger: 0->2->0, 1->3
    // two passengers: 0->1
    assert_eq!(legs[1].to, 0);
    assert_eq!(legs[2].from, 0);
    assert_eq!(legs[2].to, 1);
    assert_eq!(legs[3].from, 1);
    assert_eq!(legs[1].place, 1);
    assert_eq!(legs[2].place, 2);
    assert_eq!(legs[3].place, 3);
    assert_eq!(legs[0].passengers, 1);
    assert_eq!(legs[1].passengers, 1);
    assert_eq!(legs[2].passengers, 2);
    assert_eq!(legs[3].passengers, 1);

    assert_eq!(ret.1, 
      "UPDATE leg SET reserve=LEAST(reserve, 6) WHERE route_id=123 AND place <= 1;\n\
      UPDATE leg SET place=place+1, reserve=GREATEST(0,reserve-0) WHERE route_id=123 AND place >= 2;\n\
      INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (2,0,3,2,0,1,2,123,1);\n\
      UPDATE leg SET to_stand=0, distance=0, reserve=2 WHERE id=1;\n\
      UPDATE taxi_order AS o SET route_id=123, leg_id=2, cab_id=r.cab_id, status=1, eta=0 FROM route AS r WHERE r.id=123 AND o.id=1 AND o.status=0;\n\
      UPDATE leg SET reserve=LEAST(reserve, 1) WHERE route_id=123 AND place BETWEEN 2 AND 2;\n\
      UPDATE leg SET place=place+1, reserve=GREATEST(0,reserve-1) WHERE route_id=123 AND place >= 3;\n\
      INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (3,1,3,3,0,1,0,123,1);\n\
      UPDATE leg SET to_stand=1, distance=0, reserve=0 WHERE id=2;\n\
      UPDATE leg SET passengers=passengers+1 WHERE route_id=123 AND place BETWEEN 2 AND 2;\n");
  }

  #[test]
  fn test_try_to_extend_route_when_nonperfect_match2() {
    let stops: Vec<Stop> = get_stops();
    let mut legs: Vec<Leg> = vec![
      Leg{ id: 0, route_id: 123, from: 0, to: 2, place: 0, dist: 1, reserve:1, started: None, completed: None, status: 0, passengers:1},
      Leg{ id: 1, route_id: 123, from: 2, to: 3, place: 1, dist: 1, reserve:3, started: None, completed: None, status: 0, passengers:1},
    ];
    let order1= Order{ id: 1, from: 1, to: 3,
      wait: 10,loss: 50,dist: 2,shared: true,in_pool: false,received: None,started: None,completed: None,at_time: None,eta: 0,
    };
    let demand: Vec<Order> = vec![order1];
    // route is 0->2->3
    // order is 1->3
    assert_eq!(legs.len(), 2);
    let ret = extend_routes(0, &demand, &stops, &mut legs, &mut 2);
    assert_eq!(ret.0.len(), 0); // order fits the route
    assert_eq!(legs.len(), 3); // one more leg
    assert_eq!(legs[1].to, 1);
    assert_eq!(legs[2].from, 1);
    assert_eq!(legs[2].to, 3);
    assert_eq!(legs[0].place, 0);
    assert_eq!(legs[1].place, 1);
    assert_eq!(legs[2].place, 2);
    assert_eq!(legs[0].passengers, 1);
    assert_eq!(legs[1].passengers, 1);
    assert_eq!(legs[2].passengers, 2);
    assert_eq!(ret.1, 
      "UPDATE leg SET reserve=LEAST(reserve, 6) WHERE route_id=123 AND place <= 1;\n\
      UPDATE leg SET place=place+1, reserve=GREATEST(0,reserve-0) WHERE route_id=123 AND place >= 2;\n\
      INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (2,1,3,2,0,1,2,123,1);\n\
      UPDATE leg SET to_stand=1, distance=0, reserve=2 WHERE id=1;\n\
      UPDATE taxi_order AS o SET route_id=123, leg_id=2, cab_id=r.cab_id, status=1, eta=0 FROM route AS r WHERE r.id=123 AND o.id=1 AND o.status=0;\n\
      UPDATE leg SET passengers=passengers+1 WHERE route_id=123 AND place BETWEEN 2 AND 2;\n");
  }

  #[test]
  fn test_try_to_extend_route_debugging_a_real_bug() {
    let db_conn = "postgresql://kabina:kaboot@192.168.10.176:5432/kabina";
    let mut client = Client::connect(&db_conn, NoTls).unwrap();
    let stops = read_stops(&mut client);
    init_distance(&stops);

    let mut legs: Vec<Leg> = vec![
      Leg{ id: 133, route_id: 29, from: 3025, to: 3023, place: 0, dist: 6, reserve:1, started: None, completed: None, status: 0, passengers:1},
      Leg{ id: 134, route_id: 29, from: 3023, to: 3762, place: 1, dist: 3, reserve:1, started: None, completed: None, status: 0, passengers:0},
      Leg{ id: 135, route_id: 29, from: 3762, to: 4165, place: 2, dist: 2, reserve:1, started: None, completed: None, status: 0, passengers:1},
      Leg{ id: 136, route_id: 29, from: 3023, to: 3762, place: 3, dist: 10,reserve:1, started: None, completed: None, status: 0, passengers:2},
    ];
    let order1= Order{ id: 0, from: 4066, to: 4063,
      wait: 15,loss: 70, dist: 6, shared: true,in_pool: false,received: None,started: None,completed: None,at_time: None,eta: 0,
    };
    let demand: Vec<Order> = vec![order1];
    let ret = extend_routes(0, &demand, &stops, &mut legs, &mut 926);
    assert_eq!(ret.0.len(), 1); // order does not fit the route
  }

  #[test]
  fn test_try_to_extend_route_when_nonperfect_match3() {
    let stops: Vec<Stop> = get_stops();
    let mut legs: Vec<Leg> = vec![
      Leg{ id: 0, route_id: 123, from: 0, to: 2, place: 0, dist: 1, reserve:1, started: None, completed: None, status: 0, passengers:1},
      Leg{ id: 1, route_id: 123, from: 2, to: 3, place: 1, dist: 1, reserve:3, started: None, completed: None, status: 0, passengers:1},
    ];
    let order1= Order{ id: 1, from: 2, to: 1,
      wait: 10,loss: 50,dist: 2,shared: true,in_pool: false,received: None,started: None,completed: None,at_time: None,eta: 0,
    };
    let demand: Vec<Order> = vec![order1];
    // route is 0->2->3
    // order is 2->1
    assert_eq!(legs.len(), 2);
    let ret = extend_routes(0, &demand, &stops, &mut legs, &mut 2);
    assert_eq!(ret.0.len(), 0); // order fits the route
    assert_eq!(legs.len(), 3); // one more leg
    assert_eq!(legs[1].to, 1);
    assert_eq!(legs[2].from, 1);
    assert_eq!(legs[2].to, 3);
    assert_eq!(legs[1].place, 1);
    assert_eq!(legs[2].place, 2);
    assert_eq!(legs[0].passengers, 1);
    assert_eq!(legs[1].passengers, 2);
    assert_eq!(legs[2].passengers, 1);
    assert_eq!(ret.1, 
      "UPDATE taxi_order AS o SET route_id=123, leg_id=1, cab_id=r.cab_id, status=1, eta=0, in_pool=true FROM route AS r WHERE r.id=123 AND o.id=1 AND o.status=0;\n\
      UPDATE leg SET reserve=LEAST(reserve, 1) WHERE route_id=123 AND place BETWEEN 1 AND 1;\n\
      UPDATE leg SET place=place+1, reserve=GREATEST(0,reserve-1) WHERE route_id=123 AND place >= 2;\n\
      INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (2,1,3,2,0,1,1,123,1);\n\
      UPDATE leg SET to_stand=1, distance=0, reserve=1 WHERE id=1;\n\
      UPDATE leg SET passengers=passengers+1 WHERE route_id=123 AND place BETWEEN 1 AND 1;\n");
  }

  #[test]
  fn test_check_if_perfect_match_from_and_to() {
    let legs = get_test_legs();
    assert_eq!(check_if_perfect_match_from(2, legs[1].route_id, &legs, 0), 2);
    assert_eq!(check_if_perfect_match_from(4, legs[0].route_id, &legs, 0), 0);
    assert_eq!(check_if_perfect_match_to(3, &legs, 0), 2);
    assert_eq!(check_if_perfect_match_to(1, &legs, 0), 0);
  }

  #[test]
  fn test_modify_legs() {
    let order= Order{ id: 1, from: 1, to: 2, wait: 10,loss: 50,dist: 2,
        shared: true,in_pool: false,received: None,started: None,completed: None,at_time: None,eta: 0,
    };
    let mut f = LegIndicesWithDistance { 
      idx_from: 0, idx_to: 1, dist: 1, order: order };
    let max_leg_id: &mut i64 = &mut 1;
    let mut legs = get_test_legs();
    let out = modify_legs(&mut f, max_leg_id, &mut legs); 
    assert_eq!(out, "UPDATE leg SET reserve=LEAST(reserve, 8) WHERE route_id=123 AND place <= 0;\n\
    UPDATE leg SET place=place+1, reserve=GREATEST(0,reserve-0) WHERE route_id=123 AND place >= 1;\n\
    INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (1,1,1,1,0,1,0,123,1);\n\
    UPDATE leg SET to_stand=1, distance=0, reserve=0 WHERE id=0;\nUPDATE taxi_order AS o SET route_id=123, \
    leg_id=1, cab_id=r.cab_id, status=1, eta=0 FROM route AS r WHERE r.id=123 AND o.id=1 AND o.status=0;\n");
  }

  #[test]
  fn test_count_actual_distance() {
    let legs = get_test_legs();
    let order= Order{ id: 1, from: 1, to: 2, wait: 10,loss: 50,dist: 2,
      shared: true,in_pool: false,received: None,started: None,completed: None,at_time: None,eta: 0,
    };
    let ret = count_actual_distance(0, 2, &legs, &order);
    assert_eq!(ret, 3);
  }

  #[test]
  fn test_decrease_reserve_after() {
    let mut legs = get_test_legs();
    assert_eq!(legs[0].reserve, 1);
    assert_eq!(legs[2].reserve, 3);
    update_place_and_reserve_after(1, &mut legs, 1);
    assert_eq!(legs[0].reserve, 1);
    assert_eq!(legs[2].reserve, 2);
  }

  #[test]
  fn test_decrease_reserve_before() {
    let mut legs = get_test_legs();
    assert_eq!(legs[0].reserve, 1);
    assert_eq!(legs[2].reserve, 3);
    decrease_reserve_before(1, &mut legs, 0);
    assert_eq!(legs[0].reserve, 0);
    assert_eq!(legs[2].reserve, 3);
  }

  #[test]
  fn test_decrease_reserve_between() {
    let mut legs = get_test_legs();
    assert_eq!(legs[1].reserve, 2);
    update_reserve_and_pass_between(0, 1, &mut legs, 0);
    assert_eq!(legs[1].reserve, 0);
  }

  #[test]
  fn test_sum_distances() {
    let legs = get_test_legs();
    let ret = sum_distances(&legs, 1);
    assert_eq!(ret, 4);
  }

}