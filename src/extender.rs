use std::{thread};
use rand::Rng;
use std::io::Write;
use postgres::{Client, NoTls};
use crate::model::{ Order, Stop, Leg, RouteStatus };
use crate::repo::{find_legs_by_status,assignOrder,assignOrderFindLeg,updatePlacesInLegs,create_leg,updateLegABit,updateLegABitWithRouteId};
use crate::distance::{DIST};
use crate::pool::{bearing_diff};

const max_legs: i8 = 8;
const extend_margin : f32 = 1.05;
pub const max_angle: f32 = 120.0;

//#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LegIndicesWithDistance {
    idx_from: i32,
    idx_to: i32, 
    dist: i32
}

// impl LegIndicesWithDistance {
//     pub fn new(idx_from: i32, idx_to: i32, dist: i32) -> Self {
//         LegIndicesWithDistance { idx_from, idx_to, dist }
//     }
// }

pub fn findMatchingRoutes(client: &mut Client, demand: &Vec<Order>, stops: &Vec<Stop>, max_leg_id: &mut i64) 
                          -> (Vec<Order>, thread::JoinHandle<()>) {
    if demand.len() == 0 {
        return (Vec::new(), thread::spawn(|| { }));
    }
    let mut legs: Vec<Leg> = find_legs_by_status(client, RouteStatus::ASSIGNED);
    if legs.len() == 0 {
        return (demand.to_vec(), thread::spawn(|| { }));
    }
    println!("findMatchingRoutes START, orders count={} legs count={}", demand.len(), legs.len());
    let mut ret: Vec<Order> = Vec::new();
    let mut sql_bulk: String = String::from("");
    for taxiOrder in demand.iter() {
        let sql: String = try_to_extend_route(&taxiOrder, &mut legs, &stops, max_leg_id);
        if sql == "nope" { // if not matched or extended
            ret.push(*taxiOrder); // it will go to pool finder
        } else {
            sql_bulk += &(sql + "\n");
        }
    }
    println!("findMatchingRoutes STOP, rest orders count={}", ret.len());
    writeSqlToFile(&sql_bulk, "route_extender");
    // EXECUTE SQL !!
    let handle = getHandle(sql_bulk, "extender".to_string());
    return (ret, handle);
}

pub fn writeSqlToFile(sql: &String, label: &str) {
  let mut rng = rand::thread_rng();
  let file_name = format!("{}{}.sql", label.to_string(), rng.gen_range(0..10000000));
  let msg = format!("SQL for {} failed", file_name);
  let mut file = std::fs::File::create(&file_name).expect(&("Create ".to_string() + &msg));
  file.write_all(sql.as_bytes()).expect(&("Write ".to_string() + &msg));
}

fn try_to_extend_route(demand: & Order, legs: &mut Vec<Leg>, stops: &Vec<Stop>, max_leg_id: &mut i64) -> String {
    unsafe {
      let mut feasible: Vec<LegIndicesWithDistance> = Vec::new();
      let mut i = 1;
      let mut initial_distance: i16 = 0;
      while i < legs.len() {
        // not from 0 as each leg we are looking for must have a predecessor
        // routes from the same stand which have NOT started will surely be seen by passengers,
        // they can get aboard
        // TASK: MAX WAIT check
        let leg: Leg = legs[i];
        let not_too_long: bool = count_legs(leg.route_id, legs) <= max_legs;
        if leg.status == RouteStatus::ASSIGNED as i32 || leg.status == RouteStatus::ACCEPTED as i32 {
          initial_distance += leg.dist as i16;
        }
        if demand.from != leg.to // direct hit in the next leg
            // previous leg is from the same route
            && legs[i - 1].route_id == leg.route_id
            // the previous leg cannot be completed TASK!! in the future consider other statuses here
            && legs[i - 1].status != RouteStatus::COMPLETED as i32
            && (demand.from == leg.from // direct hit
              || (not_too_long
                    && DIST[leg.from as usize][demand.from as usize] + DIST[demand.from as usize][leg.to as usize]
                       < (leg.dist as f32 * extend_margin) as i16
                    && bearing_diff(stops[leg.from as usize].bearing, stops[demand.from as usize].bearing) < max_angle
                    && bearing_diff(stops[demand.from as usize].bearing, stops[leg.to as usize].bearing) < max_angle
                    ) // 5% TASK - global config, wait at stop?
             )
        // we want the previous leg to be active
        // to give some time for both parties to get the assignment
        {
          // OK, so we found the first 'pickup' leg, either direct hit or can be extended
          let mut to_found: bool = false;
          let mut distance_in_pool: i16 = 0;
          // we have found "from", now let's find "to"
          let mut k = i; // "to might be in the same leg as "from", we have to start from 'i'
          while k < legs.len() {
            if i != k { // 'i' countet already
              distance_in_pool += legs[k].dist as i16;
            }
            if !legs[k].route_id == leg.route_id {
              initial_distance = 0; // new route
              // won't find; this leg is the first leg in the next route and won't be checked as i++
              break;
            }
            if demand.to == legs[k].to { // direct hit
              to_found = true;
              break;
            }
            if not_too_long
                && DIST[legs[k].from as usize][demand.to as usize] 
                   + DIST[demand.to as usize][legs[k].to as usize]
                      < (legs[k].dist as f32 * extend_margin) as i16
                && bearing_diff(stops[legs[k].from as usize].bearing, stops[demand.to as usize].bearing) < max_angle
                && bearing_diff(stops[demand.to as usize].bearing, stops[legs[k].to as usize].bearing) < max_angle {
              // passenger is dropped before "getToStand", but the whole distance is counted above
              distance_in_pool -= DIST[demand.to as usize][legs[k].to as usize];
              to_found = true;
              break;
            }
            k += 1;
          }
          if to_found && demand.wait as i16 >= initial_distance
              // TASK: maybe distance*maxloss is a performance bug,
              // distanceWithLoss should be stored and used
              && (1.0 + demand.loss as f32 / 100.0) * demand.dist as f32 >= distance_in_pool as f32 {
              feasible.push(LegIndicesWithDistance{
                  idx_from: i as i32, 
                  idx_to: k as i32, 
                  dist: (initial_distance + distance_in_pool) as i32
              });
          }
          i = k;
        }
        i += 1;
      }
      // TASK: sjekk if demand.from == last leg.toStand - this might be feasible
      if feasible.len() == 0 { // empty
          return "nope".to_string();
      }
      feasible.sort_by_key(|e| e.dist.clone());
      // TASK: MAX LOSS check
      return modifyLeg(demand, legs, &mut feasible[0], max_leg_id);
    }
  }
  
fn modifyLeg(demand: &Order, legs: &mut Vec<Leg>, idxs: &mut LegIndicesWithDistance, max_leg_id: &mut i64) -> String {
    // pickup phase
    let mut sql: String = String::from("");
    let fromLeg: Leg = legs[idxs.idx_from as usize];
    println!("Order {} assigned to existing route: {}", demand.id, fromLeg.route_id);
    if demand.from == fromLeg.from { // direct hit, we don't modify that leg
        // TODO: eta should be calculated !!!!!!!!!!!!!!!!!!!!!
        sql += &assignOrder(demand.id, fromLeg.id, fromLeg.route_id, 0, "matchRoute IN");
    } else { 
    sql += &extendLegsInDB(demand.id, &fromLeg, demand.from, max_leg_id, "IN");
    // now assign the order to the new leg
    sql += &assignOrderFindLeg(demand.id, 
                                fromLeg.place + 1, fromLeg.route_id, 0, "extendRoute IN");

    // now do it all in the copy of the database - "legs" vector
    // legs will be used for another order, the list must be updated
    extendsLegsInVec(fromLeg, demand.from, idxs.idx_from, legs);
    }
    // drop-off phase
    let toLeg: Leg = legs[idxs.idx_to as usize];
    if demand.to != toLeg.to { // one leg more, ignore situation with ==
    sql += &extendLegsInDB(demand.id, &toLeg, demand.to, max_leg_id, "OUT");
    extendsLegsInVec(toLeg, demand.to, idxs.idx_to, legs);
    }
    return sql;
}
  
fn extendsLegsInVec(leg: Leg, from: i32, idx:i32, legs: &mut Vec<Leg>) {
  unsafe {
    // new leg
    let newLeg = Leg { 
        id: -1, // we don't know what it will be during insert
        route_id: leg.route_id,
        from: from,
        to: leg.to,
        place: leg.place + 1,
        dist:  DIST[from as usize][leg.to as usize] as i32,
        status: RouteStatus::ASSIGNED as i32,
        completed: None,
        started: None
    };
    legs.insert(idx as usize + 1, newLeg);
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
  
fn extendLegsInDB(order_id: i64, leg: &Leg, from: i32, max_leg_id: &mut i64, label: &str) -> String {
  unsafe {
    let mut sql: String = String::from("");
    println!("new, extended {} leg, route {}, place {}", label, leg.route_id, leg.place + 1);
    // we will add a new leg on "place", but there is already a leg with that place
    // we have to increment place in that leg and in all subsequent ones
    sql += &updatePlacesInLegs(leg.route_id, leg.place + 1);
    // one leg more in that free place
    sql += &create_leg(from,
                        leg.to,
                        leg.place + 1,
                        RouteStatus::ASSIGNED,
                        DIST[from as usize][leg.to as usize],
                        leg.route_id as i64, max_leg_id, // TODO: all IDs should be i64
                         &("route extender ".to_string() + &label.to_string()));

    // modify existing leg so that it goes to a new waypoint in-between
    if leg.id != -1 {
    sql += &updateLegABit(leg.id, from, 
                DIST[leg.from as usize][from as usize]);
    } else { // less efficient & more risky (there can always be a bug in "placing")
    sql += &updateLegABitWithRouteId(leg.route_id, leg.place, from, 
                DIST[leg.from as usize][from as usize]);
    }
    return sql;
  }
}

fn count_legs(id: i32, legs: &Vec<Leg>) -> i8 {
    let mut count: i8 = 0;
    for l in legs.iter() {
        if l.route_id == id {
            count += 1;
        }
    }
    return count;
}

pub fn getHandle(sql: String, label: String)  -> thread::JoinHandle<()> {
  return thread::spawn(move || {
      match Client::connect("postgresql://kabina:kaboot@localhost/kabina", NoTls) {
          Ok(mut c) => {
              if sql.len() > 0 {
                  c.batch_execute(&sql);
              }
          }
          Err(err) => {
              panic!("Could not connect DB in: {}", &label);
          }
      }
  });
}

