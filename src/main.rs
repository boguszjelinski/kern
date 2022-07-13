use postgres::{Client, NoTls, Error};
use chrono::{DateTime, TimeZone, NaiveDateTime, Utc, Local, FixedOffset, Duration};
use std::time::{SystemTime};

mod repo;
mod model;
mod distance;
use crate::model::{ Order, OrderStatus, Stop, Cab, CabStatus, Leg, RouteStatus };

const max_assign_time: i64 = 3;
const max_legs: i8 = 8;
const extend_margin : f32 = 1.05;
const max_angle: i16 = 120;

fn main() -> Result<(), Error> {
    let mut client = Client::connect("postgresql://kabina:kaboot@192.168.10.176/kabina", NoTls)?;

    let stops = repo::read_stops(&mut client);
    distance::init_distance(&stops);
    
    let orders = repo::find_orders_by_status_and_time(&mut client, OrderStatus::RECEIVED , Local::now() - Duration::minutes(5));
    println!("{}", orders.len());
    Ok(())
}

fn expire_orders(client: &mut Client, demand: & Vec<Order>) -> Vec<Order> {
    let mut ret: Vec<Order> = Vec::new();
    let mut ids: String = "".to_string();
    for o in demand.iter() {
      //if (o.getCustomer() == null) {
      //  continue; // TODO: how many such orders? the error comes from AddOrderAsync in API, update of Customer fails
      //}
        let minutesRcvd = match o.received.elapsed() {
            Ok(elapsed) => (elapsed.as_secs()/60) as i64,
            Err(_) => -1
        };
        let minutesAt : i64 = get_elapsed(o.at_time);
        
        if (minutesAt == -1 && minutesRcvd > max_assign_time)
                    || (minutesAt != -1 && minutesAt > max_assign_time) {
            ids = ids + &"order_id=".to_string() + &o.id.to_string() + &",".to_string();
            //OrderStatus.REFUSED
            client.execute(
                "UPDATE taxi_order SET status=6 WHERE id=$1", &[&o.id]);
        } else {
            ret.push(*o);
        }
    }
    if ids.len() > 0 {
      println!("{} refused, max assignment time exceeded", ids);
    }
    return ret;
}

fn get_elapsed(val: Option<SystemTime>) -> i64 {
    match val {
        Some(x) => { 
            match x.elapsed() {
                Ok(elapsed) => (elapsed.as_secs()/60) as i64,
                Err(_) => -1
            }
        }
        None => -1
    }
}

fn getRidOfDistantCustomers(demand: Vec<Order>, supply: Vec<Cab>) -> Vec<Order> {
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

fn getRidOfDistantCabs(demand: Vec<Order>, supply: Vec<Cab>) -> Vec<Cab>{
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

fn findMatchingRoutes(client: &mut Client, demand: Vec<Order>, stops: Vec<Stop>) -> Vec<Order> {
    if demand.len() == 0 {
        return demand;
    }
    let mut legs: Vec<Leg> = repo::find_legs_by_status(client, RouteStatus::ASSIGNED);
    if legs.len() == 0 {
        return demand;
    }
    println!("findMatchingRoutes START, orders count={} legs count={}", demand.len(), legs.len());
    let mut ret: Vec<Order> = Vec::new();
    let mut sql_bulk: String = String::from("");
    for taxiOrder in demand.iter() {
        let sql: String = try_to_extend_route(&taxiOrder, &mut legs, &stops);
        if sql == "nope" { // if not matched or extended
            ret.push(*taxiOrder); // it will go to pool finder
        } else {
            sql_bulk += &(sql + "\n");
        }
    }
    println!("findMatchingRoutes STOP, rest orders count={}", ret.len());
    // EXECUTE SQL !!

    return ret;
}

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

fn count_legs(id: i32, legs: &Vec<Leg>) -> i8 {
    let mut count: i8 = 0;
    for l in legs.iter() {
        if l.route_id == id {
            count += 1;
        }
    }
    return count;
}

fn bearing_diff(a: i16, b: i16) -> i16 {
    let mut r = (a - b) % 360;
    if r < -180 {
      r += 360;
    } else if r >= 180 {
      r -= 360;
    }
    return r.abs();
}

fn try_to_extend_route(demand: & Order, legs: &mut Vec<Leg>, stops: &Vec<Stop>) -> String {
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
                  && distance::DIST[leg.from as usize][demand.from as usize] + distance::DIST[demand.from as usize][leg.to as usize]
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
              && distance::DIST[legs[k].from as usize][demand.to as usize] 
                 + distance::DIST[demand.to as usize][legs[k].to as usize]
                    < (legs[k].dist as f32 * extend_margin) as i16
              && bearing_diff(stops[legs[k].from as usize].bearing, stops[demand.to as usize].bearing) < max_angle
              && bearing_diff(stops[demand.to as usize].bearing, stops[legs[k].to as usize].bearing) < max_angle {
            // passenger is dropped before "getToStand", but the whole distance is counted above
            distance_in_pool -= distance::DIST[demand.to as usize][legs[k].to as usize];
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
    return modifyLeg(demand, legs, &mut feasible[0]);
  }
}

fn modifyLeg(demand: &Order, legs: &mut Vec<Leg>, 
             idxs: &mut LegIndicesWithDistance) -> String {
    // pickup phase
    let mut sql: String = String::from("");
    let fromLeg: Leg = legs[idxs.idx_from as usize];
    println!("Order {} assigned to existing route: {}", demand.id, fromLeg.route_id);
    if demand.from == fromLeg.from { // direct hit, we don't modify that leg
        // TODO: eta should be calculated !!!!!!!!!!!!!!!!!!!!!
        sql += &repo::assignOrder(demand.id, fromLeg.id, fromLeg.route_id, 0, "matchRoute IN");
    } else { 
      sql += &extendLegsInDB(demand.id, &fromLeg, demand.from, "IN");
      // now assign the order to the new leg
      sql += &repo::assignOrderFindLeg(demand.id, 
                                fromLeg.place + 1, fromLeg.route_id, 0, "extendRoute IN");

      // now do it all in the copy of the database - "legs" vector
      // legs will be used for another order, the list must be updated
      extendsLegsInVec(fromLeg, demand.from, idxs.idx_from, legs);
    }
    // drop-off phase
    let toLeg: Leg = legs[idxs.idx_to as usize];
    if demand.to != toLeg.to { // one leg more, ignore situation with ==
      sql += &extendLegsInDB(demand.id, &toLeg, demand.to, "OUT");
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
        dist:  distance::DIST[from as usize][leg.to as usize] as i32,
        status: RouteStatus::ASSIGNED as i32,
        completed: None,
        started: None
    };
    legs.insert(idx as usize + 1, newLeg);
    // old leg
    legs[idx as usize].to = from;
    legs[idx as usize].dist = 
            distance::DIST[leg.from as usize][from as usize] as i32;

    // now "place" in route for next legs has to be incremented
    let mut i = idx as usize + 2;
    while i < legs.len() && legs[i].route_id == leg.route_id {
        legs[i].place += 1;
        i += 1;
    }
  }
}

fn extendLegsInDB(order_id: i64, leg: &Leg, from: i32, label: &str) -> String {
  unsafe {
    let mut sql: String = String::from("");
    println!("new, extended {} leg, route {}, place {}", label, leg.route_id, leg.place + 1);
    // we will add a new leg on "place", but there is already a leg with that place
    // we have to increment place in that leg and in all subsequent ones
    sql += &repo::updatePlacesInLegs(leg.route_id, leg.place + 1);
    // one leg more in that free place
    sql += &repo::create_leg(order_id, 
                              from,
                              leg.to,
                              leg.place + 1,
                              RouteStatus::ASSIGNED,
                              distance::DIST[from as usize][leg.to as usize],
                              leg.route_id, &("route extender ".to_string() + &label.to_string()));

    // modify existing leg so that it goes to a new waypoint in-between
    if leg.id != -1 {
      sql += &repo::updateLegABit(leg.id, from, 
                  distance::DIST[leg.from as usize][from as usize]);
    } else { // less efficient & more risky (there can always be a bug in "placing")
      sql += &repo::updateLegABitWithRouteId(leg.route_id, leg.place, from, 
                  distance::DIST[leg.from as usize][from as usize]);
    }
    return sql;
  }
}