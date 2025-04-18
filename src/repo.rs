use std::cmp;
use log::{debug, warn};
use mysql::*;
use mysql::prelude::*;
use chrono::{Local, NaiveDateTime};
use crate::extender::STOP_WAIT;
use crate::model::{Branch, Cab, CabAssign, CabStatus, Leg, Order, OrderStatus, RouteStatus, Stop, MAXORDID};
use crate::distance::DIST;
use crate::stats::{STATS, Stat, add_avg_element, update_val, count_average};
use crate::utils::get_elapsed;

pub fn find_orders_by_status_and_time(conn: &mut PooledConn, status: OrderStatus, at_time: NaiveDateTime) -> Vec<Order> {
    let mut ret : Vec<Order> = Vec::new();
    let qry = "SELECT id, from_stand, to_stand, max_wait, max_loss, distance, shared, in_pool, \
               received, started, completed, at_time, eta, route_id FROM taxi_order WHERE status =".to_string() 
               + &(status as u8).to_string() + 
               &" and (at_time is NULL or at_time < '".to_string() + &at_time.to_string() + &"') ORDER by route_id".to_string();

    let selected: Result<Vec<Row>> = conn.query(qry);
    
    match selected {
        Ok(sel) => {
            for r in sel {
                ret.push(Order {
                    id: r.get(0).unwrap(),
                    from: r.get(1).unwrap(),
                    to: r.get(2).unwrap(),
                    wait: r.get(3).unwrap(),
                    loss: r.get(4).unwrap(),
                    dist: r.get(5).unwrap(),
                    //shared: r.get(6).unwrap(),
                    //in_pool: r.get(7).unwrap(),
                    received: get_naivedate(&r, 8),
                    //started: get_naivedate(&r, 9),
                    //completed: get_naivedate(&r, 10),
                    at_time: get_naivedate(&r, 11),
                    //eta: r.get(12).unwrap(),
                    route_id: if matches!(status, OrderStatus::RECEIVED) { -1 } else { get_i64(&r, 13) }
                });
            }
        },
        Err(error) => warn!("Problem reading row: {:?}", error),
    }
    return ret;
}

pub fn read_stops(conn: &mut PooledConn) -> Vec<Stop> {
    return conn.query_map(
        "SELECT id, latitude, longitude, bearing, capacity FROM stop",
        |(id, latitude, longitude, bearing, capacity)| {
            Stop { id, latitude, longitude, bearing, capacity }
        },
    ).unwrap();
}

pub fn read_free_taxi_orders(conn: &mut PooledConn) -> Vec<CabAssign> {
    return conn.query_map(
        "SELECT id, customer_id, cab_id, from_stand, to_stand, max_loss, shared, received FROM freetaxi_order",
        |(id, cust_id, cab_id, from, to, loss, shared, received)| {
            CabAssign { id, cust_id, cab_id, from, to, loss, shared, received }
        },
    ).unwrap();
}

pub fn read_max(conn: &mut PooledConn, table: &str) -> i64 {
    let qry = "SELECT MAX(id) FROM ".to_string() + &table.to_string();
    let selected: Result<Vec<Row>> = conn.query(qry);

    match selected {
        Ok(sel) => {
            for r in sel {
                let max: i64 = get_i64(&r, 0);
                return if max != -1 { max + 1 } else { 1 };
            }
        },
        Err(error) => warn!("Problem reading row: {:?}", error),
    }
    return 1; // no row
}

pub fn find_cab_by_status(conn: &mut PooledConn, status: CabStatus) -> Vec<Cab> {
    let mut sql = format!("SELECT id, location, seats FROM cab WHERE status={} ", status as u8);
    if status == CabStatus::FREE { // marked by Kim as FREE but a new route is created (while on last leg), 
        //so this cab is not free
        sql += &" AND id NOT IN (SELECT cab_id FROM route WHERE status IN (1,5) )"; 
    }
    return conn.query_map(sql, |(id, location, seats)| 
                            { Cab { id, location, seats, dist: 0 } },).unwrap();
}

pub fn find_free_cab_and_on_last_leg(conn: &mut PooledConn) -> Vec<Cab> {
    let mut free_cabs = find_cab_by_status(conn, CabStatus::FREE);

    let mut last_leg_cabs = conn.query_map( // 5: STARTED
        "SELECT cab_id, to_stand, seats, l1.started, distance FROM leg l1, route r, cab c \
            WHERE not exists (select * from leg l2 where l1.route_id = l2.route_id and \
                            l1.id!=l2.id AND l2.place>=l1.place) \
            AND l1.status=5 AND r.id = l1.route_id AND NOT locked AND c.id=cab_id".to_string(),
        |(id, location, seats, started, distance)| 
            {   
                let mut dist: i16 = distance; // default if we can't calculate elapsed
                let stamp: Option<NaiveDateTime> = started;
                let passed = get_elapsed(stamp);
                if passed != -1 { 
                    if passed as i16 > dist {
                        dist = 0;
                    } else {
                        dist -= passed as i16;
                    }
                }
                Cab { id, location, seats, dist} 
            },
    ).unwrap();
    last_leg_cabs.append(&mut free_cabs);
    return last_leg_cabs;
}

/*
pub fn get_list_of_free_cabs(conn: &mut PooledConn) -> Vec<i32>{
    let mut ret: Vec<i32> = Vec::new();
    let selected: Result<Vec<Row>>  = conn.query("SELECT id FROM cab WHERE status=1");
    match selected {
        Ok(sel) => { for r in sel { ret.push(r.get(0).unwrap()); }},
        Err(error) => warn!("Problem reading row: {:?}", error),
    }
    return ret;
}
*/

pub fn find_legs(conn: &mut PooledConn) -> Vec<Leg> {
    let mut ret: Vec<Leg> = Vec::new();
    let qry = "SELECT l.id, l.from_stand, l.to_stand, l.place, l.distance, l.started, l.completed, \
                    l.route_id, l.status, l.reserve, l.passengers, c.seats FROM leg l, route r, cab c \
                    WHERE r.id=l.route_id AND r.cab_id=c.id AND (l.status = 1 OR l.status = 5) AND NOT r.locked \
                    ORDER BY l.route_id ASC, l.place ASC";
    let selected: Result<Vec<Row>> = conn.query(qry);
    
    match selected {
        Ok(sel) => {
            for r in sel {
                ret.push(Leg {
                    id: r.get(0).unwrap(),
                    from: r.get(1).unwrap(),
                    to: r.get(2).unwrap(),
                    place: r.get(3).unwrap(),
                    dist: r.get(4).unwrap(),
                    started: get_naivedate(&r, 5),
                    //completed: get_naivedate(&r, 6),
                    route_id: r.get(7).unwrap(), 
                    status: get_route_status(r.get(8).unwrap()),
                    reserve: r.get(9).unwrap(),
                    passengers: r.get(10).unwrap(),
                    seats: r.get(11).unwrap(),
                });
            }
        },
        Err(error) => warn!("Problem reading row: {:?}", error),
    }
    return ret;
}

pub fn get_route_status(idx: i32) -> RouteStatus {
    return unsafe { ::std::mem::transmute(idx as i8) };
}

pub fn assign_order_find_cab(order_id: i64, leg_id: i64, route_id: i64, eta: i32, in_pool: &str, called_by: &str) -> String {   
    debug!("Assigning order_id={} to route_id={}, leg_id={}, module: {}",
                                            order_id, route_id, leg_id, called_by);
    if leg_id == -1 {
        return format!("\
        UPDATE taxi_order SET route_id={}, cab_id=(SELECT cab_id FROM route where id={}), status=1, eta={}, in_pool={} \
        WHERE id={} AND status=0;\n", // it might be cancelled in the meantime, we have to be sure. 
        route_id, route_id, eta, in_pool, order_id);
    }
    return format!("\
        UPDATE taxi_order SET route_id={}, leg_id={}, cab_id=(SELECT cab_id FROM route where id={}), status=1, eta={}, in_pool={} \
        WHERE id={} AND status=0;\n", // it might be cancelled in the meantime, we have to be sure. 
        route_id, leg_id, route_id, eta, in_pool, order_id);
}

pub fn assign_order(order_id: i64, cab_id: i64, leg_id: i64, route_id: i64, eta: i16, in_pool: &str, called_by: &str) -> String {   
    debug!("Assigning order_id={} to cab_id={}, route_id={}, leg_id={}, module: {}",
                                            order_id, cab_id, route_id, leg_id, called_by);
    return format!("\
        UPDATE taxi_order SET route_id={}, leg_id={}, cab_id={}, status=1, eta={}, in_pool={} \
        WHERE id={} AND status=0;\n", // it might be cancelled in the meantime, we have to be sure. 
        route_id, leg_id, cab_id, eta, in_pool, order_id);
}

pub fn assign_order_no_leg(order_id: i64, cab_id: i64, route_id: i64, eta: i16, in_pool: &str, called_by: &str) -> String {   
    debug!("Assigning order_id={} to cab_id={}, route_id={}, NO LEG, module: {}",
                                            order_id, cab_id, route_id, called_by);
    return format!("\
        UPDATE taxi_order SET route_id={}, cab_id={}, status=1, eta={}, in_pool={} \
        WHERE id={} AND status=0;\n", // it might be cancelled in the meantime, we have to be sure. 
        route_id, cab_id, eta, in_pool, order_id);
}

pub fn create_leg(order_id: i64, from: i32, to: i32, place: i32, status: RouteStatus, dist: i16, reserve: i32,
                  route_id: i64, max_leg_id: &mut i64, passengers: i8, called_by: &str) -> String {
    debug!("Adding leg to route: leg_id={}, route_id={}, order_id={}, from={}, to={}, place={}, distance={}, reserve={}, module: {}", 
                                *max_leg_id, route_id, order_id, from, to, place, dist,
                                cmp::max(reserve, 0), called_by);
    let ret = format!("\
        INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES \
        ({},{},{},{},{},{},{},{},{});\n", *max_leg_id, from, to, place, dist, status as u8, cmp::max(reserve, 0), route_id, passengers);
    *max_leg_id += 1;
    return ret;
}

pub fn update_leg_a_bit2(route_id: i64, leg_id: i64, to: i32, dist: i16, reserve: i32, passengers: i8) -> String {
    debug!("Updating existing route_id={}, leg_id={}, to={}, distance={}, reserve={}, passengers={}", 
                route_id, leg_id, to, dist, reserve, passengers);
    return format!("\
        UPDATE leg SET to_stand={}, distance={}, reserve={}, passengers={} WHERE id={};\n", 
        to, dist, reserve, passengers, leg_id);
}

pub fn update_place_in_legs_after(route_id: i64, place: i32) -> String {
    debug!("Updating places in route_id={} starting with place={}", route_id, place);
    return format!("UPDATE leg SET place=place+1 WHERE route_id={} AND place >= {};\n", route_id, place);
}

pub fn update_passengers_and_reserve_in_legs_between(route_id: i64, reserve: i32, place_from: i32, place_to: i32) -> String {
    if place_from > place_to {
        return "".to_string();
    }
    debug!("Updating passengers and reserve in route_id={}, reserve={} from place={} to place={}", 
                    route_id, reserve, place_from, place_to);
    return format!("\
        UPDATE leg SET passengers=passengers+1, reserve=LEAST(reserve, {}) WHERE route_id={} AND place BETWEEN {} AND {};\n", 
                    reserve, route_id, place_from, place_to);
}

pub fn update_reserve_after(route_id: i64, cost: i32, place_from: i32) -> String {
    if cost < 0 {
        return "".to_string();
    }
    debug!("Updating reserve in route_id={}, cost={} from place={}", route_id, cost, place_from);
    return format!("\
        UPDATE leg SET reserve=GREATEST(0, reserve-{}) WHERE route_id={} AND place >= {};\n", cost, route_id, place_from);
}

pub fn update_reserves_in_legs_before_and_including(route_id: i64, place: i32, wait_diff: i32) -> String {
    if place < 0 {
        return "".to_string();
    }
    debug!("Updating reserve in route_id={}, before place={}, wait_diff={}", 
            route_id, place, wait_diff);
    return format!("UPDATE leg SET reserve=LEAST(reserve, {}) WHERE route_id={} AND place <= {};\n", 
                wait_diff, route_id, place);
}

pub fn update_reserves_in_legs_before_and_including2(route_id: i64, place: i32, wait_diff: i32, cost: i32) -> String {
    if place < 0 {
        return "".to_string();
    }
    debug!("Updating reserve in route_id={}, BEFORE place={}, wait_diff={}", 
            route_id, place, wait_diff);
            // first reserves for other orders, decreased by added cost
    let mut sql = format!("\
        UPDATE leg SET reserve=GREATEST(0, reserve-{}) WHERE route_id={} AND place <= {};\n", cost, route_id, place);
        // for wait reserve for the current order 
    sql += format!("\
        UPDATE leg SET reserve=LEAST(reserve, {}) WHERE route_id={} AND place <= {};\n", wait_diff, route_id, place).as_str();
    return sql;
}

pub fn assign_pool_to_cab(cab: Cab, orders: &Vec<Order>, pool: Branch, max_route_id: &mut i64, mut max_leg_id: &mut i64, stop_wait: i16) -> String {
    let order = orders[pool.ord_ids[0] as usize];
    let mut place = 0;
    let mut eta = 0; // expected time of arrival
    let cab_dist = unsafe { DIST[cab.location as usize][orders[pool.ord_ids[0] as usize].from as usize] };
    let res = count_reserves(cab_dist, pool, orders, stop_wait);

    let mut sql: String = update_cab_add_route(&cab, &order, &mut place, &mut eta, res.0, max_route_id, &mut max_leg_id);
    // legs & routes are assigned to customers in Pool
    sql += &assign_orders_and_save_legs(cab.id, *max_route_id, place, pool, eta, &mut max_leg_id, orders, res.1);
    *max_route_id += 1;
    return sql;
}

// TODO: assign to free cab can make this assignment invalid, cab update and route & legs inserts must be atomic
// or protected - cab with status=1, route with conditional INSERT:
// INSERT INTO x_table (instance, user, item) SELECT 919191, 123, 456 
// WHERE (SELECT COUNT(*) FROM x_table WHERE user=123 AND item=456) = 0 
// 
// !! KEX does not have 'reserve' here, creat_leg get ZERO as a reserve
fn update_cab_add_route(cab: &Cab, order: &Order, place: &mut i32, eta: &mut i16, reserve: i32,  
                        max_route_id: &mut i64, max_leg_id: &mut i64) -> String {
    // 0: CabStatus.ASSIGNED TODO: hardcoded status
    let mut sql: String = String::from("UPDATE cab SET status=0 WHERE id=");
    sql += &(cab.id.to_string() + &";\n".to_string());
    // mark any active route as LOCKED
    sql += &format!("UPDATE route SET locked = true WHERE status IN (1,5) AND cab_id=");
    sql += &(cab.id.to_string() + &";\n".to_string());
    // then new route
    sql += &format!("INSERT INTO route (id, status, cab_id, locked) VALUES ({},{},{}, false);\n", // 0 will be updated soon
                    *max_route_id, 1, cab.id).to_string(); // 1=ASSIGNED

    if cab.location != order.from { // cab has to move to pickup the first customer
        unsafe {
            *eta = DIST[cab.location as usize][order.from as usize];
        }
        sql += &create_leg(order.id, cab.location, order.from, *place, 
                    RouteStatus::ASSIGNED, *eta, reserve,
                            *max_route_id, max_leg_id, 0, "assignCab");
        *place += 1;
        //TODO: statSrvc.addToIntVal("total_pickup_distance", Math.abs(cab.getLocation() - order.fromStand));
    }
    return sql;
}

// count reserves on legs
// reserves have to obey max_wait and max_loss
// returnes reserves for legs in Branch as well as in the leg for cab (if needed)
fn count_reserves(cab_dist: i16, br: Branch, orders: &Vec<Order>, stop_wait: i16) -> (i32, [i32; MAXORDID]) {
    // not all "c" values will produce legs below in "assign...", but we will use it as index for values -> res[c]
    let mut res: [i32; MAXORDID] = [16000; MAXORDID]; // we will decreas value
    // first max_wait
    let mut dist:i32 = cab_dist as i32;
    let cab_reserve:i32;
    for c in 0 .. (br.ord_numb - 1) as usize { // the last cell is 'o', no need to check
        if br.ord_actions[c] == 'i' as i8 {
            let mut reserve: i32 = orders[br.ord_ids[c] as usize].wait - dist;
            if reserve < 0 {
                warn!("Max wait of order {} is not met", orders[br.ord_ids[c] as usize].id);
                reserve = 0;
            }
            if c > 0 && reserve < res[c-1] { // we have to update all legs before, including current
                for d in 0..c + 1 {
                    res[d] = reserve;
                }
            }
        }
        /*if c > MAXINPOOL * 2 - 1 { // this should never happen: ..(br.ord_numb - 1) should always give max 6
            warn!("Strange index: {}", c);
            break;
        }*/
        let stand1: i32 = if br.ord_actions[c] == 'i' as i8 
                            { orders[br.ord_ids[c] as usize].from } else { orders[br.ord_ids[c] as usize].to };
        let stand2: i32 = if br.ord_actions[c + 1] == 'i' as i8
                          { orders[br.ord_ids[c + 1] as usize].from } else { orders[br.ord_ids[c + 1] as usize ].to };
        if stand1 != stand2 {
            unsafe { dist += (DIST[stand1 as usize][stand2 as usize] + stop_wait) as i32; }
        }
    }    
    cab_reserve = res[0]; // "wait" reserve for all legs before last 'i' will be the same, [0] is as good as any of them

    // max_loss
    for c in 0 .. (br.ord_numb - 1) as usize { // -1 as the last cell is 'o'
        if br.ord_actions[c] == 'i' as i8 {
            dist = 0;
            for d in c + 1 .. br.ord_numb as usize {
                let stand1: i32 = if br.ord_actions[d-1] == 'i' as i8 
                            { orders[br.ord_ids[d-1] as usize].from } else { orders[br.ord_ids[d-1] as usize].to };
                let stand2: i32 = if br.ord_actions[d] == 'i' as i8
                          { orders[br.ord_ids[d] as usize].from } else { orders[br.ord_ids[d] as usize ].to };
                if stand1 != stand2 {
                    unsafe { dist +=(DIST[stand1 as usize][stand2 as usize] + stop_wait) as i32; }
                }
                if br.ord_actions[d] == 'o' as i8 && br.ord_ids[d] == br.ord_ids[c] {
                    // TODO: this should not be counted each time, store it!!
                    let acceptable_distance: i32 = ((1.0 + orders[br.ord_ids[c] as usize].loss as f32 / 100.0) 
                                                    * orders[br.ord_ids[c] as usize].dist as f32) as i32;
                    let mut reserve:i32  = acceptable_distance - dist;
                    if reserve < 0 {
                        warn!("Max loss of order {} is not met", orders[br.ord_ids[c] as usize].id);
                        reserve = 0;
                    }
                    for e in c..d { // which means excluding d
                        if res[e] > reserve { // correct only those legs that have bigger reserve, there might be legs with smaller reserve
                            res[e] = reserve;
                        }
                    }
                }
            }
        }
    }
    return (cab_reserve, res);
}

fn log_pool(cab_id: i64, route_id: i64, e: Branch, orders: &Vec<Order>) {
    let mut branch:String = String::from("");
    for i in 0..e.ord_numb as usize {
        branch += &format!("{}{},", orders[e.ord_ids[i] as usize].id, (e.ord_actions[i] as u8) as char).to_string();
    }
    debug!("assign_orders_and_save_legs: route_id={}, cab_id={}, pool={}", route_id, cab_id, branch);
}

fn assign_orders_and_save_legs(cab_id: i64, route_id: i64, mut place: i32, e: Branch, mut eta: i16,
                                max_leg_id: &mut i64, orders: &Vec<Order>, reserve: [i32; MAXORDID]) -> String {
    log_pool(cab_id, route_id, e, orders);
    let mut sql: String = String::from("");
    let mut passengers: i8 = 0;

    for c in 0 .. (e.ord_numb - 1) as usize {
      let order = orders[e.ord_ids[c] as usize];
      let stand1: i32 = if e.ord_actions[c] == 'i' as i8 { order.from } else { order.to };
      let stand2: i32 = if e.ord_actions[c + 1] == 'i' as i8
                        { orders[e.ord_ids[c + 1] as usize].from } else { orders[e.ord_ids[c + 1] as usize ].to } ;
      unsafe {
        if e.ord_actions[c] == 'i' as i8 {
            passengers += 1;
        } else {
            passengers -= 1;
        }
        let dist: i16 = DIST[stand1 as usize][stand2 as usize];
        if stand1 != stand2 { // there is movement
            sql += &create_leg(order.id, stand1, stand2, place, RouteStatus::ASSIGNED, dist, reserve[c],
                                route_id, max_leg_id, passengers, "assignOrdersAndSaveLegs");
            place += 1;
        }
        if e.ord_actions[c] == 'i' as i8 {
            // if there are many orders from one stand, and cab is already there (ergo no leg added -> place==0)
            // then we don't have leg_id to assign to. This foreign key is not crucial for clients.
            // TODO: update orders with no leg in that route (leg_id=NULL) with the first leg, after all legs are created
            if place > 0 {
                // TODO: leg_id-1 might indicate a leg incomming to "from" or starting from "from", depending on stand1!=stand above 
                // leg_id-1 because create_leg increments ID
                sql += &assign_order(order.id, cab_id, *max_leg_id -1, route_id, eta, "true", "assignOrdersAndSaveLegs1");
            } else {
                sql += &assign_order_no_leg(order.id, cab_id, route_id, eta, "true", "assignOrdersAndSaveLegs2");
            }
            add_avg_element(Stat::AvgOrderAssignTime, get_elapsed(order.received));
        }
        if stand1 != stand2 {
            eta += dist + STOP_WAIT;
        }
      }
    }
    return sql;
}

pub fn assign_order_to_cab_lcm(sol: Vec<(i16,i16)>, cabs: &mut Vec<Cab>, demand: &mut Vec<Order>, max_route_id: &mut i64, 
                              max_leg_id: &mut i64) -> String {
    let mut sql: String = String::from("");
    for (_, (cab_idx, ord_idx)) in sol.iter().enumerate() {
        let order = demand[*ord_idx as usize];
        let cab: Cab = cabs[*cab_idx as usize];
        let mut place = 0;
        let mut eta: i16 = 0; // cab's leg is not important for customers
        // this leg should not be extended now, but it might be in the future with "last leg in active route" project
        // so we need to have a valid reserve
        let mut reserve: i32 = order.wait - unsafe { DIST[cab.location as usize][order.from as usize] } as i32; // expected time of arrival
        if reserve < 0 { reserve = 0; } 
        sql += &update_cab_add_route(&cab, &order, &mut place, &mut eta,  reserve, max_route_id, max_leg_id);
        let loss = unsafe { DIST[order.from as usize][order.to as usize] as f32
            * (100.0 + order.loss as f32) / 100.0 } as i32 ;
        if reserve > loss { reserve = loss; } 
        sql += &assign_order_to_cab(order, cab, place, eta, reserve, *max_route_id, max_leg_id, "assignCustToCabLCM");
        cabs[*cab_idx as usize].id = -1; // munkres should not assign this cab
        demand[*ord_idx as usize].id = -1;
        *max_route_id += 1;
    }
    return sql;
}

fn assign_order_to_cab(order: Order, cab: Cab, place: i32, eta: i16, reserve: i32, route_id: i64, 
                    max_leg_id: &mut i64, called_by: &str) -> String {
    let mut sql: String = String::from("");
    unsafe {
        sql += &create_leg(order.id, order.from, order.to, place, RouteStatus::ASSIGNED, 
                       DIST[order.from as usize][order.to as usize], reserve, route_id, max_leg_id, 1, called_by);
    }
    sql += &assign_order(order.id, cab.id, *max_leg_id -1 , route_id, // -1 cause it is incremented in create_leg
                        eta, "false", "assignOrderToCab");
    add_avg_element(Stat::AvgOrderAssignTime, get_elapsed(order.received));
    return sql;
}

pub fn assign_cust_to_cab_munkres(sol: Vec<i16>, cabs: &Vec<Cab>, demand: &Vec<Order>, max_route_id: &mut i64, 
                            max_leg_id: &mut i64) -> String {
    let mut sql: String = String::from("");
    
    for (cab_idx, ord_idx) in sol.iter().enumerate() {
        if *ord_idx == -1 {
            continue; // cab not assigned
        }
        let order = demand[*ord_idx as usize];
        let cab: Cab = cabs[cab_idx];
        let mut place = 0;
        let mut eta = 0; // expected time of arrival, see comments in LCM above
        let mut reserve: i32 = order.wait - unsafe { DIST[cab.location as usize][order.from as usize] } as i32; // expected time of arrival
        if reserve < 0 { 
            // TODO/TASK we should communicate with the customer, if this is acceptable, more than WAIT TIME
            reserve = 0; 
        } 
        
        let loss = unsafe { DIST[order.from as usize][order.to as usize] as f32 * (order.loss as f32) / 100.0 } as i32 ;
        if reserve > loss { reserve = loss; } 
        sql += &update_cab_add_route(&cab, &order, &mut place, &mut eta, reserve, max_route_id, max_leg_id);
        sql += &assign_order_to_cab(order, cabs[cab_idx], place, eta, reserve, *max_route_id, max_leg_id, "assignCustToCabMunkres");
        *max_route_id += 1;
    }
    return sql;
}

pub fn create_reloc_route(cab: &Cab, dest_stop: i64,  
                            max_route_id: &mut i64, max_leg_id: &mut i64) -> String {
    let mut place = 0;
    let mut eta = 0;
    let reserve = 0;
    
    // fake order
    let order = Order { id: -1, from: dest_stop as i32, to: -1, wait: 0, loss: 0, dist: 0, 
                                received: None, at_time: None, route_id: -1 };
    let sql = update_cab_add_route(&cab, &order, &mut place, &mut eta, reserve, max_route_id, max_leg_id);
    *max_route_id += 1;
    return sql;
}

pub fn save_status() -> String {
    let mut sql: String = String::from("");
    update_val(Stat::AvgOrderAssignTime, count_average(Stat::AvgOrderAssignTime));
    unsafe {
    for s in Stat::iterator() {
        sql += &format!("UPDATE stat SET int_val={} WHERE UPPER(name)=UPPER('{}');", STATS[*s as usize], s.to_string());
    }}
    return sql;
}

pub fn assign_requests_for_free_cabs(conn: &mut PooledConn, max_route_id: &mut i64, max_leg_id: &mut i64) {
    // list free cabs
    // retrieve requests from free cabs
    // in a loop over freetaxi_order
    // a) set cab status assigned but if assigned by e.g. pool - do nothing, it will be deleted
    // b) create route and assign to cab
    // c) generate taxi_order and assign to cab and route
    // d) generate one leg and assign to route
    // after the loop - delete all retreived requests (new might have come)
    let orders = read_free_taxi_orders(conn);
    let cabs = find_cab_by_status(conn, CabStatus::FREE);
    if orders.len() == 0 {
        return;
    }
    if cabs.len() == 0 {
        warn!("No cabs available");
        delete_req_for_free_cabs(orders);
        return;
    }
    let mut ids: Vec<i64> = vec![];
    for c in &cabs {
        ids.push(c.id);
    }
    let mut sql: String = "".to_string();
    for o in &orders {
        if !ids.contains(&o.cab_id) { //this cab is not free any longer, assigned by pool e.g.
            continue;
        }
        let loc = cab_location(&cabs, o.cab_id);
        if loc != o.from {
            warn!("Requested free cab had other location, DB: {}, reuqested: {}", loc, o.from);
        }
        let reserve = ((o.loss as f32 / 100.0) * unsafe {DIST[o.from as usize][o.to as usize]} as f32).round() as i32;
        sql += &format!("UPDATE cab SET status=0 WHERE id={};\n", o.cab_id);
        sql += &insert_route(*max_route_id, o.cab_id);
        sql += &insert_leg(*max_leg_id, *max_route_id, o, reserve);
        sql += &insert_order(*max_leg_id, *max_route_id, o);
        *max_route_id += 1;
        *max_leg_id += 1;
    }
    sql += &delete_req_for_free_cabs(orders);
    run_sql(conn, sql);
}

fn cab_location(cabs: &Vec<Cab>, cab_id: i64) -> i32 {
    return match cabs.into_iter().find(|x| x.id == cab_id) {
        Some(x) => { x.location }
        None => -1
    };
}

pub fn run_sql(conn: &mut PooledConn, sql: String) {
    if sql.len() > 0 {
        match conn.query_iter(sql) { // here SYNC execution
            Ok(_) => {}
            Err(err) => {
                warn!("Solver SQL output failed to run, err: {}", err);
            }
        }
    }
}

fn delete_req_for_free_cabs(list: Vec<CabAssign>) -> String {
    if list.len() == 0 {
        return "".to_string();
    }
    let mut ids: String = "".to_string();
    for i in 0 .. list.len() -1 {
        ids += &format!("{},", list[i].id);
    }
    ids += &list[list.len() - 1].id.to_string();
    return format!("DELETE FROM freetaxi_order WHERE id IN ({});\n", ids);
}

fn insert_order(route_id: i64, leg_id: i64, o: &CabAssign) -> String {
    return format!("INSERT INTO taxi_order (from_stand, to_stand, max_loss, max_wait, shared, in_pool, eta,\
                     status, received, distance, customer_id, cab_id, leg_id, route_id) VALUES (\
                    {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});\n",
            o.from, o.to, o.loss, -1, o.shared, false, 0, OrderStatus::ASSIGNED as i32,
            naive_to_string(o.received), unsafe {DIST[o.from as usize][o.to as usize]}, o.cust_id, o.cab_id, leg_id, route_id);
}

fn naive_to_string(time: Option<NaiveDateTime>) -> NaiveDateTime {
    return match time {
        Some(x) => { x }
        None => Local::now().naive_local()
    };
}

fn insert_route(route_id: i64, cab_id: i64) -> String {
    return format!("INSERT INTO route (id, status, cab_id, locked) VALUES ({},{},{},false);\n", // 0 will be updated soon
                    route_id, RouteStatus::ASSIGNED as i32, cab_id).to_string();
}

// TODO: number of passengers requested
fn insert_leg(_leg_id: i64, route_id: i64, o: &CabAssign, _reserve: i32) -> String {
    /*let ret = format!("\
    INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES \
    ({},{},{},{},{},{},{},{},{});\n", leg_id, o.from, o.to, 0, unsafe {DIST[o.from as usize][o.to as usize]}, RouteStatus::ASSIGNED as i32, 
        reserve, route_id, 1);
    */
    return format!("INSERT INTO route (id, status, cab_id, locked) VALUES ({},{},{},false);\n", // 0 will be updated soon
                    route_id, RouteStatus::ASSIGNED as i32, o.cab_id).to_string();
}


fn get_naivedate(row: &Row, index: usize) -> Option<NaiveDateTime> {
    let val: Option<mysql::Value> = row.get(index);
    return match val {
        Some(x) => {
            if x == Value::NULL {
                None
            } else {
                row.get(index)
            }
        }
        None => None
    };
}

fn get_i64(row: &Row, index: usize) -> i64 {
    let val: Option<mysql::Value> = row.get(index);
    return match val {
        Some(x) => {
            if x == Value::NULL {
                -1
            } else {
                row.get(index).unwrap()
            }
        }
        None => -1
    };
}

#[cfg(test)]
mod tests {
  use super::*;
  use serial_test::serial;
  use crate::model::MAXORDERSNUMB;

  fn init_test_data(order_count: u8) -> [Order; MAXORDERSNUMB] {
    let stop_count = 8;
    unsafe {
        for i in 0..stop_count { DIST[i][i+1]= 2 ; }
        for i in 0..order_count as usize { 
            DIST[i][stop_count -1 -i] = 2*(stop_count -1 -i*2) as i16;
        }
    }
    let o: Order = Order { id: 0, from: 0, to: stop_count as i32 - 1, wait: 10, loss: 90, dist: 7, 
        //shared: true, in_pool: true, 
                            received: None, 
                            //started: None, completed: None, 
                            at_time: None, 
                            //eta: 10, 
                            route_id: -1 };
    let mut orders: [Order; MAXORDERSNUMB] = [o; MAXORDERSNUMB];
    for i in 0..order_count as usize {
        let to: i32 = stop_count as i32 -1 -i as i32;
        unsafe{
            orders[i] = Order { id: i as i64, from: i as i32, to: to, wait: 10, loss: 90, dist: DIST[i as usize][to as usize] as i32, 
                            //shared: true, in_pool: true, 
                            received: None, 
                            //started: None, completed: None, 
                            at_time: None, 
                            //eta: 10, 
                            route_id: -1 };
        }
    }
    return orders;
  }

  fn get_test_branch(order_count: u8) -> Branch {
    let mut br: Branch = Branch::new();
    br.cost = 1;
    br.outs = order_count;
    br.ord_numb = (order_count * 2) as i16;
    br.ord_ids = [0,1,2,3,3,2,1,0];
    br.ord_actions = ['i' as i8, 'i' as i8, 'i' as i8, 'i' as i8, 'o' as i8, 'o' as i8, 'o' as i8, 'o' as i8,];
    br.cab = 0;
    return br;
  }

  #[test]
  #[serial]
  #[ignore]
  fn test_assign_orders_and_save_legs() {
    let place = 0;
    let eta: i16 =0;
    let mut max_leg_id: i64 =0;
    let order_count = 4;

    let br = get_test_branch(order_count);
    
    let orders = init_test_data(order_count);
    let cab = Cab { id:0, location:0, seats: 10, dist: 0 };
    let reserves: [i32; MAXORDID] = [0; MAXORDID];
    let sql = assign_orders_and_save_legs(cab.id, 0, place, br, eta, &mut max_leg_id, &orders.to_vec(), reserves);
    //println!("{}", sql);
    assert_eq!(sql, "INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (0,0,1,0,2,1,0,0,1);\nUPDATE taxi_order SET route_id=0, leg_id=0, cab_id=0, status=1, eta=0, in_pool=true WHERE id=0 AND status=0;\nINSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (1,1,2,1,2,1,0,0,2);\nUPDATE taxi_order SET route_id=0, leg_id=1, cab_id=0, status=1, eta=3, in_pool=true WHERE id=1 AND status=0;\nINSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (2,2,3,2,2,1,0,0,3);\nUPDATE taxi_order SET route_id=0, leg_id=2, cab_id=0, status=1, eta=6, in_pool=true WHERE id=2 AND status=0;\nINSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (3,3,0,3,0,1,0,0,4);\nUPDATE taxi_order SET route_id=0, leg_id=3, cab_id=0, status=1, eta=9, in_pool=true WHERE id=3 AND status=0;\nINSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (4,0,7,4,14,1,0,0,5);\nUPDATE taxi_order SET route_id=0, leg_id=4, cab_id=0, status=1, eta=10, in_pool=true WHERE id=0 AND status=0;\nINSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (5,7,4,5,0,1,0,0,4);\nINSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (6,4,5,6,2,1,0,0,3);\n");
  }

/*
  #[test]
  fn test_check_route_reserve() {
    let order_count = 4;
    let br = get_test_branch(order_count);
    let orders = init_test_data(order_count);
    // this is a linear route, so we have to have 90% reserve of the shortest distance on that route
    // 90% of 1 is 0 (truncated int)
    // so therefore we had to use '2' as minimal distance
    assert_eq!(check_route_reserve(br, 0, &orders), 12); // 14*90%-14 = 12.6 (solo*loss - actual duration)
    assert_eq!(check_route_reserve(br, 3, &orders), 1); // 2*90%-1 = 1
  }
*/
}
