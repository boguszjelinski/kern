use postgres::{Client};
use chrono::{DateTime, Local};
use std::time::{SystemTime};
use crate::model::{ Order, OrderStatus, Stop, Cab, CabStatus, Leg, RouteStatus, Branch,MAXORDERSNUMB };
use crate::distance::{DIST};
use crate::stats::{stats, Stat};

pub fn find_orders_by_status_and_time(client: &mut Client, status: OrderStatus, at_time: DateTime<Local>) -> Vec<Order> {
    let mut ret : Vec<Order> = Vec::new();
    let qry = "select id, from_stand, to_stand, max_wait, max_loss, distance, shared, in_pool, \
               received, started, completed, at_time, eta from taxi_order o where o.status = $1 \
               and (o.at_time is NULL or o.at_time < '".to_string() + &at_time.to_string() + &"')".to_string();
    for row in client.query(&qry, &[&(status as i32)]).unwrap() {
        ret.push(Order {
            id: row.get(0),
            from: row.get(1),
            to: row.get(2),
            wait: row.get(3),
            loss: row.get(4),
            dist: row.get(5),
            shared: row.get(6),
            in_pool: row.get(7),
            received: row.get::<usize,Option<SystemTime>>(8),
            started: row.get::<usize,Option<SystemTime>>(9),
            completed: row.get::<usize,Option<SystemTime>>(10),
            at_time: row.get::<usize,Option<SystemTime>>(11),
            eta: row.get(12)
        });
    }
    return ret;
}

pub fn read_stops(client: &mut Client) -> Vec<Stop> {
    let mut ret: Vec<Stop> = Vec::new();
    for row in client.query("SELECT id, latitude, longitude, bearing FROM stop", &[]).unwrap() {
        ret.push(Stop {
            id: row.get(0),
            latitude: row.get(1),
            longitude: row.get(2),
            bearing: row.get(3)
        });
    }
    return ret;
}

pub fn read_max(client: &mut Client, table: &str) -> i64 {
    for row in client.query(&("SELECT MAX(id) FROM ".to_string() + &table.to_string()), &[]).unwrap() {
        let max: Option<i64> = row.get(0);
        return match max {
            Some(x) => { x + 1 }
            None => 1
        }
    }
    return 1; // no row
}

pub fn find_cab_by_status(client: &mut Client, status: CabStatus) -> Vec<Cab>{
    let mut ret: Vec<Cab> = Vec::new();
    for row in client.query("SELECT id, location FROM cab WHERE status=$1", 
                                &[&(status as i32)]).unwrap() {
        ret.push(Cab {
            id: row.get(0),
            location: row.get(1)
        });
    }
    return ret;
}

pub fn find_legs_by_status(client: &mut Client, status: RouteStatus) -> Vec<Leg> {
    let mut ret: Vec<Leg> = Vec::new();
    for row in client.query("SELECT id, from_stand, to_stand, place, distance, started, completed, route_id, status \
        FROM leg WHERE status = $1 ORDER BY route_id ASC, place ASC", &[&(status as i32)]).unwrap() {
        ret.push(Leg {
            id: row.get(0),
            from: row.get(1),
            to: row.get(2),
            place: row.get(3),
            dist: row.get(4),
            started: row.get::<usize,Option<SystemTime>>(5),
            completed: row.get::<usize,Option<SystemTime>>(6),
            route_id: row.get(7), 
            status: row.get(8)
        });
    }
    return ret;
}

pub fn assignOrder(order_id: i64, leg_id: i64, route_id: i64, eta: i32, calledBy: &str) -> String {   
    println!("Assigning order_id={} to route_id={}, leg_id={}, routine {}",
                                            order_id, route_id, leg_id, calledBy);
    return format!("\
        UPDATE taxi_order AS o SET route_id={}, leg_id={}, cab_id=r.cab_id, status=1, eta={} \
        FROM route AS r WHERE r.id={} AND o.id={} AND o.status=0;", // it might be cancelled in the meantime, we have to be sure. 
        route_id, leg_id, eta, route_id, order_id);
}

pub fn assignOrder2(order_id: i64, cab_id: i64, leg_id: i64, route_id: i64, eta: i16, in_pool: &str, calledBy: &str) -> String {   
    println!("Assigning order_id={} to cab_id={}, route_id={}, leg_id={}, routine {}",
                                            order_id, cab_id, route_id, leg_id, calledBy);
    return format!("\
        UPDATE taxi_order SET route_id={}, leg_id={}, cab_id={}, status=1, eta={}, in_pool={} \
        WHERE id={} AND status=0;\n", // it might be cancelled in the meantime, we have to be sure. 
        route_id, leg_id, cab_id, eta, in_pool, order_id);
}

pub fn assignOrderFindLeg(order_id: i64, place: i32, route_id: i64, eta: i32, calledBy: &str) -> String {   
    println!("Assigning order_id={} to route_id={}, leg_id=UNKNOWN, place={}, routine {}",
                                            order_id, route_id, place, calledBy);
    return format!("\
        UPDATE taxi_order AS o SET route_id={}, leg_id=l.id, cab_id=r.cab_id, status=1, eta={} \
        FROM route AS r, leg AS l WHERE r.id={} AND l.route_id={} AND l.place={} \
        AND o.id={} AND o.status=0;", // it might be cancelled in the meantime, we have to be sure. 
        route_id, eta, route_id, route_id, place, order_id);
}

pub fn create_leg(from: i32, to: i32, place: i32, status: RouteStatus, dist: i16,
                  route_id: i64, max_leg_id: &mut i64, calledBy: &str) -> String {
    println!("Adding leg to route: route_id={}, routine {}", route_id, calledBy);
    let ret = format!("\
        INSERT INTO leg (id, from_stand, to_stand, place, distance, status, route_id) VALUES \
        ({},{},{},{},{},{},{});\n", *max_leg_id, from, to, place, dist, status as u8, route_id);
    *max_leg_id += 1;
    return ret;
}

pub fn updateLegABit(leg_id: i64, to: i32, dist: i16) -> String {
    println!("Updating existing leg_id={} to={}", leg_id, to);
    return format!("\
        UPDATE leg SET to_stand={}, distance={} \
        WHERE id={};\n", to, dist, leg_id);
}

pub fn updateLegABitWithRouteId(route_id: i64, place: i32, to: i32, dist: i16) -> String {
    // TODO: sjekk in log how many such cases
    println!("Updating existing leg with route_id={} place={} to={}", route_id, place, to);
    return format!("\
        UPDATE leg SET to_stand={}, distance={} \
        WHERE route_id={} AND place={};\n", to, dist, route_id, place);
}

pub fn updatePlacesInLegs(route_id: i64, place: i32) -> String {
    println!("Updating places in route_id={} starting with place={}", route_id, place);
    return format!("\
        UPDATE leg SET place=place+1 \
        WHERE route_id={} AND place >= {};\n", route_id, place);
}

pub fn assignPoolToCab(cab: Cab, orders: &[Order; MAXORDERSNUMB], pool: Branch, max_route_id: &mut i64, 
                        mut max_leg_id: &mut i64) -> String {
    let order = orders[pool.ordIDs[0] as usize];
    let mut place = 0;
    let mut eta = 0; // expected time of arrival
    let mut sql: String = assignCab(&cab, &order, &mut place, &mut eta, max_route_id, &mut max_leg_id);
    // legs & routes are assigned to customers in Pool
    // if not assigned to a Pool we have to create a single-task route here
    sql += &assignOrdersAndSaveLegsV2(cab.id, *max_route_id, place, pool, eta, &mut max_leg_id, orders);
    *max_route_id += 1;
    return sql;
}

fn assignCab(cab: &Cab, order: &Order, place: &mut i32, eta: &mut i16, max_route_id: &mut i64, max_leg_id: &mut i64) -> String {
    // 0: CabStatus.ASSIGNED TODO: hardcoded status
    let mut sql: String = String::from("UPDATE cab SET status=0 WHERE id=");
    sql += &(cab.id.to_string() + &";\n".to_string());
    // alter table route alter column id add generated always as identity
    // ALTER TABLE route ADD PRIMARY KEY (id)
    // ALTER TABLE taxi_order ALTER COLUMN customer_id DROP NOT NULL;
    sql += &format!("INSERT INTO route (id, status, cab_id) VALUES ({},{},{});\n", 
                    *max_route_id, 1, cab.id).to_string(); // 1=ASSIGNED

    if cab.location != order.from { // cab has to move to pickup the first customer
        unsafe {
            *eta = DIST[cab.location as usize][order.from as usize];
        }
        sql += &create_leg(order.from, cab.location, *place, RouteStatus::ASSIGNED, *eta,
                            *max_route_id, max_leg_id, "assignCab");
        *place += 1;
        //TODO: statSrvc.addToIntVal("total_pickup_distance", Math.abs(cab.getLocation() - order.fromStand));
    }
    return sql;
}

fn assignOrdersAndSaveLegsV2(cab_id: i64, route_id: i64, mut place: i32, e: Branch, mut eta: i16,
                                max_leg_id: &mut i64, orders: &[Order; MAXORDERSNUMB]) -> String {
    //logPool2(cab, route_id, e);
    let mut sql: String = String::from("");
    for c in 0 .. (e.ordNumb - 1) as usize {
      let order = orders[e.ordIDs[c] as usize];
      let stand1 = if e.ordActions[c] == 'i' as i8 { order.from } else { order.to };
      let stand2 = if e.ordActions[c + 1] == 'i' as i8
                        { orders[e.ordIDs[c + 1] as usize].from } else { orders[e.ordIDs[c + 1] as usize ].to } ;
      unsafe {
        let dist = DIST[stand1 as usize][stand2 as usize];
        let leg_id = *max_leg_id;

        if stand1 != stand2 { // there is movement
            sql += &create_leg(stand1, stand2, place, RouteStatus::ASSIGNED, dist,
                                route_id, max_leg_id, "assignOrdersAndSaveLegsV2");
            place += 1;
        }
        if e.ordActions[c] == 'i' as i8 {
            sql += &assignOrder2(order.id, cab_id, leg_id, route_id, eta, "true", "assignOrdersAndSaveLegs1");
        }
        if stand1 != stand2 {
            eta += dist;
        }
      }
    }
    return sql;
}

pub fn assignCustToCabLCM(sol: Vec<(i16,i16)>, cabs: &Vec<Cab>, demand: &Vec<Order>, max_route_id: &mut i64, 
                              max_leg_id: &mut i64) -> String {
    let mut sql: String = String::from("");
    for (_, (cab_idx, ord_idx)) in sol.iter().enumerate() {
        let order = demand[*ord_idx as usize];
        let mut place = 0;
        let mut eta = 0; // expected time of arrival
        sql += &assignCab(&cabs[*cab_idx as usize], &order, &mut place, &mut eta, max_route_id, max_leg_id);
        sql += &assignOrderToCab(order, cabs[*cab_idx as usize], place, eta, *max_route_id, max_leg_id, "assignCustToCabLCM");
        *max_route_id += 1;
    }
    return sql;
}

fn assignOrderToCab(order: Order, cab: Cab, place: i32, eta: i16, route_id: i64, 
                    max_leg_id: &mut i64, calledBy: &str) -> String {
    let mut sql: String = String::from("");
    unsafe {
        sql += &create_leg(order.from, order.to, place, RouteStatus::ASSIGNED, 
                       DIST[order.from as usize][order.to as usize], route_id, max_leg_id, calledBy);
    }
    sql += &assignOrder2(order.id, cab.id, *max_leg_id -1 , route_id, // -1 cause it is incremented in create_leg
                        eta, "false", "assignOrderToCab");
    return sql;
}

pub fn assignCustToCabMunkres(sol: Vec<i16>, cabs: &Vec<Cab>, demand: &Vec<Order>, max_route_id: &mut i64, 
                            max_leg_id: &mut i64) -> String {
    let mut sql: String = String::from("");
    for (cab_idx, ord_idx) in sol.iter().enumerate() {
        if *ord_idx == -1 {
            continue; // cab not assigned
        }
        let order = demand[*ord_idx as usize];
        let mut place = 0;
        let mut eta = 0; // expected time of arrival
        sql += &assignCab(&cabs[cab_idx], &order, &mut place, &mut eta, max_route_id, max_leg_id);
        sql += &assignOrderToCab(order, cabs[cab_idx], place, eta, *max_route_id, max_leg_id, "assignCustToCabMunkres");
        *max_route_id += 1;
    }
    return sql;
}

pub fn save_status(client: &mut Client, stat: [i64; Stat::TOTAL_PICKUP_DISTANCE as usize + 1]) {
    let mut sql: String = String::from("");
    for s in Stat::iterator() {
        sql += &format!("UPDATE stat SET int_val={} WHERE name={};", stat[*s as usize], s.to_string());
    }
    client.batch_execute(&sql);
}
