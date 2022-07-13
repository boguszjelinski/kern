use postgres::{Client, NoTls, Error};
use chrono::{DateTime, TimeZone, NaiveDateTime, Utc, Local, FixedOffset, Duration};
use std::time::{SystemTime};

use crate::model::{ Order, OrderStatus, Stop, Cab, CabStatus, Leg, RouteStatus };

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
            received: row.get::<usize,SystemTime>(8),
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

pub fn assignOrder(order_id: i64, leg_id: i32, route_id: i32, eta: i32, calledBy: &str) -> String {   
    println!("Assigning order_id={} to route_id={}, leg_id={}, routine {}",
                                            order_id, route_id, leg_id, calledBy);
    return format!("\
        UPDATE o SET o.route_id={}, o.leg_id={}, o.cab_id=r.cab_id, o.status=1, o.eta={} \
        FROM taxi_order o INNER JOIN route r ON r.id={} \
        WHERE o.id={} AND o.status=0", // it might be cancelled in the meantime, we have to be sure. 
        route_id, leg_id, eta, route_id, order_id);
}

pub fn assignOrderFindLeg(order_id: i64, place: i32, route_id: i32, eta: i32, calledBy: &str) -> String {   
    println!("Assigning order_id={} to route_id={}, leg_id=UNKNOWN, place={}, routine {}",
                                            order_id, route_id, place, calledBy);
    return format!("\
        UPDATE o SET o.route_id={}, o.leg_id=l.id, o.cab_id=r.cab_id, o.status=1, o.eta={} \
        FROM taxi_order o INNER JOIN route r ON r.id={} \
        INNER JOIN leg l ON l.route_id={} AND l.place={} \
        WHERE o.id={} AND o.status=0", // it might be cancelled in the meantime, we have to be sure. 
        route_id, eta, route_id, route_id, place, order_id);
}

pub fn create_leg(order_id: i64, from: i32, to: i32, place: i32, status: RouteStatus, dist: i16,
                  route_id: i32, calledBy: &str) -> String {
    println!("Adding leg to route: route_id={}, routine {}", route_id, calledBy);
    return format!("\
        INSERT INTO leg (from_stand, to_stand, place, distance, status, route_id) VALUES \
        ({},{},{},{},{},{})", from, to, place, dist, status as u8, route_id);
}

pub fn updateLegABit(leg_id: i32, to: i32, dist: i16) -> String {
    println!("Updating existing leg_id={} to={}", leg_id, to);
    return format!("\
        UPDATE leg SET to_stand={}, distance={} \
        WHERE id={}", to, dist, leg_id);
}

pub fn updateLegABitWithRouteId(route_id: i32, place: i32, to: i32, dist: i16) -> String {
    // TODO: sjekk in log how many such cases
    println!("Updating existing leg with route_id={} place={} to={}", route_id, place, to);
    return format!("\
        UPDATE leg SET to_stand={}, distance={} \
        WHERE route_id={} AND place={}", to, dist, route_id, place);
}

pub fn updatePlacesInLegs(route_id: i32, place: i32) -> String {
    println!("Updating places in route_id={} starting with place={}", route_id, place);
    return format!("\
        UPDATE leg SET place=place+1 \
        WHERE route_id={} AND place >= {}", route_id, place);
}
