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