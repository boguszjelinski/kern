use pool::orders_to_array;
use postgres::{Client, NoTls, Error};
use chrono::{Local, Duration};
use std::time::{SystemTime};
use std::{thread};

mod repo;
mod model;
mod distance;
mod extender;
mod pool;
use crate::model::{Order,OrderStatus,OrderTransfer,Stop,Cab,CabStatus,Branch,MAXSTOPSNUMB,MAXCABSNUMB,MAXORDERSNUMB,MAXBRANCHNUMB};
use crate::extender::{ findMatchingRoutes, writeSqlToFile };
use crate::distance::{DIST};
use crate::pool::{orders_to_transfer_array, array_to_orders, cabs_to_array, stops_to_array, find_pool};
use crate::repo::{assignPoolToCab};

const max_assign_time: i64 = 3;

fn main() -> Result<(), Error> {
    println!("cargo:rustc-link-lib=dynapool30");
    let mut client = Client::connect("postgresql://kabina:kaboot@localhost:5432/kabina", NoTls)?; // 192.168.10.176
    let stops = repo::read_stops(&mut client);
    distance::init_distance(&stops);

    let tmpModel = prepare_data(&mut client);
    match tmpModel {
        Some(mut x) => { 
            dispatch(&mut client, &mut x.0, &mut x.1, stops);
         },
        None => {
            println!("Nothing to do");
        }
    }
    Ok(())
}

#[link(name = "dynapool30")]
extern "C" {
    fn dynapool(
		numbThreads: i32,
		distance: &[[i16; MAXSTOPSNUMB]; MAXSTOPSNUMB],
		distSize: i32,
		stops: &[Stop; MAXSTOPSNUMB],
		stopsSize: i32,
		orders: &[OrderTransfer; MAXORDERSNUMB],
		ordersSize: i32,
		cabs: &[Cab; MAXCABSNUMB],
		cabsSize: i32,
		ret: &mut [Branch; MAXBRANCHNUMB], // returned values
		retSize: i32,
		count: &mut i32 // returned count of values
    );
}

fn dispatch(client: &mut Client, orders: &mut Vec<Order>, mut cabs: &mut Vec<Cab>, stops: Vec<Stop>) {
    let use_ext_pool: bool = false;
    let thread_numb: i32 = 4;
    let mut max_route_id : i64 = repo::read_max(client, "route");
    let mut max_leg_id : i64 = repo::read_max(client, "leg");
    let lenBefore = orders.len();
    // ROUTE EXTENDER
    let ret = findMatchingRoutes(client, orders, &stops);
    let mut demand = ret.0;
    let lenAfter = demand.len();
    let extenderHandle: thread::JoinHandle<()> = ret.1;
    if lenBefore != lenAfter {
      println!("Route extender found allocated {} requests", lenBefore - lenAfter);
    }
    let mut pl: Vec<Branch> = Vec::new();
    let mut sql: String = String::from("");
    if use_ext_pool {
        (pl, sql) = find_extern_pool(demand, cabs, stops, thread_numb, max_route_id, max_leg_id);
    } else {
        // TODO: only pool of four??
        for p in (2..5).rev() { // 4,3,2
            let mut ret = find_pool(p, thread_numb as i16,
                    &mut demand, &mut cabs, &stops, &mut max_route_id, &mut max_leg_id);
            pl.append(&mut ret.0);
            sql += &ret.1;
            println!("Pools for inPool={}: {}", p, ret.0.len());
        }
    }
    writeSqlToFile(&sql, "pool");
    let poolHandle: thread::JoinHandle<_> = thread::spawn(move || {
        match Client::connect("postgresql://kabina:kaboot@localhost/kabina", NoTls) {
            Ok(mut c) => {
                //                if sql.len() > 0 {
                //        c.batch_execute(&sql);
                //    }
            }
            Err(err) => {
                panic!("Pool could not connect DB");
            }
        }
    });
    // marking assigned orders to get rid of them; cabs are marked in find_pool 
    let numb = markOrders(pl, orders);
    println!("Number of assigned orders: {}", numb);

    // we have to join so that the next run of dispatcher gets updated orders
    extenderHandle.join().expect("Extender SQL thread being joined has panicked");
    poolHandle.join().expect("Pool SQL thread being joined has panicked");
}

fn markOrders(pl: Vec<Branch>, orders: &mut Vec<Order>) -> i32 {
    let mut count = 0;
    for b in pl.iter() {
        for o in 0..b.ordNumb as usize {
            if b.ordActions[o] == 'i' as i8 { // do not count twice
                orders[b.ordIDs[o] as usize].id = -1;
                count += 1;
            }
        }
    }
    return count;
}

fn find_extern_pool(demand: Vec<Order>, cabs: &mut Vec<Cab>, stops: Vec<Stop>, threads: i32,
                    mut max_route_id: i64, mut max_leg_id: i64) -> (Vec<Branch>, String) {
    let mut ret: Vec<Branch> = Vec::new();  
    let orders: [OrderTransfer; MAXORDERSNUMB] = orders_to_transfer_array(&demand);
    let mut br: [Branch; MAXBRANCHNUMB] = [Branch::new(); MAXBRANCHNUMB];
    let mut cnt: i32 = 0;  
    unsafe {
        dynapool(
            threads,
            &DIST,
            MAXSTOPSNUMB as i32,
            &stops_to_array(&stops),
            stops.len() as i32,
            &orders,
            demand.len() as i32,
            &cabs_to_array(&cabs),
            cabs.len() as i32,
            &mut br, // returned values
            MAXBRANCHNUMB as i32,
            &mut cnt // returned count of values
        );
    }
    let mut sql: String = String::from("");
    for i in 0 .. cnt as usize {
        ret.push(br[i]); // just convert to vec
        sql += &assignPoolToCab(cabs[br[i].cab as usize], &orders_to_array(&demand), br[i], &mut max_route_id, &mut max_leg_id);
        // remove the cab from list so that it cannot be allocated twice, by LCM or Munkres
        cabs[br[i].cab as usize].id = -1;
        
    }
    //  RUN SQL
    return (ret, sql);
  }

fn prepare_data(client: &mut Client) -> Option<(Vec<Order>, Vec<Cab>)> {
    let mut orders = repo::find_orders_by_status_and_time(
                client, OrderStatus::RECEIVED , Local::now() - Duration::minutes(5));
    if orders.len() == 0 {
        println!("No demand");
        return None;
    }
    println!("Orders, input: {}", orders.len());
    
//    orders = expire_orders(client, &orders);
    if orders.len() == 0 {
        println!("No demand, expired");
        return None;
    }
    let mut cabs = repo::find_cab_by_status(client, CabStatus::FREE);
    if orders.len() == 0 || cabs.len() == 0 {
        println!("No cabs available");
        return None;
    }
    println!("Initial count, demand={}, supply={}", orders.len(), cabs.len());
    orders = getRidOfDistantCustomers(&orders, &cabs);
    if orders.len() == 0 {
      println!("No suitable demand, too distant");
      return None; 
    }
    cabs = getRidOfDistantCabs(&orders, &cabs);
    if cabs.len() == 0 {
      println!("No cabs available, too distant");
      return None; 
    }
    return Some((orders, cabs));
}

// TODO: bulk update
fn expire_orders(client: &mut Client, demand: & Vec<Order>) -> Vec<Order> {
    let mut ret: Vec<Order> = Vec::new();
    let mut ids: String = "".to_string();
    for o in demand.iter() {
      //if (o.getCustomer() == null) {
      //  continue; // TODO: how many such orders? the error comes from AddOrderAsync in API, update of Customer fails
      //}
        let minutesRcvd = get_elapsed(o.received);
        let minutesAt : i64 = get_elapsed(o.at_time);
        
        if (minutesAt == -1 && minutesRcvd > max_assign_time)
                    || (minutesAt != -1 && minutesAt > max_assign_time) {
            ids = ids + &o.id.to_string() + &",".to_string();
        } else {
            ret.push(*o);
        }
    }
    if ids.len() > 0 {
      let sql = ids[0..ids.len() - 1].to_string(); // remove last comma
      client.execute(
        "UPDATE taxi_order SET status=6 WHERE id IN ($1);\n", &[&sql]); //OrderStatus.REFUSED
      println!("{} refused, max assignment time exceeded", &ids);
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

fn getRidOfDistantCustomers(demand: &Vec<Order>, supply: &Vec<Cab>) -> Vec<Order> {
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

fn getRidOfDistantCabs(demand: &Vec<Order>, supply: &Vec<Cab>) -> Vec<Cab> {
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
