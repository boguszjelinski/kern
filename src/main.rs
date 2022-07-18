use postgres::{Client, NoTls, Error};
use chrono::{Local, Duration};
use std::time::{SystemTime};

mod repo;
mod model;
mod distance;
mod extender;
mod pool;
use crate::model::{Order,OrderStatus,Stop,Cab,CabStatus,Branch,MAXSTOPSNUMB,MAXCABSNUMB,MAXORDERSNUMB,MAXBRANCHNUMB};
use crate::extender::{ findMatchingRoutes };
use crate::distance::{DIST};
use crate::pool::{orders_to_array, cabs_to_array, stops_to_array, find_pool};
use crate::repo::{assignPoolToCab};

const max_assign_time: i64 = 3;

fn main() -> Result<(), Error> {
    let mut client = Client::connect("postgresql://kabina:kaboot@localhost/kabina", NoTls)?; // 192.168.10.176
    let stops = repo::read_stops(&mut client);
    distance::init_distance(&stops);

    let tmpModel = prepare_data(&mut client);
    match tmpModel {
        Some(mut x) => { 
            dispatch(&mut client, x.0, &mut x.1, stops);
         },
        None => {
            println!("Nothing to do");
        }
    }
    Ok(())
}

#[link(name = "dynapool25")]
extern "C" {
    fn dynapool(
		numbThreads: i32,
		distance: &[[i16; MAXSTOPSNUMB]; MAXSTOPSNUMB],
		distSize: i32,
		stops: &[Stop; MAXSTOPSNUMB],
		stopsSize: i32,
		orders: &[Order; MAXORDERSNUMB],
		ordersSize: i32,
		cabs: &[Cab; MAXCABSNUMB],
		cabsSize: i32,
		ret: &mut [Branch; MAXBRANCHNUMB], // returned values
		retSize: i32,
		count: &mut i32 // returned count of values
    );
}

fn dispatch(client: &mut Client, orders: Vec<Order>, mut cabs: &mut Vec<Cab>, stops: Vec<Stop>) {
    let use_ext_pool: bool = true;
    let thread_numb: i32 = 4;
    let mut max_route_id : i32 = repo::read_max(client, "route");
    let mut max_leg_id : i32 = repo::read_max(client, "leg");
    let lenBefore = orders.len();
    // ROUTE EXTENDER
    let demand = findMatchingRoutes(client, orders, &stops);
    let lenAfter = demand.0.len();
    if lenBefore != lenAfter {
      println!("Route extender found allocated {} requests", lenBefore - lenAfter);
    }
    let mut pl: Vec<Branch> = Vec::new();
    
    if use_ext_pool {
        pl = find_extern_pool(demand.0, cabs, stops, thread_numb, max_route_id, max_leg_id);
    } else {
        // TODO: only pool of four??
        for p in (2..4).rev() { // 4,3,2
            let mut ret = find_pool(p, thread_numb as i16, &demand.0, &mut cabs, &stops, 
                                    &mut max_route_id, &mut max_leg_id);
            pl.append(&mut ret.0);
            println!("Pools for inPool={}: {}", p, ret.0.len());
        }
    }
}

fn find_extern_pool(demand: Vec<Order>, cabs: &mut Vec<Cab>, stops: Vec<Stop>, threads: i32,
                    mut max_route_id: i32, mut max_leg_id: i32) -> Vec<Branch> {
    let mut ret: Vec<Branch> = Vec::new();  
    let orders: [Order; MAXORDERSNUMB] = orders_to_array(&demand);
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
        sql += &assignPoolToCab(cabs[br[i].cab as usize], &orders, br[i], &mut max_route_id, &mut max_leg_id);
        // remove the cab from list so that it cannot be allocated twice, by LCM or Munkres
        cabs[br[i].cab as usize].id = -1;
    }
    //  RUN SQL
    return ret;
  }

fn prepare_data(client: &mut Client) -> Option<(Vec<Order>, Vec<Cab>)> {
    let mut orders = repo::find_orders_by_status_and_time(
                client, OrderStatus::RECEIVED , Local::now() - Duration::minutes(5));
    if orders.len() == 0 {
        println!("No demand");
        return None;
    }
    println!("Orders, input: {}", orders.len());
    
    orders = expire_orders(client, &orders);
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
            ids = ids + &"order_id=".to_string() + &o.id.to_string() + &",".to_string();
            // TODO: async bulk update 
            client.execute(
                "UPDATE taxi_order SET status=6 WHERE id=$1", &[&o.id]); //OrderStatus.REFUSED
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
