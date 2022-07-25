use model::{KernCfg};
use stats::Stat;
use pool::orders_to_array;
use postgres::{Client, NoTls, Error};
use chrono::{Local, Duration};
use std::collections::HashMap;
use std::time::{SystemTime,Instant};
use std::{thread};
use std::env;
use hungarian::minimize;
use serde::{Deserialize};
use log::{error, info, warn, LevelFilter, SetLoggerError};
use log4rs::{
    append::{
        console::{ConsoleAppender, Target},
        file::FileAppender,
    },
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
    filter::threshold::ThresholdFilter,
};

mod repo;
mod model;
mod distance;
mod extender;
mod pool;
mod stats;
use crate::model::{Order,OrderStatus,OrderTransfer,Stop,Cab,CabStatus,Branch,MAXSTOPSNUMB,MAXCABSNUMB,MAXORDERSNUMB,MAXBRANCHNUMB};
use crate::extender::{ findMatchingRoutes, writeSqlToFile, getHandle};
use crate::pool::{orders_to_transfer_array, cabs_to_array, stops_to_array, find_pool};
use crate::repo::{assignPoolToCab};
use crate::distance::{DIST};

// default config, overwritten by cfg file
pub static mut cnfg: KernCfg = KernCfg { 
    max_assign_time: 3, // min
    max_solver_size: 500, // count
    run_after: 15 // secs
};


const cfg_file_default: &str = "kern.toml";

fn main() -> Result<(), Error> {
    println!("cargo:rustc-link-lib=dynapool30");
    // reading Config
    let mut cfg_file: String = cfg_file_default.to_string();
    let args: Vec<String> = env::args().collect();
    if args.len() == 3 && args[1] == "-f" {
        cfg_file = args[2].to_string();
    }
    println!("Config file: {cfg_file}");
    let settings = config::Config::builder()
        .add_source(config::File::with_name(&cfg_file))
        // Add in settings from the environment (with a prefix of APP)
        // Eg.. `APP_DEBUG=1 ./target/app` would set the `debug` key
        //.add_source(config::Environment::with_prefix("APP"))
        .build()
        .unwrap();
    let cfg = settings
        .try_deserialize::<HashMap<String, String>>()
        .unwrap();
    let db_conn_str = &cfg["db_conn"];

    println!("Database: {db_conn_str}");
    unsafe {
        cnfg = KernCfg { 
            max_assign_time: cfg["max_assign_time"].parse().unwrap(),
            max_solver_size: cfg["max_solver_size"].parse().unwrap(),
            run_after:       cfg["run_after"].parse().unwrap(),
        };
    }
    setup_log(cfg["log_file"].clone());
    info!("Starting up");

    // init DB
    let mut client = Client::connect(&db_conn_str, NoTls)?; // 192.168.10.176
    let stops = repo::read_stops(&mut client);
    distance::init_distance(&stops);
    
    loop {
        let start = Instant::now();
        let tmpModel = prepare_data(&mut client);
        match tmpModel {
            Some(mut x) => { 
                dispatch(&db_conn_str, &mut client, &mut x.0, &mut x.1, &stops);
            },
            None => {
                println!("Nothing to do");
            }
        }
        unsafe {
        let wait: u64 = cnfg.run_after - start.elapsed().as_secs();
        if wait > 0 {
            thread::sleep(std::time::Duration::from_secs(wait));
        }}
    }
    Ok(())
}

fn setup_log(file_path: String) {
    let level = log::LevelFilter::Info;
    // Build a stderr logger.
    let stderr = ConsoleAppender::builder().target(Target::Stderr).build();
    // Logging to log file.
    let logfile = FileAppender::builder()
        // Pattern: https://docs.rs/log4rs/*/log4rs/encode/pattern/index.html
        .encoder(Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S)} {l} - {m}\n")))
        .build(file_path)
        .unwrap();

    // Log Trace level output to file where trace is the default level
    // and the programmatically specified level to stderr.
    let config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .appender(
            Appender::builder()
                .filter(Box::new(ThresholdFilter::new(level)))
                .build("stderr", Box::new(stderr)),
        )
        .build(
            Root::builder()
                .appender("logfile")
                .appender("stderr")
                .build(LevelFilter::Trace),
        )
        .unwrap();

    // Use this to change log levels at runtime.
    // This means you can change the default log level to trace
    // if you are trying to debug an issue and need more logs on then turn it off
    // once you are done.
    let _handle = log4rs::init_config(config);
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

fn dispatch(host: &String, client: &mut Client, orders: &mut Vec<Order>, mut cabs: &mut Vec<Cab>, stops: &Vec<Stop>) {
    let use_ext_pool: bool = true;
    let thread_numb: i32 = 4;
    let mut max_route_id : i64 = repo::read_max(client, "route");
    let mut max_leg_id : i64 = repo::read_max(client, "leg");
    let lenBefore = orders.len();
    // ROUTE EXTENDER
    let ret = findMatchingRoutes(&host, client, orders, &stops, &mut max_leg_id);
    let mut demand = ret.0;
    let lenAfter = demand.len();
    let extenderHandle: thread::JoinHandle<()> = ret.1;
    if lenBefore != lenAfter {
      println!("Route extender found allocated {} requests", lenBefore - lenAfter);
    }
    let mut pl: Vec<Branch> = Vec::new();
    let mut sql: String = String::from("");
    if use_ext_pool {
        (pl, sql) = find_extern_pool(&mut demand, cabs, stops, thread_numb, &mut max_route_id, &mut max_leg_id);
    } else {
        for p in (2..5).rev() { // 4,3,2
            let mut ret = find_pool(p, thread_numb as i16,
                    &mut demand, &mut cabs, &stops, &mut max_route_id, &mut max_leg_id);
            pl.append(&mut ret.0);
            sql += &ret.1;
        }
    }
    writeSqlToFile(&sql, "pool");
    let pool_handle = getHandle(host.clone(), sql, "pool".to_string());

    // marking assigned orders to get rid of them; cabs are marked in find_pool 
    let numb = countOrders(pl, &demand);
    println!("Number of assigned orders: {}", numb);
    // shrinking vectors, getting rid of .id == -1 and (TODO) distant orders and cabs !!!!!!!!!!!!!!!
    (*cabs, demand) = shrink(&cabs, demand);
    // LCM
    let mut lcm_handle = thread::spawn(|| { });
    unsafe {
    if demand.len() > cnfg.max_solver_size && cabs.len() > cnfg.max_solver_size {
        // too big to send to solver, it has to be cut by LCM
        // first just kill the default thread
        lcm_handle.join().expect("LCM SQL thread being joined has panicked");
        lcm_handle = Lcm(host, &cabs, &demand, &mut max_route_id, &mut max_leg_id, 
                std::cmp::min(demand.len(), cabs.len()) as i16 - cnfg.max_solver_size as i16);
        println!("After LCM: demand={}, supply={}", demand.len(), cabs.len());
    }}
    // SOLVER
    let sol = munkres(&cabs, &demand);
    sql = repo::assignCustToCabMunkres(sol, &cabs, &demand, &mut max_route_id, &mut max_leg_id);

    writeSqlToFile(&sql, "munkres");
    if sql.len() > 0 {
      client.batch_execute(&sql); // here SYNC execution
    }
    // we have to join so that the next run of dispatcher gets updated orders
    extenderHandle.join().expect("Extender SQL thread being joined has panicked");
    pool_handle.join().expect("Pool SQL thread being joined has panicked");
    lcm_handle.join().expect("LCM SQL thread being joined has panicked");
}

fn Lcm(host: &String, cabs: &Vec<Cab>, orders: &Vec<Order>, max_route_id: &mut i64, max_leg_id: &mut i64, howMany: i16) 
                                -> thread::JoinHandle<()> {
    let BIG_COST: i32 = 1000000;
    if howMany < 1 { // we would like to find at least one
        println!("LCM asked to do nothing");
        return thread::spawn(|| { });
    }
    let mut cabs_cpy = cabs.to_vec();
    let mut orders_cpy = orders.to_vec();
    let mut lcmMinVal = BIG_COST;
    let mut pairs: Vec<(i16,i16)> = vec![];
    for i in 0..howMany { // we need to repeat the search (cut off rows/columns) 'howMany' times
        lcmMinVal = BIG_COST;
        let mut smin: i16 = -1;
        let mut dmin: i16 = -1;
        // now find the minimal element in the whole matrix
        unsafe {
        for (s, cab) in cabs_cpy.iter().enumerate() {
            if cab.id == -1 {
                continue;
            }
            for (d, order) in orders_cpy.iter().enumerate() {
                if order.id != -1 && (distance::DIST[cab.location as usize][order.from as usize] as i32) < lcmMinVal {
                    lcmMinVal = distance::DIST[cab.location as usize][order.from as usize] as i32;
                    smin = s as i16;
                    dmin = d as i16;
                }
            }
        }}
        if lcmMinVal == BIG_COST {
            println!("LCM minimal cost is BIG_COST - no more interesting stuff here");
            break;
        }
        // binding cab to the customer order
        pairs.push((smin, dmin));
        // removing the "columns" and "rows" from a virtual matrix
        cabs_cpy[smin as usize].id = -1;
        orders_cpy[dmin as usize].id = -1;
    }
    let sql = repo::assignCustToCabLCM(pairs, &cabs, &orders, max_route_id, max_leg_id);
    return getHandle(host.clone(), sql, "LCM".to_string());
}

fn shrink (cabs: &Vec<Cab>, orders: Vec<Order>) -> (Vec<Cab>, Vec<Order>) {
    let mut newCabs: Vec<Cab> = vec![];
    let mut newOrders: Vec<Order> = vec![];
    // v.iter().filter(|x| x % 2 == 0).collect() ??
    for c in cabs.iter() { 
        if c.id != -1 { newCabs.push(*c); }
    }
    for o in orders.iter() { 
        if o.id != -1 { newOrders.push(*o); }
    }
    return (newCabs, newOrders);
}

fn countOrders(pl: Vec<Branch>, orders: &Vec<Order>) -> i32 {
    let mut countInBranches = 0;
    let mut countInOrders = 0;
    for b in pl.iter() {
        for o in 0..b.ordNumb as usize {
            if b.ordActions[o] == 'i' as i8 { // do not count twice
                if orders[b.ordIDs[o] as usize].id == -1 {
                    countInOrders += 1;
                }
                countInBranches += 1;
            }
        }
    }
    if countInBranches != countInOrders {
        panic!("Error! Number of orders marked as assigned ({}) does not equal orders in branches: {}",
            countInOrders, countInBranches);
    }
    return countInBranches;
}

fn find_extern_pool(demand: &mut Vec<Order>, cabs: &mut Vec<Cab>, stops: &Vec<Stop>, threads: i32,
                    max_route_id: &mut i64, max_leg_id: &mut i64) -> (Vec<Branch>, String) {
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
        sql += &assignPoolToCab(cabs[br[i].cab as usize], &orders_to_array(&demand), br[i], max_route_id, max_leg_id);
        // remove the cab from list so that it cannot be allocated twice, by LCM or Munkres
        cabs[br[i].cab as usize].id = -1;
        // mark orders as assigned too
        for o in 0..br[i].ordNumb as usize {
            demand[br[i].ordIDs[o] as usize].id = -1;
        }
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
        unsafe {
        if (minutesAt == -1 && minutesRcvd > cnfg.max_assign_time)
                    || (minutesAt != -1 && minutesAt > cnfg.max_assign_time) {
            ids = ids + &o.id.to_string() + &",".to_string();
        } else {
            ret.push(*o);
        }}
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

// returns indexes of orders assigned to cabs - vec[1]==5 would mean 2nd cab assigned 6th order
fn munkres(cabs: &Vec<Cab>, orders: &Vec<Order>) -> Vec<i16> {
    let mut ret: Vec<i16> = vec![];
    let mut matrix: Vec<i32> = vec![];
    for c in cabs.iter() {
        for o in orders.iter() {
            unsafe {
                matrix.push(distance::DIST[c.location as usize][o.from as usize] as i32);
            }
        }
    }
    let assignment = minimize(&matrix, cabs.len() as usize, orders.len() as usize);
    
    for s in assignment {
        if s.is_some() {
            ret.push(s.unwrap() as i16);
        } else {
            ret.push(-1);
        }
    }
    return ret;
}
