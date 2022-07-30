mod repo;
mod model;
mod distance;
mod extender;
mod pool;
mod stats;
mod utils;
use distance::DIST;
use model::{KernCfg,Order,OrderStatus,OrderTransfer,Stop,Cab,CabStatus,Branch,MAXSTOPSNUMB,MAXCABSNUMB,MAXORDERSNUMB,MAXBRANCHNUMB};
use stats::{Stat,update_max_and_avg_time,update_max_and_avg_stats,incr_val};
use pool::{orders_to_array,orders_to_transfer_array, cabs_to_array, stops_to_array, find_pool};
use repo::{CNFG, assign_pool_to_cab};
use extender::{find_matching_routes, write_sql_to_file, get_handle};
use utils::get_elapsed;
use postgres::{Client, NoTls, Error};
use chrono::{Local, Duration};
use std::collections::HashMap;
use std::time::Instant;
use std::thread;
use std::env;
use hungarian::minimize;
use log::{info,warn,debug,error,LevelFilter};
use log4rs::{
    append::{
        console::{ConsoleAppender, Target},
        file::FileAppender,
    },
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
    filter::threshold::ThresholdFilter,
};

const CFG_FILE_DEFAULT: &str = "kern.toml";

fn main() -> Result<(), Error> {
    println!("cargo:rustc-link-lib=dynapool39");
    // reading Config
    let mut cfg_file: String = CFG_FILE_DEFAULT.to_string();
    let args: Vec<String> = env::args().collect();
    if args.len() == 3 && args[1] == "-f" {
        cfg_file = args[2].to_string();
    }
    info!("Config file: {cfg_file}");
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

    info!("Database: {db_conn_str}");
    unsafe {
        CNFG = KernCfg { 
            max_assign_time: cfg["max_assign_time"].parse().unwrap(),
            max_solver_size: cfg["max_solver_size"].parse().unwrap(),
            run_after:       cfg["run_after"].parse().unwrap(),
            max_legs:        cfg["max_legs"].parse().unwrap(),
            extend_margin:   cfg["extend_margin"].parse::<f32>().unwrap(),
            max_angle:       cfg["max_angle"].parse::<f32>().unwrap(),
            use_ext_pool:    cfg["use_ext_pool"].parse::<bool>().unwrap(),
            thread_numb:     cfg["thread_numb"].parse().unwrap(),
        };
    }
    setup_logger(cfg["log_file"].clone());
    unsafe { info!("Starting up with config:"); 
        info!("max_assign_time: {}", CNFG.max_assign_time);
        info!("max_solver_size: {}", CNFG.max_solver_size);
        info!("run_after: {}", CNFG.run_after);
        info!("max_legs: {}", CNFG.max_legs);
        info!("extend_margin: {}", CNFG.extend_margin);
        info!("max_angle: {}", CNFG.max_angle);
        info!("use_ext_pool: {}", CNFG.use_ext_pool);
        info!("thread_numb: {}", CNFG.thread_numb);
    }
    // init DB
    let mut client = Client::connect(&db_conn_str, NoTls)?; // 192.168.10.176
    let stops = repo::read_stops(&mut client);
    distance::init_distance(&stops);
    let mut itr: i32 = 0;
    loop {
        let start = Instant::now();
        let tmp_model = prepare_data(&mut client);
        match tmp_model {
            Some(mut x) => { 
                dispatch(itr, &db_conn_str, &mut client, &mut x.0, &mut x.1, &stops);
            },
            None => {
                info!("Nothing to do");
            }
        }
        update_max_and_avg_time(Stat::AvgShedulerTime, Stat::MaxShedulerTime, start);
        unsafe {
        let mut wait: u64 = CNFG.run_after - start.elapsed().as_secs();
        debug!("Sleeping in {} secs", wait);
        if wait > 60 {
            // TODO: find the bug!
            warn!("Strange wait time: {}", wait);
            wait = 0;
        }
        if wait > 0 {
            thread::sleep(std::time::Duration::from_secs(wait));
        }}
        itr += 1;
    }
}

fn setup_logger(file_path: String) {
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

#[link(name = "dynapool39")]
extern "C" {
    fn dynapool(
		numbThreads: i32,
        pool4size: i32, pool3size: i32, pool2size: i32,
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
		count: &mut i32, // returned count of values
        pool4time: &mut i32,
        pool3time: &mut i32,
        pool2time: &mut i32
    );
}

fn dispatch(itr: i32, host: &String, client: &mut Client, orders: &mut Vec<Order>, mut cabs: &mut Vec<Cab>, stops: &Vec<Stop>) {
    if orders.len() == 0 {
        info!("No demand, no dispatch");
        return;
    }
    let mut max_route_id : i64 = repo::read_max(client, "route");
    let mut max_leg_id : i64 = repo::read_max(client, "leg");
    let len_before = orders.len();
    // ROUTE EXTENDER
    stats::update_max_and_avg_stats(Stat::AvgDemandSize, Stat::MaxDemandSize, len_before as i64);
    let start_extender = Instant::now();
    let mut thread_num: i32 = 0;
    unsafe {
        thread_num = CNFG.thread_numb;
    }
    let ret = 
            find_matching_routes(thread_num, itr, &host, client, orders, &stops, &mut max_leg_id);
    update_max_and_avg_time(Stat::AvgExtenderTime, Stat::MaxExtenderTime, start_extender);
    let mut demand = ret.0;
    let len_after = demand.len();
    let extender_handle: thread::JoinHandle<()> = ret.1;
    if len_before != len_after {
        info!("Route extender allocated {} requests", len_before - len_after);
    } else {
        info!("Extender has not helped");
    }
    
    if cabs.len() == 0 {
        info!("No cabs");
        extender_handle.join().expect("Extender SQL thread being joined has panicked");
        return;
    }
    let start_pool = Instant::now();
    stats::update_max_and_avg_stats(Stat::AvgPoolDemandSize, Stat::MaxPoolDemandSize, len_after as i64);
    let mut pl: Vec<Branch> = Vec::new();
    let mut sql: String = String::from("");
    unsafe {
    if CNFG.use_ext_pool {
        (pl, sql) = find_extern_pool(&mut demand, cabs, stops, CNFG.thread_numb, &mut max_route_id, &mut max_leg_id);
    } else {
        for p in (2..5).rev() { // 4,3,2
            let mut ret = find_pool(p, CNFG.thread_numb as i16,
                    &mut demand, &mut cabs, &stops, &mut max_route_id, &mut max_leg_id);
            pl.append(&mut ret.0);
            sql += &ret.1;
        }
    }}
    write_sql_to_file(itr, &sql, "pool");
    let pool_handle = get_handle(host.clone(), sql, "pool".to_string());
    update_max_and_avg_time(Stat::AvgPoolTime, Stat::MaxPoolTime, start_pool);

    // marking assigned orders to get rid of them; cabs are marked in find_pool 
    let numb = count_orders(pl, &demand);
    info!("Pool finder - number of assigned orders: {}", numb);
    // shrinking vectors, getting rid of .id == -1 and (TODO) distant orders and cabs !!!!!!!!!!!!!!!
    (*cabs, demand) = shrink(&cabs, demand);
    stats::update_max_and_avg_stats(Stat::AvgSolverDemandSize, Stat::MaxSolverDemandSize, demand.len() as i64);
    if cabs.len() == 0 {
        info!("No cabs after pool finder");
        pool_handle.join().expect("Pool SQL thread being joined has panicked");
        return;
    }
    if demand.len() == 0 {
        info!("No demand after pool finder");
        pool_handle.join().expect("Pool SQL thread being joined has panicked");
        return;
    }
    // LCM
    let mut lcm_handle = thread::spawn(|| { });
    unsafe {
    if demand.len() > CNFG.max_solver_size && cabs.len() > CNFG.max_solver_size {
        // too big to send to solver, it has to be cut by LCM
        // first just kill the default thread
        info!("LCM input: demand={}, supply={}", demand.len(), cabs.len());
        let start_lcm = Instant::now();
        lcm_handle.join().expect("LCM SQL thread being joined has panicked");
        lcm_handle = lcm(host, &cabs, &demand, &mut max_route_id, &mut max_leg_id, 
                std::cmp::min(demand.len(), cabs.len()) as i16 - CNFG.max_solver_size as i16);
        update_max_and_avg_time(Stat::AvgLcmTime, Stat::MaxLcmTime, start_lcm);
        incr_val(Stat::TotalLcmUsed);
    }}
    // SOLVER
    let start_solver = Instant::now();
    info!("Solver input - demand={}, supply={}", demand.len(), cabs.len());
    let sol = munkres(&cabs, &demand);
    let before_solver = max_route_id;
    sql = repo::assign_cust_to_cab_munkres(sol, &cabs, &demand, &mut max_route_id, &mut max_leg_id);
    update_max_and_avg_time(Stat::AvgSolverTime, Stat::MaxSolverTime, start_solver);
    write_sql_to_file(itr, &sql, "munkres");
    if sql.len() > 0 {
        match client.batch_execute(&sql) { // here SYNC execution
            Ok(_) => {}
            Err(err) => {
                warn!("Solver SQL output failed to run {}, err: {}", sql, err);
            }
        }
    }
    // we have to join so that the next run of dispatcher gets updated orders
    let status_handle = get_handle(host.clone(), repo::save_status(), "stats".to_string());
    extender_handle.join().expect("Extender SQL thread being joined has panicked");
    pool_handle.join().expect("Pool SQL thread being joined has panicked");
    lcm_handle.join().expect("LCM SQL thread being joined has panicked");
    status_handle.join().expect("Status SQL thread being joined has panicked");
    info!("Dispatch completed, solver assigned: {}", max_route_id - before_solver);
}

fn lcm(host: &String, cabs: &Vec<Cab>, orders: &Vec<Order>, max_route_id: &mut i64, max_leg_id: &mut i64, how_many: i16) 
                                -> thread::JoinHandle<()> {
    let big_cost: i32 = 1000000;
    if how_many < 1 { // we would like to find at least one
        warn!("LCM asked to do nothing");
        return thread::spawn(|| { });
    }
    let mut cabs_cpy = cabs.to_vec();
    let mut orders_cpy = orders.to_vec();
    let mut lcm_min_val;
    let mut pairs: Vec<(i16,i16)> = vec![];
    for _ in 0..how_many { // we need to repeat the search (cut off rows/columns) 'howMany' times
        lcm_min_val = big_cost;
        let mut smin: i16 = -1;
        let mut dmin: i16 = -1;
        // now find the minimal element in the whole matrix
        unsafe {
        for (s, cab) in cabs_cpy.iter().enumerate() {
            if cab.id == -1 {
                continue;
            }
            for (d, order) in orders_cpy.iter().enumerate() {
                if order.id != -1 && (distance::DIST[cab.location as usize][order.from as usize] as i32) < lcm_min_val {
                    lcm_min_val = distance::DIST[cab.location as usize][order.from as usize] as i32;
                    smin = s as i16;
                    dmin = d as i16;
                }
            }
        }}
        if lcm_min_val == big_cost {
            info!("LCM minimal cost is big_cost - no more interesting stuff here");
            break;
        }
        // binding cab to the customer order
        pairs.push((smin, dmin));
        // removing the "columns" and "rows" from a virtual matrix
        cabs_cpy[smin as usize].id = -1;
        orders_cpy[dmin as usize].id = -1;
    }
    let sql = repo::assign_cust_to_cab_lcm(pairs, &cabs, &orders, max_route_id, max_leg_id);
    return get_handle(host.clone(), sql, "LCM".to_string());
}

fn shrink (cabs: &Vec<Cab>, orders: Vec<Order>) -> (Vec<Cab>, Vec<Order>) {
    let mut new_cabs: Vec<Cab> = vec![];
    let mut new_orders: Vec<Order> = vec![];
    // v.iter().filter(|x| x % 2 == 0).collect() ??
    for c in cabs.iter() { 
        if c.id != -1 { new_cabs.push(*c); }
    }
    for o in orders.iter() { 
        if o.id != -1 { new_orders.push(*o); }
    }
    return (new_cabs, new_orders);
}

fn count_orders(pl: Vec<Branch>, orders: &Vec<Order>) -> i32 {
    let mut count_in_branches = 0;
    let mut count_in_orders = 0;
    for b in pl.iter() {
        for o in 0..b.ord_numb as usize {
            if b.ord_actions[o] == 'i' as i8 { // do not count twice
                if orders[b.ord_ids[o] as usize].id == -1 {
                    count_in_orders += 1;
                }
                count_in_branches += 1;
            }
        }
    }
    if count_in_branches != count_in_orders {
        panic!("Error! Number of orders marked as assigned ({}) does not equal orders in branches: {}",
            count_in_orders, count_in_branches);
    }
    return count_in_branches;
}

fn find_extern_pool(demand: &mut Vec<Order>, cabs: &mut Vec<Cab>, stops: &Vec<Stop>, threads: i32,
                    max_route_id: &mut i64, max_leg_id: &mut i64) -> (Vec<Branch>, String) {
    let mut ret: Vec<Branch> = Vec::new();  
    if demand.len() > MAXORDERSNUMB || cabs.len() > MAXCABSNUMB {
        error!("Demand or supply too big, accordingly {} and {}", demand.len(), cabs.len());
        return (ret, "".to_string());
    }
    let orders: [OrderTransfer; MAXORDERSNUMB] = orders_to_transfer_array(&demand);
    let mut br: [Branch; MAXBRANCHNUMB] = [Branch::new(); MAXBRANCHNUMB];
    let mut cnt: i32 = 0;
    let mut pool4time: i32 = 0;
    let mut pool3time: i32 = 0;
    let mut pool2time: i32 = 0;
    unsafe {
        dynapool(
            threads,
            200,
            500,
            800,
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
            &mut cnt, // returned count of values
            &mut pool4time,
            &mut pool3time,
            &mut pool2time
        );
    }
    update_max_and_avg_stats(Stat::AvgPool4Time, Stat::MaxPool4Time, pool4time as i64);
    update_max_and_avg_stats(Stat::AvgPool3Time, Stat::MaxPool3Time, pool3time as i64);
  /*  for i in 0 .. cnt as usize {
        let mut str: String = String::from("");
        str += &format!("{}: cost={}, outs={}, ordNumb={}, cab={},(", i, br[i].cost, br[i].outs, br[i].ord_numb, br[i].cab);
        for j in 0.. br[i].ord_numb {
            str += &format!("{}{},", br[i].ord_ids[j as usize], br[i].ord_actions[j as usize]);
        }
        str += &format!(")\n");
        info!("{}", str);
    }
*/
    let mut sql: String = String::from("");
    for i in 0 .. cnt as usize {
        if br[i].cab == -1 || br[i].cab >= cabs.len() as i32 {
            error!("Wrong cab index: {}, array len: {}, array index: {}", br[i].cab, cnt, i);
            continue;
        }
        ret.push(br[i]); // just convert to vec
        sql += &assign_pool_to_cab(cabs[br[i].cab as usize], &orders_to_array(&demand), br[i], max_route_id, max_leg_id);
        // remove the cab from list so that it cannot be allocated twice, by LCM or Munkres
        cabs[br[i].cab as usize].id = -1;
        // mark orders as assigned too
        for o in 0..br[i].ord_numb as usize {
            demand[br[i].ord_ids[o] as usize].id = -1;
        }
    }
    //  RUN SQL
    return (ret, sql);
  }

fn prepare_data(client: &mut Client) -> Option<(Vec<Order>, Vec<Cab>)> {
    let mut orders = repo::find_orders_by_status_and_time(
                client, OrderStatus::RECEIVED , Local::now() - Duration::minutes(5));
    if orders.len() == 0 {
        info!("No demand");
        return None;
    }
    info!("Orders, input: {}", orders.len());
    
    //orders = expire_orders(client, &orders);
    if orders.len() == 0 {
        info!("No demand, expired");
        return None;
    }
    let mut cabs = repo::find_cab_by_status(client, CabStatus::FREE);
    if orders.len() == 0 || cabs.len() == 0 {
        warn!("No cabs available");
        return None;
    }
    info!("Initial count, demand={}, supply={}", orders.len(), cabs.len());
    orders = get_rid_of_distant_customers(&orders, &cabs);
    if orders.len() == 0 {
      info!("No suitable demand, too distant");
      return None; 
    }
    cabs = get_rid_of_distant_cabs(&orders, &cabs);
    if cabs.len() == 0 {
      info!("No cabs available, too distant");
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
        let minutes_rcvd = get_elapsed(o.received)/60;
        let minutes_at : i64 = get_elapsed(o.at_time)/60;
        unsafe {
        if (minutes_at == -1 && minutes_rcvd > CNFG.max_assign_time)
                    || (minutes_at != -1 && minutes_at > CNFG.max_assign_time) {
            ids = ids + &o.id.to_string() + &",".to_string();
        } else {
            ret.push(*o);
        }}
    }
    if ids.len() > 0 {
        let sql = ids[0..ids.len() - 1].to_string(); // remove last comma
        match client.execute(
            "UPDATE taxi_order SET status=6 WHERE id IN ($1);\n", &[&sql]) { //OrderStatus.REFUSED
            Ok(_) => {}
            Err(err) => {
                warn!("Expire orders failed for {}, err: {}", sql, err);
            }
        }
        debug!("{} refused, max assignment time exceeded", &ids);
    }
    return ret;
}

fn get_rid_of_distant_customers(demand: &Vec<Order>, supply: &Vec<Cab>) -> Vec<Order> {
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

fn get_rid_of_distant_cabs(demand: &Vec<Order>, supply: &Vec<Cab>) -> Vec<Cab> {
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
