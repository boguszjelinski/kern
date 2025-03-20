/// Kabina minibus/taxi dispatcher
/// Copyright (c) 2022 by Bogusz Jelinski bogusz.jelinski@gmail.com

mod repo;
mod model;
mod distance;
mod extender;
mod pool;
mod stats;
mod utils;
use distance::DIST;
use model::{KernCfg, Order, OrderStatus, OrderTransfer, Stop, Cab, CabStatus, Branch,
            MAXSTOPSNUMB, MAXCABSNUMB, MAXORDERSNUMB, MAXBRANCHNUMB, MAXINPOOL};
use stats::{Stat,update_max_and_avg_time,update_max_and_avg_stats,incr_val};
use pool::{orders_to_transfer_array, cabs_to_array, stops_to_array, find_pool};
use repo::{assign_pool_to_cab, assign_requests_for_free_cabs, run_sql};
use extender::{find_matching_routes, get_handle}; // write_sql_to_file
use utils::get_elapsed;
use mysql::*;
use mysql::prelude::*;
use chrono::{Local, Duration};
use std::collections::HashMap;
use std::ptr::addr_of;
use std::time::Instant;
use std::thread;
use std::env;
use std::mem;
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

const MAXLCM : usize = 20000; // !! max number of cabs or orders sent to LCM in C
const CFG_FILE_DEFAULT: &str = "kern.toml";

fn main() -> std::result::Result<(), Box<dyn std::error::Error>>  {
    // cargo rustc --release -- -L /Users/bogusz.jelinski/Rust/kern/pool
    // RUSTFLAGS='-L /Users/bogusz.jelinski/Rust/kern/pool' cargo build --release
    //println!("cargo:rustc-link-search=/Users/bogusz.jelinski/Rust/kern/pool");
    //println!("cargo:rustc-link-lib=static=dynapool");
    //println!("cargo:rustc-link-arg=--max-memory=20294967296");
    // reading Config
    let mut cfg_file: String = CFG_FILE_DEFAULT.to_string();

    // command line arguments
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
    setupcfg(&cfg);
    let cfig = KernCfg::access();

    // init DB
    // 192.168.10.176
    let url: &str = &db_conn_str;
    let pool = Pool::new(url)?;
    let mut conn = pool.get_conn()?;

    let stops = repo::read_stops(&mut conn);
    distance::init_distance(&stops, cfig.cab_speed);
    
    unsafe {
        if cfig.use_extern_pool {
            initMem();
        }
    }

    // Kern main, infinite loop
    loop {
        let start = Instant::now();
        // get newly requested trips and free cabs, reject expired orders (no luck this time)
        let tmp_model = prepare_data(&mut conn, cfig.max_assign_time);

        match tmp_model {
            Some(mut x) => { 
                dispatch(&db_conn_str, &mut conn, &mut x.0, &mut x.1, &stops, *cfig);
            },
            None => {
                info!("Nothing to do");
            }
        }
        update_max_and_avg_time(Stat::AvgShedulerTime, Stat::MaxShedulerTime, start);

        // check if we should wait for new orders
        let mut wait: u64 = cfig.run_after - start.elapsed().as_secs();
        if wait > 60 {
            // TODO: find the bug!
            warn!("Strange wait time: {}", wait);
            wait = 0;
        }
        if wait > 0 {
            debug!("Sleeping in {} secs", wait);
            thread::sleep(std::time::Duration::from_secs(wait));
        }
    }
}

fn setupcfg(cfg: & HashMap<String, String>) {
    let mut c: KernCfg = KernCfg::new();
    c.max_assign_time = cfg["max_assign_time"].parse().unwrap();
    c.max_solver_size = cfg["max_solver_size"].parse().unwrap();
    c.run_after      = cfg["run_after"].parse().unwrap();
    c.max_legs       = cfg["max_legs"].parse().unwrap();
    c.max_angle      = cfg["max_angle"].parse().unwrap();
    c.max_angle_dist = cfg["max_angle_dist"].parse().unwrap();
    c.use_pool       = cfg["use_pool"].parse::<bool>().unwrap();
    c.use_extern_pool= cfg["use_extern_pool"].parse::<bool>().unwrap();
    c.use_extender   = cfg["use_extender"].parse::<bool>().unwrap();
    c.thread_numb    = cfg["thread_numb"].parse().unwrap();
    c.stop_wait      = cfg["stop_wait"].parse().unwrap();
    c.cab_speed      = cfg["cab_speed"].parse().unwrap();
    c.max_pool5_size = cfg["max_pool5_size"].parse().unwrap();
    c.max_pool4_size = cfg["max_pool4_size"].parse().unwrap();
    c.max_pool3_size = cfg["max_pool3_size"].parse().unwrap();
    c.max_pool2_size = cfg["max_pool2_size"].parse().unwrap();
    c.max_pool2_size = cfg["max_pool2_size"].parse().unwrap();
    c.solver_delay =cfg["solver_delay"].parse().unwrap();

    KernCfg::put(c);

    setup_logger(cfg["log_file"].clone());
    info!("Starting up with config:"); 
    info!("max_assign_time: {}", c.max_assign_time);
    info!("max_solver_size: {}", c.max_solver_size);
    info!("run_after: {}", c.run_after);
    info!("max_legs: {}", c.max_legs);
    info!("max_angle: {}", c.max_angle);
    info!("max_angle_dist: {}", c.max_angle_dist);
    info!("use_pool: {}", c.use_pool);
    info!("use_extern_pool: {}", c.use_extern_pool);
    info!("use_extender: {}", c.use_extender);
    info!("thread_numb: {}", c.thread_numb);
    info!("stop_wait: {}", c.stop_wait);
    info!("cab_speed: {}", c.cab_speed);
    info!("pool5_size: {}", c.max_pool5_size);
    info!("pool4_size: {}", c.max_pool4_size);
    info!("pool3_size: {}", c.max_pool3_size);
    info!("pool2_size: {}", c.max_pool2_size);
    info!("solver_delay: {}", c.solver_delay);
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
                .build(LevelFilter::Debug),
        )
        .unwrap();

    // Use this to change log levels at runtime.
    // This means you can change the default log level to trace
    // if you are trying to debug an issue and need more logs on then turn it off
    // once you are done.
    let _handle = log4rs::init_config(config);
}

#[link(name = "dynapool")]
unsafe extern "C" {
    unsafe fn dynapool(
		numbThreads: i32,
        poolsize: &[i32; MAXINPOOL - 1], // max sizes
		distance: *const [[i16; 5200]; 5200], 
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
        pooltime: &mut [i32; MAXINPOOL - 1] // performance statistics
    );

    unsafe fn c_lcm(
        distance: *const [[i16; 5200]; 5200],
        distSize: i32,
        orders: &[OrderTransfer; MAXORDERSNUMB],
        ordersSize: i32,
        cabs: &[Cab; MAXCABSNUMB],
        cabsSize: i32,
        how_many: i32,
        supply: &mut [i16; MAXLCM], // returned values
        demand: &mut [i16; MAXLCM], // returned values
        count: &mut i32 // returned count of values
    );
    
    unsafe fn initMem();
}

fn run_extender(conn: &mut PooledConn, orders: &Vec<Order>, stops: &Vec<Stop>, 
                max_leg_id: &mut i64, label: &str, cfg: &KernCfg) -> Vec<Order> {
    let len_before = orders.len();
    if cfg.use_extender {
        let start_extender = Instant::now();
        let demand = find_matching_routes(conn, orders, &stops, max_leg_id, cfg);
        update_max_and_avg_time(Stat::AvgExtenderTime, Stat::MaxExtenderTime, start_extender);
        let len_after = demand.len();
        if len_before != len_after {
            info!("{}: route extender allocated {} requests, max_leg_id: {}", label, len_before - len_after, max_leg_id);
        } else {
            info!("{}: extender has not helped", label);
        }
        return demand;
    } else {
        return orders.to_vec();
    }
}

// three steps:
// 1) route extender
// 2) pool finder
// 3) solver (LCM in most scenarious won't be called)
// SQL updates execute in background as async
fn dispatch(host: &String, conn: &mut PooledConn, orders: &mut Vec<Order>, mut cabs: &mut Vec<Cab>, stops: &Vec<Stop>, cfg: KernCfg) -> usize {
    let mut max_route_id : i64 = repo::read_max(conn, "route"); // +1, first free ID
    let mut max_leg_id : i64 = repo::read_max(conn, "leg");

    if orders.len() == 0 {
        info!("No demand, no dispatch");
        // but check orders from free cabs
        assign_requests_for_free_cabs(conn, &mut max_route_id, &mut max_leg_id);
        return 0;
    }
    stats::update_max_and_avg_stats(Stat::AvgDemandSize, Stat::MaxDemandSize, orders.len() as i64);
    
    // check if we want to run extender is done in run_extender
    let mut demand
        = run_extender(conn, orders, &stops, &mut max_leg_id, "FIRST", &cfg);
    if cabs.len() == 0 {
        info!("No cabs");
        return 0;
    }
    // POOL FINDER
    if cfg.use_pool && orders.len() > 0 {
        let start_pool = Instant::now();
        stats::update_max_and_avg_stats(Stat::AvgPoolDemandSize, Stat::MaxPoolDemandSize, demand.len() as i64);
        let pl: Vec<Branch>;
        let sql: String;
        // 2 versions available - in C (external) and Rust
        if cfg.use_extern_pool {
            (pl, sql) = find_external_pool(&mut demand, cabs, stops, cfg.thread_numb, &mut max_route_id, &mut max_leg_id, cfg);
        } else {
            (pl, sql) = find_internal_pool(&mut demand, cabs, stops, &mut max_route_id, &mut max_leg_id, cfg);
        }
        update_max_and_avg_time(Stat::AvgPoolTime, Stat::MaxPoolTime, start_pool);
        //write_sql_to_file(itr, &sql, "pool");
        //for s in split_sql(sql, 150) {
        //    client.batch_execute(&s).unwrap();
        //}
        run_sql(conn, sql);
        // marking assigned orders to get rid of them; cabs are marked in find_pool 
        let numb = count_orders(pl, &demand);
        info!("Pool finder - number of assigned orders: {}", numb);

        // let's try extender on the new routes if there still is demand
        (*cabs, demand) = shrink(&cabs, demand);
        demand
            = run_extender(conn, &demand, &stops, &mut max_leg_id, "SECOND", &cfg);
    }

    // we don't want to run run solver each time, once a minute is fine, these are som trouble-making customers :)

    demand = get_old_orders(&demand, cfg.solver_delay);

    if demand.len() > 0 {
        // shrinking vectors, getting rid of .id == -1 and (TODO) distant orders and cabs !!!!!!!!!!!!!!!
        (*cabs, demand) = shrink(&cabs, demand);
        stats::update_max_and_avg_stats(Stat::AvgSolverDemandSize, Stat::MaxSolverDemandSize, demand.len() as i64);
        if cabs.len() == 0 {
            info!("No cabs after pool finder");
            return 0;
        }
        if demand.len() == 0 {
            info!("No demand after pool finder");
            return 0;
        }
        // LCM presolver
        let mut lcm_handle = thread::spawn(|| { });
        if demand.len() > cfg.max_solver_size && cabs.len() > cfg.max_solver_size {
            // too big to send to solver, it has to be cut by LCM
            // first just kill the default thread
            info!("LCM input: demand={}, supply={}", demand.len(), cabs.len());
            let start_lcm = Instant::now();
            lcm_handle.join().expect("LCM SQL thread being joined has panicked");
            let cabs_len = cabs.len();
            let ord_len = orders.len();
            lcm_handle = lcm(host, &mut cabs, &mut demand, &mut max_route_id, &mut max_leg_id, 
                    std::cmp::min(ord_len, cabs_len) as i16 - cfg.max_solver_size as i16);
            update_max_and_avg_time(Stat::AvgLcmTime, Stat::MaxLcmTime, start_lcm);
            incr_val(Stat::TotalLcmUsed);
            (*cabs, demand) = shrink(&cabs, demand);
        }
        // SOLVER
        let start_solver = Instant::now();
        info!("Solver input - demand={}, supply={}", demand.len(), cabs.len());
        let sol = munkres(&cabs, &demand);
        let before_solver = max_route_id;

        let sql = repo::assign_cust_to_cab_munkres(sol, &cabs, &demand, &mut max_route_id, &mut max_leg_id);
        
        update_max_and_avg_time(Stat::AvgSolverTime, Stat::MaxSolverTime, start_solver);
        //write_sql_to_file(itr, &sql, "munkres");
        run_sql(conn, sql);
        lcm_handle.join().expect("LCM SQL thread being joined has panicked");
        info!("Dispatch completed, solver assigned: {}", max_route_id - before_solver);
    }
    // we have to join so that the next run of dispatcher gets updated orders
    let status_handle = get_handle(host.clone(), repo::save_status(), "stats".to_string());
    status_handle.join().expect("Status SQL thread being joined has panicked");

    assign_requests_for_free_cabs(conn, &mut max_route_id, &mut max_leg_id); // someone went into and took this cab

    return 0; // 0: all orders served
}

// least/low cost method - shrinking the model so that it can be sent to solver
fn lcm(host: &String, mut cabs: &mut Vec<Cab>, mut orders: &mut Vec<Order>, max_route_id: &mut i64, max_leg_id: &mut i64, how_many: i16) 
                                -> thread::JoinHandle<()> {
    if how_many < 1 { // we would like to find at least one
        warn!("LCM asked to do nothing");
        return thread::spawn(|| { });
    }
    //let pairs: Vec<(i16,i16)> = lcm_gen_pairs2(cabs, orders, how_many);
    let pairs: Vec<(i16,i16)> = extern_lcm(cabs, orders, how_many);
    let sql = repo::assign_order_to_cab_lcm(pairs, &mut cabs, &mut orders, max_route_id, max_leg_id);
    return get_handle(host.clone(), sql, "LCM".to_string());
}

fn extern_lcm(cabs: &Vec<Cab>, orders: &Vec<Order>, how_many: i16) -> Vec<(i16,i16)> {
    let cabs_cpy = cabs.to_vec(); // clone
    let orders_cpy = orders.to_vec();
    let mut supply: [i16; MAXLCM] = [0; MAXLCM];
    let mut demand: [i16; MAXLCM] = [0; MAXLCM];
    let mut count: i32 = 0;

    unsafe { c_lcm(
        addr_of!(DIST),
        MAXSTOPSNUMB as i32,
        &orders_to_transfer_array(&orders_cpy),
        orders_cpy.len() as i32,
        &cabs_to_array(&cabs_cpy),
        cabs_cpy.len() as i32,
        how_many as i32,
        &mut supply, // returned values
        &mut demand,
        &mut count
    );}
    
    let mut pairs: Vec<(i16,i16)> = vec![];
    for i in 0..count as usize {
        pairs.push((supply[i], demand[i]));
    }
    return pairs;
}

// remove orders and cabs allocated by the pool so that the vectors can be sent to solver
fn shrink(cabs: &Vec<Cab>, orders: Vec<Order>) -> (Vec<Cab>, Vec<Order>) {
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

fn get_old_orders(orders: &Vec<Order>, solver_delay: i32) -> Vec<Order> {
    let mut new_orders: Vec<Order> = vec![];
    for o in orders.iter() { 
        if get_elapsed(o.received) > solver_delay as i64 { 
            new_orders.push(*o); 
        }
    }
    return new_orders;
}

// count orders allocated by pool finder
// only for reporting
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

fn find_internal_pool(demand: &mut Vec<Order>, cabs: &mut Vec<Cab>, stops: &Vec<Stop>, 
                    max_route_id: &mut i64, max_leg_id: &mut i64, cfg: KernCfg) -> (Vec<Branch>, String) {
    let mut pl: Vec<Branch> = Vec::new();  
    let mut sql: String = String::from("");

    for p in (2..6).rev() { //5,4,3,2
        if (p == 5 && demand.len() < (cfg.max_pool5_size) as usize ) || // 5: TODO: check if it works!!
            (p == 4 && demand.len() < (cfg.max_pool4_size) as usize ) ||
            (p == 3 && demand.len() < (cfg.max_pool3_size) as usize ) ||
            (p == 2 && demand.len() < (cfg.max_pool2_size) as usize ) {
            let now = Instant::now();
            let mut ret = find_pool(p, cfg.thread_numb as i16,
                                                            demand,  cabs, &stops, max_route_id, max_leg_id,
                                                            cfg.max_angle, cfg.stop_wait);
            print!("Pool with {}, found pools: {}\n", p, ret.0.len());
            info!("Pool with {}, found pools: {}\n", p, ret.0.len());
            let el = now.elapsed().as_secs() as i64;
            match p {
                5 => update_max_and_avg_stats(Stat::AvgPool5Time, Stat::MaxPool5Time, el),
                4 => update_max_and_avg_stats(Stat::AvgPool4Time, Stat::MaxPool4Time, el),
                3 => update_max_and_avg_stats(Stat::AvgPool3Time, Stat::MaxPool3Time, el),
                _=>{},
            }
            /*for b in ret.0.iter() {
                for c in 0..b.ord_numb as usize {
                    print!("{}{:?},", b.ord_ids[c], char::from_u32(b.ord_actions[c] as u32).unwrap());
                }
                println!("");
            }*/
            pl.append(&mut ret.0);
            sql += &ret.1;
        }
    }
    return (pl, sql);
}

// if fail then dump input and output
fn validate_answer(br: &[Branch; MAXBRANCHNUMB], cnt: &i32, demand_size: usize, cabs: &Vec<Cab>) {
    let mut fail_found: bool = false;
    for i in 0 .. *cnt as usize {
        fail_found = false;
        if br[i].ord_numb > MAXINPOOL as i16 * 2 || br[i].ord_numb < 0 {
            warn!("ord_numb > {}", MAXINPOOL * 2);
            fail_found = true;
            break;
        }
        let mut in_count = 0;
        let mut out_count = 0;
        for j in 0 .. br[i].ord_numb as usize{
            if br[i].ord_ids[j] >= demand_size as i16 {
                warn!("ord_ids >= demandSize, id: {}, size: {}", br[i].ord_ids[j], demand_size);
                fail_found = true;
                break;
            }
            // last stop cannot be 'i'
            if (j == (br[i].ord_numb - 1) as usize) && br[i].ord_actions[j] == 'i' as i8 {
                warn!("last stop is 'i'");
                fail_found = true;
                break;
            }
            if br[i].ord_actions[j] == 'i' as i8 {
                in_count += 1;
                // every in has its out
                let mut found_out = false;
                for k in j + 1 .. br[i].ord_numb as usize {
                    if br[i].ord_actions[k] == 'o' as i8 
                        && br[i].ord_ids[k] == br[i].ord_ids[j] {
                            found_out = true;
                            break;
                        }
                }
                if !found_out {
                    warn!("OUT not found");
                    fail_found = true;
                    break;
                }
            }
            if br[i].ord_actions[j] == 'o' as i8 {
                out_count += 1;
            }
        }
        if fail_found { // go out of outer loop
            break;
        }
        if in_count != br[i].ord_numb / 2 {
            warn!("in_count: {}, ord_numb: {}", in_count, br[i].ord_numb);
            fail_found = true;
            break;
        }
        if out_count != br[i].ord_numb / 2 {
            warn!("out_count: {}, ord_numb: {}", out_count, br[i].ord_numb);
            fail_found = true;
            break;
        }
        // cab on the list
        if br[i].cab >= cabs.len() as i16 {
            warn!("Cab index {} higher than list.len {}", br[i].cab, cabs.len());
            fail_found = true;
            break;
        }
    }
    if fail_found { // dump input
        panic!("Pool corrupt");
    } 
}

// calling a C routine
fn find_external_pool(demand: &mut Vec<Order>, cabs: &mut Vec<Cab>, stops: &Vec<Stop>, threads: i32,
                      max_route_id: &mut i64, max_leg_id: &mut i64, cfg: KernCfg) -> (Vec<Branch>, String) {
    let mut ret: Vec<Branch> = Vec::new();  
    if demand.len() > MAXORDERSNUMB || cabs.len() > MAXCABSNUMB {
        error!("Demand or supply too big, accordingly {} and {}", demand.len(), cabs.len());
        return (ret, "".to_string());
    }
    let orders: [OrderTransfer; MAXORDERSNUMB] = orders_to_transfer_array(&demand);
    let mut br: [Branch; MAXBRANCHNUMB] = [Branch::new(); MAXBRANCHNUMB];
    let mut cnt: i32 = 0;
    let mut poolsize = [0; MAXINPOOL as usize - 1];
    let mut pooltime = [0; MAXINPOOL as usize - 1];
    info!("Size of Branch: {}", mem::size_of::<Branch>());
    unsafe {
        /*poolsize[0] = CNFG.max_pool5_size;
        poolsize[1] = CNFG.max_pool4_size;
        poolsize[2] = CNFG.max_pool3_size;
        poolsize[3] = CNFG.max_pool2_size;
        */
        poolsize[0] = cfg.max_pool4_size;
        poolsize[1] = cfg.max_pool3_size;
        poolsize[2] = cfg.max_pool2_size;

        dynapool(
            threads,
            &poolsize,
            addr_of!(DIST),
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
            &mut pooltime
        );
    }
    validate_answer(&br, &cnt, demand.len(), cabs);
    update_max_and_avg_stats(Stat::AvgPool5Time, Stat::MaxPool5Time, pooltime[0] as i64);
    update_max_and_avg_stats(Stat::AvgPool4Time, Stat::MaxPool4Time, pooltime[1] as i64);
    update_max_and_avg_stats(Stat::AvgPool3Time, Stat::MaxPool3Time, pooltime[2] as i64);

    /*
    info!("CNT: {}", cnt);
    info!("BR0: {}", br[0].ord_numb);
    info!("BR1: {}", br[1].ord_numb);
    for i in 0 .. cnt as usize {
        let mut str: String = String::from("");
        str += &format!("{}: cost={}, outs={}, ordNumb={}, cab={},(", i, br[i].cost, br[i].outs, br[i].ord_numb, br[i].cab);
        for j in 0.. br[i].ord_numb {
            str += &format!("{}:{},", br[i].ord_ids[j as usize], br[i].ord_actions[j as usize]);
        }
        str += &format!(")\n");
        info!("{}", str);
    }
        */

    // generate SQL
    let mut sql: String = String::from("");
    'outer: for i in 0 .. cnt as usize {
        // first two quality checks
        if br[i].cab == -1 || br[i].cab >= cabs.len() as i16 {
            error!("Wrong cab index: {}, array len: {}, array index: {}", br[i].cab, cnt, i);
            continue;
        }
        //print!("{}: ", br[i].ord_numb);
        for c in 0 .. br[i].ord_numb as usize {
            if br[i].ord_ids[c] < 0 || br[i].ord_ids[c] as usize > MAXORDERSNUMB {
                error!("Wrong order index: {}", br[i].ord_ids[c]);
                continue 'outer;
            }
            //print!("{}{:?},", br[i].ord_ids[c], char::from_u32(br[i].ord_actions[c] as u32).unwrap());
        }
        //println!("");
        /*unsafe {
        if !wait_constraints_met(&br[i], 
                            DIST[cabs[br[i].cab as usize].location as usize][demand[br[i].ord_ids[0] as usize].from as usize],
                            &demand
                        ) {
            continue;
        }
        }*/
        ret.push(br[i]); // just convert to vec
        sql += &assign_pool_to_cab(cabs[br[i].cab as usize], demand, br[i], max_route_id, max_leg_id, cfg.stop_wait);
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

// checking only maxWait
/*
fn wait_constraints_met(el: &Branch, dist_cab: i16, orders: &Vec<Order>) -> bool {
    // TASK: distances in pool should be stored to speed-up this check
    let mut dist = dist_cab;
    unsafe {
    for i in 0..el.ord_numb as usize -1 {
        let o: Order = orders[el.ord_ids[i] as usize];
        if el.ord_actions[i] == 'i' as i8 && dist > o.wait as i16 {
            println!("WAIT: i:{}, ord_numb={} real={} > requested={}, id={}, order_id={}", 
                i, el.ord_numb, dist, o.wait, el.ord_ids[i], orders[el.ord_ids[i] as usize].id);
            for k in 0..el.ord_numb as usize {
                print!("{}{},", el.ord_ids[k], el.ord_actions[k])
            }
            println!("");
            return false;
        }
        let o2: Order = orders[el.ord_ids[i+1] as usize];
        let from = if el.ord_actions[i] == ('i' as i8) { o.from as usize } else { o.to as usize };
        let to = if el.ord_actions[i + 1] == 'i' as i8 { o2.from as usize } else { o2.to as usize};
        if from != to { 
            dist += DIST[from][to] + CNFG.stop_wait;
        }
    }}
    return true;
}
*/

// 1) get unassigned orders and free cabs, 
// 2) expire old orders
// 3) some orders and cabs are too distant, although som cabs may end their last legs soon
// TODO: cabs on last leg should be considered
fn prepare_data(conn: &mut PooledConn, max_assign_time: i64) -> Option<(Vec<Order>, Vec<Cab>)> {
    let mut orders = repo::find_orders_by_status_and_time(
                conn, OrderStatus::RECEIVED , (Local::now() - Duration::minutes(5)).naive_local());
    if orders.len() == 0 {
        info!("No demand");
        return None;
    }
    info!("Orders before expiry check, input: {}", orders.len());
    
    orders = expire_orders(conn, &orders, max_assign_time);
    if orders.len() == 0 {
        info!("No demand, expired");
        return None;
    }
    let mut cabs = repo::find_cab_by_status(conn, CabStatus::FREE);
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
fn expire_orders(conn: &mut PooledConn, demand: &Vec<Order>, max_assign_time: i64) -> Vec<Order> {
    let mut ret: Vec<Order> = Vec::new();
    let mut ids: String = "".to_string();
    for o in demand.iter() {
      //if (o.getCustomer() == null) {
      //  continue; // TODO: how many such orders? the error comes from AddOrderAsync in API, update of Customer fails
      //}
        let minutes_rcvd = get_elapsed(o.received)/60;
        let minutes_at : i64 = get_elapsed(o.at_time)/60;
        
        if (minutes_at == -1 && minutes_rcvd > max_assign_time)
                    || (minutes_at != -1 && minutes_at > max_assign_time) {
            ids = ids + &o.id.to_string() + &",".to_string();
        } else {
            ret.push(*o);
        }
    }
    if ids.len() > 0 {
        let sql = ids[0..ids.len() - 1].to_string(); // remove last comma
        conn.query_iter("UPDATE taxi_order SET status=6 WHERE id IN (".to_string() + &sql + ")").unwrap();
        debug!("{} refused, max assignment time exceeded", &ids);
    }
    return ret;
}

// if we find just one cab nearby - continue with this order
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

// cargo test -- --test-threads=1
#[cfg(test)]
mod tests {
  use std::vec;
  use super::*;
  //use chrono::format::InternalNumeric;
  use serial_test::serial;
  use crate::distance::init_distance;
  use distance::DIST;

  fn test_orders_invalid() -> Vec<Order> {
    return vec![
        Order{ id: 1, from: 1, to: 2, wait: 10, loss: 50, dist: 2, 
            received: None,at_time: None, route_id: -1},
        Order{ id: -1, from: 1, to: 2, wait: 10, loss: 50, dist: 2, 
            received: None,at_time: None,route_id: -1}
    ];
  }

  fn test_orders() -> Vec<Order> {
    return vec![
        Order{ id: 0, from: 0, to: 1, wait: 10, loss: 50, dist: 2, 
            received: None,at_time: None, route_id: -1},
        Order{ id: 1, from: 1, to: 2, wait: 10, loss: 50, dist: 2, 
            received: None,at_time: None, route_id: -1}
    ];
  }

  fn test_cabs() -> Vec<Cab> {
    return vec![
        Cab{ id: 0, location: 2, seats: 10},
        Cab{ id: 1, location: 3, seats: 10}
    ];
  }

  fn test_cabs_invalid() -> Vec<Cab> {
    return vec![
        Cab{ id: 1, location: 0, seats: 10},
        Cab{ id: -1, location: 1, seats: 10}
    ];
  }

  fn test_stops() -> Vec<Stop> {
    return vec![
      Stop{ id: 0, bearing: 0, latitude: 1.0, longitude: 1.0},
      Stop{ id: 1, bearing: 0, latitude: 1.000000001, longitude: 1.000000001},
      Stop{ id: 2, bearing: 0, latitude: 1.000000002, longitude: 1.000000002},
      Stop{ id: 3, bearing: 0, latitude: 1.000000003, longitude: 1.000000003},
      Stop{ id: 4, bearing: 0, latitude: 1.000000004, longitude: 1.000000004},
      Stop{ id: 5, bearing: 0, latitude: 1.000000005, longitude: 1.000000005}
    ];
  }

  #[test]
  #[serial]
  fn test_shrink() {
    let orders: Vec<Order> = test_orders_invalid();
    let cabs: Vec<Cab> = test_cabs_invalid();
    assert_eq!(cabs.len(), 2);
    assert_eq!(orders.len(), 2);
    let ret = shrink(&cabs, orders);
    assert_eq!(ret.0.len(), 1);
    assert_eq!(ret.1.len(), 1);
  }

  #[test]
  #[serial]
  fn test_munkres() {
    let orders: Vec<Order> = test_orders_invalid();
    let cabs: Vec<Cab> = test_cabs_invalid();
    let ret = munkres(&cabs, &orders);
    assert_eq!(ret.len(), 2);
  }

  #[test]
  #[serial]
  fn test_get_rid_of_distant_cabs() {
    let orders: Vec<Order> = test_orders_invalid();
    let cabs: Vec<Cab> = test_cabs_invalid();
    let ret = get_rid_of_distant_cabs(&orders, &cabs);
    assert_eq!(ret.len(), 2); // not distant
  }

  #[test]
  #[serial]
  fn test_get_rid_of_distant_orders() {
    let orders: Vec<Order> = test_orders_invalid();
    let cabs: Vec<Cab> = test_cabs_invalid();
    let ret = get_rid_of_distant_customers(&orders, &cabs);
    assert_eq!(ret.len(), 2); // not distant
  }

  #[test]
  #[serial]
  fn test_find_extern_pool() {
    let mut orders: Vec<Order> = test_orders();
    let mut cabs: Vec<Cab> = test_cabs();
    let stops = test_stops();
    init_distance(&stops, 30);
    unsafe { initMem(); }
    let ret = find_external_pool(&mut orders, &mut cabs, &stops, 1_i32,
                                                         &mut 0, &mut 0, KernCfg::new());
    assert_eq!(ret.0.len(), 1); 
    /*assert_eq!(ret.1, 
        "UPDATE cab SET status=0 WHERE id=0;\n\
        INSERT INTO route (id, status, cab_id) VALUES (0,1,0);\n\
        INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (0,2,1,0,0,1,8,0,0);\n\
        INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (1,1,2,1,0,1,2,0,1);\n\
        UPDATE taxi_order SET route_id=0, leg_id=1, cab_id=0, status=1, eta=0, in_pool=true WHERE id=1 AND status=0;\n\
        INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (2,2,0,2,0,1,8,0,0);\n\
        INSERT INTO leg (id, from_stand, to_stand, place, distance, status, reserve, route_id, passengers) VALUES (3,0,1,3,0,1,2,0,1);\n\
        UPDATE taxi_order SET route_id=0, leg_id=3, cab_id=0, status=1, eta=0, in_pool=true WHERE id=0 AND status=0;\n"); 
        */
  }

  fn get_stops(step: f64, size: usize) -> Vec<Stop> {
    let mut stops: Vec<Stop> = vec![];
    let mut c: i64 = 0;
    for i in 0..size {
      for j in 0..size {
        stops.push(
          Stop{ id: c, bearing: 0, latitude: 49.0 + step * i as f64, longitude: 19.000 + step * j as f64}
        );
        c = c + 1;
      }
    }
    return stops;
  }

  fn get_orders(size: usize, stops: i32) -> Vec<Order> {
    let mut ret: Vec<Order> = vec![];
    for i in 0..size as i32 {     
        let from: i32 = i % stops;
        let to: i32 = if from + 5 >= stops { from - 5} else { from + 5} ;
        let dista = unsafe { DIST[from as usize][to as usize] as i32 };
        ret.push(Order{ id: i as i64, from, to, wait: 15, loss: 70, dist: dista, 
                    received: Some(Local::now().naive_local()), at_time: None, 
                    route_id: -1 });
    }
    return ret;
  }

  fn get_cabs(size: usize) -> Vec<Cab> {
    let mut ret: Vec<Cab> = vec![];
    for i in 0..size {
        ret.push(Cab{ id: i as i64, location: (i % 2400) as i32, seats: 10});
    }
    return ret;
  }

  #[test]
  #[serial]
  fn test_performance_find_extern_pool() {
    let stops = get_stops(0.03, 49);
    init_distance(&stops, 30);
    let mut orders: Vec<Order> = get_orders(100, 49);
    let mut cabs: Vec<Cab> = get_cabs(1000);
    unsafe { initMem(); }
    let start = Instant::now();
    let ret = find_external_pool(&mut orders, &mut cabs, &stops, 8_i32, 
                                                        &mut 0, &mut 0, KernCfg::new());
    let elapsed = start.elapsed();
    println!("Elapsed: {:?}", elapsed); 
    println!("Len: {}", ret.1.len()); 
    assert_eq!(ret.0.len(), 15); 
    assert_eq!(ret.1.len(), 18127); // TODO: Rust gives 21444
  }

  #[test]
  #[serial]
  fn test_performance_find_intern_pool() {
    let stops = get_stops(0.03, 49);
    init_distance(&stops, 30);
    let mut orders: Vec<Order> = get_orders(100, 49);
    let mut cabs: Vec<Cab> = get_cabs(1000);
    let start = Instant::now();
    let ret = find_internal_pool(&mut orders, &mut cabs, &stops, 
                                                    &mut 0, &mut 0, KernCfg::new());
    let elapsed = start.elapsed();
    println!("Elapsed: {:?}", elapsed); 
    assert_eq!(ret.0.len(), 15); 
    assert_eq!(ret.1.len(), 19112);
  }

  #[test]
  #[serial]
  fn test_performance_find_intern_pool_4in() {
    let mut max_route_id : i64 = 0;
    let mut max_leg_id : i64 = 0;
    let stops = get_stops(0.05, 49);
    init_distance(&stops, 30);
    let mut demand: Vec<Order> = get_orders(100, 49);
    let mut cabs = get_cabs(1000);
    unsafe { initMem(); }
    let start = Instant::now();
    let cfg = KernCfg::new();
    let ret = find_pool(4, 8, &mut demand,  &mut cabs, &stops, 
                                                &mut max_route_id, &mut max_leg_id, 
                                                cfg.max_angle, cfg.stop_wait);
    let elapsed = start.elapsed();
    println!("Elapsed: {:?}", elapsed); 
    assert_eq!(ret.0.len() > 0, false); 
  }

  #[test]  
  #[serial]
  fn test_performance_find_extern_pool5() {
    let stops = get_stops(0.01, 49);
    init_distance(&stops, 30);
    let mut orders: Vec<Order> = get_orders(10, 49);
    let mut cabs: Vec<Cab> = get_cabs(1000);
    unsafe { initMem(); }
    let start = Instant::now();
    let ret = find_external_pool(&mut orders, &mut cabs, &stops, 8_i32, 
                                                        &mut 0, &mut 0, KernCfg::new());
    let elapsed = start.elapsed();
    println!("Elapsed: {:?}", elapsed); 
    assert_eq!(ret.0.len(), 2); 
    assert_eq!(ret.1.len(), 3322);
  }

  #[test]  
  #[serial]
  fn test_performance_lcm() {
    let stops = get_stops(0.05, 49);
    init_distance(&stops, 30);
    let mut orders: Vec<Order> = get_orders(2000, 49);
    let mut cabs: Vec<Cab> = get_cabs(2000);
    //let ret = lcm_gen_pairs(&mut cabs, &mut orders, 100);
    let start = Instant::now();
    let ret2 = lcm_gen_pairs2(&mut cabs, &mut orders, 2000);
    println!("Elapsed: {}", start.elapsed().as_millis());
    assert_eq!(ret2.len(), 2000);
    // assert_eq!(ret2[0].0, 901);
    // assert_eq!(ret2[0].1, 1499);
    // assert_eq!(ret2[1].0, 902);
    // assert_eq!(ret2[1].1, 1498);
  } 

  
fn lcm_gen_pairs2(cabs: &Vec<Cab>, orders: &Vec<Order>, how_many: i16) -> Vec<(i16,i16)> {
    // let us start with a big cost - is there any smaller?
    let big_cost: i32 = 1000000;
    let mut cabs_cpy = cabs.to_vec(); // clone
    let mut orders_cpy = orders.to_vec();
    let mut lcm_min_val;
    let mut pairs: Vec<(i16,i16)> = vec![];
    for _ in 0..how_many { // we need to repeat the search (cut off rows/columns) 'howMany' times
        lcm_min_val = big_cost;
        let mut smin: i16 = -1;
        let mut dmin: i16 = -1;
        // now find the minimal element in the whole matrix
        unsafe {
        let mut s: usize = 0;
        let mut found = false;
        for cab in cabs_cpy.iter() {
            if cab.id == -1 {
                s += 1;
                continue;
            }
            let mut d: usize = 0;
            for order in orders_cpy.iter() {
                if order.id != -1 && (distance::DIST[cab.location as usize][order.from as usize] as i32) < lcm_min_val {
                    lcm_min_val = distance::DIST[cab.location as usize][order.from as usize] as i32;
                    smin = s as i16;
                    dmin = d as i16;
                    if lcm_min_val == 0 { // you can't have a better solution
                        found = true;
                        break;
                    }
                }
                d += 1;
            }
            if found {
                break; // yes, we could have loop labels and break two of them here, but this is for migration to C
            }
            s += 1;
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
    return pairs;
}


  #[test]
  fn a() {
    let mut cabs: Vec<i64> = vec![];
    cabs.push(1);
    cabs.push(2);
    for i in &mut cabs {
        if *i == 1 { *i = -1; }
    }
    print!("{:?}", cabs[0]);
  }
}
