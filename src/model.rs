use chrono::NaiveDateTime;
use std::sync::{Mutex, MutexGuard};

pub const MAXSTOPSNUMB : usize = 5200;
pub const MAXORDERSNUMB: usize = 20000; // max not assigned
pub const MAXCABSNUMB: usize = 39900;
pub const MAXBRANCHNUMB: usize = 5000; // size of pool finder's response

pub const MAXINPOOL : usize = 4;
pub const MAXORDID : usize = MAXINPOOL * 2;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Stop {
    pub id: i64,
    pub bearing: i32,
	pub latitude: f64,
    pub longitude: f64,
    pub capacity: i16
}

#[derive(Copy, Clone, Debug)]
pub struct Order {
    pub id: i64, // -1 as to-be-dropped
	pub from: i32,
    pub to: i32,
	pub wait: i32, // expected pick up time
	pub loss: i32, // allowed loss of time in detour
	pub dist: i32, // distance without pool
 //   pub shared: bool, // agreed to be in pool
 //   pub in_pool: bool, // actually in pool
    pub received: Option<NaiveDateTime>,
 //   pub started: Option<NaiveDateTime>,
 //   pub completed: Option<NaiveDateTime>,
    pub at_time: Option<NaiveDateTime>,
 //   pub eta: i32, // proposed wait time
    pub route_id: i64,
  //  cab: Cab,
  //  customer: Customer
}

// transfer object for external pool
#[repr(C)]
#[derive(Copy, Clone)]
pub struct OrderTransfer {
    pub id: i64, // -1 as to-be-dropped
	pub from: i32,
    pub to: i32,
	pub wait: i32,
	pub loss: i32,
	pub dist: i32
}

pub struct CabAssign {
    pub id: i64,
    pub cust_id: i64,
    pub cab_id: i64,
	pub from: i32,
    pub to: i32,
    pub loss: i32,
    pub shared: bool,
    pub received: Option<NaiveDateTime>
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Cab {
    pub id: i64,
	pub location: i32, // last known location, current location if FREE
    pub dist: i16, // for cabs on last leg of a route, this is the distance to the last stop
    pub seats: i32
}

#[derive(Copy, Clone)]
pub struct Leg {
    pub id: i64,
    pub route_id: i64,
    pub from: i32,
    pub to: i32,
    pub place: i32, // place in route
    pub dist: i32,
    pub reserve: i32, // to match constraints - wait, loss
    pub started: Option<NaiveDateTime>,
 //   pub completed: Option<NaiveDateTime>,
    pub status: RouteStatus,
    pub passengers: i32, // to meet cab's capacity
    pub seats: i32,
}

/*pub struct Customer {
    pub id: i32,
	pub name: String
}
*/

// dispatcher is interested only into free cabs and non-assigned customers
// TODO: cabs on last leg should be considered
#[derive(Copy, Clone, PartialEq)]
pub enum CabStatus {
//    ASSIGNED,
    FREE = 1,
//    CHARGING, // out of order, ...
}

#[derive(Copy, Clone)]
pub enum OrderStatus {
    RECEIVED = 0,  // sent by customer
    ASSIGNED = 1,  // assigned to a cab, a proposal sent to customer with time-of-arrival
//    ACCEPTED,  // plan accepted by customer, waiting for the cab
//    CANCELLED, // cancelled by customer before assignment
//    REJECTED,  // proposal rejected by customer
//    ABANDONED, // cancelled after assignment but before 'PICKEDUP'
//    REFUSED,   // no cab available, cab broke down at any stage
//    PICKEDUP,
//    COMPLETED
}

#[derive(Copy,Clone, Debug, PartialEq, Eq)]
pub enum RouteStatus {
//    PLANNED,   // proposed by Pool
    ASSIGNED = 1,  // not confirmed, initial status
//    ACCEPTED = 2,  // plan accepted by customer, waiting for the cab
//    REJECTED,  // proposal rejected by customer(s)
//    ABANDONED, // cancelled after assignment but before 'PICKEDUP'
    STARTED = 5,   // status needed by legs
//    COMPLETED = 6
}

/*
impl RouteStatus {
    fn from_u32(value: u32) -> RouteStatus {
        match value {
           // 0 => RouteStatus::PLANNED,
            1 => RouteStatus::ASSIGNED,
           // 2 => RouteStatus::ACCEPTED,
           // 3 => RouteStatus::REJECTED,
           // 4 => RouteStatus::ABANDONED,
            5 => RouteStatus::STARTED,
           // 6 => RouteStatus::COMPLETED,
            _ => panic!("Unknown value: {}", value),
        }
    }
}
*/

/*#[derive(Clone)]
pub struct Route {
	pub id: i64,
//    pub reserve: i32
}
*/

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct Branch {
	pub cost: i16, // the length of the route
	pub outs: u8, // BYTE, number of OUT nodes, so that we can guarantee enough IN nodes
	pub ord_numb: i16, // it is in fact ord number *2; length of vectors below - INs & OUTs
	pub ord_ids : [i16; MAXORDID],
	pub ord_actions: [i8; MAXORDID],
	pub cab :i32,
    pub parity: u8
}

impl Branch {
    pub fn new() -> Self {
        Self {
            cost: 0,
			outs: 0,
			ord_numb: 0,
			ord_ids: [0; MAXORDID],
			ord_actions: [0; MAXORDID],
			cab : -1,
            parity: 0
        }
    }
}

// config read from a file
#[derive(Copy, Clone)]
pub struct KernCfg {
	pub max_assign_time: i64,
    pub max_solver_size: usize,
    pub run_delay:u64,
    pub max_legs: i8,
    pub max_angle: i16,
    pub max_angle_dist: i16,
    pub use_pool: bool,
    pub use_extern_pool: bool,
    pub use_extender: bool,
    pub thread_numb: i32,
    pub stop_wait: i16,
    pub cab_speed: i8,
    pub max_pool5_size: i32,
    pub max_pool4_size: i32,
    pub max_pool3_size: i32,
    pub max_pool2_size: i32,
    pub solver_delay: i32,
}

impl KernCfg {
    pub const fn new() -> Self {
        KernCfg { 
            max_assign_time: 3, // min
            max_solver_size: 500, // count
            run_delay: 15, // secs
            max_legs: 8,
            max_angle: 120,
            max_angle_dist: 3, 
            use_pool: true,
            use_extern_pool: false,
            use_extender: false,
            thread_numb: 11,
            stop_wait: 1,
            cab_speed: 30,
            max_pool5_size: 40,
            max_pool4_size: 130,
            max_pool3_size: 350,
            max_pool2_size: 1000,
            solver_delay: 60,
        }
    }

    pub fn access() -> MutexGuard<'static, KernCfg> {
        static GLOBSTATE: Mutex<KernCfg> = Mutex::new(KernCfg::new());
        GLOBSTATE.lock().unwrap()
    }

    pub fn put(val: KernCfg) {
        let mut s = Self::access();
        s.max_assign_time = val.max_assign_time; // min
        s.max_solver_size = val.max_solver_size; // count
        s.run_delay = val.run_delay; // secs
        s.max_legs = val.max_legs;
        s.max_angle = val.max_angle;
        s.max_angle_dist = val.max_angle_dist;
        s.use_pool = val.use_pool;
        s.use_extern_pool = val.use_extern_pool;
        s.use_extender = val.use_extender;
        s.thread_numb = val.thread_numb;
        s.stop_wait = val.stop_wait;
        s.cab_speed = val.cab_speed;
        s.max_pool5_size = val.max_pool5_size;
        s.max_pool4_size = val.max_pool4_size;
        s.max_pool3_size = val.max_pool3_size;
        s.max_pool2_size = val.max_pool2_size;
        s.solver_delay = val.solver_delay;
    }
}
