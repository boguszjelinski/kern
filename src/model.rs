use std::time::{SystemTime};

pub const MAXSTOPSNUMB : usize = 5200;
pub const MAXORDERSNUMB: usize = 2000;
pub const MAXCABSNUMB: usize = 10000;
pub const MAXBRANCHNUMB: usize = 200;

pub const MAXINPOOL : usize = 4;
pub const MAXORDID : usize = MAXINPOOL * 2;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Stop {
    pub id: i64,
    pub bearing: i32,
	pub latitude: f64,
    pub longitude: f64
}

#[derive(Copy, Clone)]
pub struct Order {
    pub id: i64, // -1 as to-be-dropped
	pub from: i32,
    pub to: i32,
	pub wait: i32,
	pub loss: i32,
	pub dist: i32,
    pub shared: bool,
    pub in_pool: bool,
    pub received: Option<SystemTime>,
    pub started: Option<SystemTime>,
    pub completed: Option<SystemTime>,
    pub at_time: Option<SystemTime>,
    pub eta: i32,
  //  cab: Cab,
  //  customer: Customer
}

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


#[repr(C)]
#[derive(Copy, Clone)]
pub struct Cab {
    pub id: i64,
	pub location: i32
}

#[derive(Copy, Clone)]
pub struct Leg {
    pub id: i32,
    pub route_id: i32,
    pub from: i32,
    pub to: i32,
    pub place: i32,
    pub dist: i32,
    pub started: Option<SystemTime>,
    pub completed: Option<SystemTime>,
    pub status: i32 // TODO: RouteStatus
}

pub struct Customer {
    pub id: i32,
	pub name: String
}

pub enum CabStatus {
    ASSIGNED,
    FREE,
    CHARGING, // out of order, ...
}

#[derive(Copy, Clone)]
pub enum OrderStatus {
    RECEIVED,  // sent by customer
    ASSIGNED,  // assigned to a cab, a proposal sent to customer with time-of-arrival
    ACCEPTED,  // plan accepted by customer, waiting for the cab
    CANCELLED, // cancelled by customer before assignment
    REJECTED,  // proposal rejected by customer
    ABANDONED, // cancelled after assignment but before 'PICKEDUP'
    REFUSED,   // no cab available, cab broke down at any stage
    PICKEDUP,
    COMPLETED
}

#[derive(Copy, Clone)]
pub enum RouteStatus {
    PLANNED,   // proposed by Pool
    ASSIGNED,  // not confirmed, initial status
    ACCEPTED,  // plan accepted by customer, waiting for the cab
    REJECTED,  // proposal rejected by customer(s)
    ABANDONED, // cancelled after assignment but before 'PICKEDUP'
    STARTED,   // status needed by legs
    COMPLETED
}

impl RouteStatus {
    fn from_u32(value: u32) -> RouteStatus {
        match value {
            0 => RouteStatus::PLANNED,
            1 => RouteStatus::ASSIGNED,
            2 => RouteStatus::ACCEPTED,
            3 => RouteStatus::REJECTED,
            4 => RouteStatus::ABANDONED,
            5 => RouteStatus::STARTED,
            6 => RouteStatus::COMPLETED,
            _ => panic!("Unknown value: {}", value),
        }
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct Branch {
	pub cost: i16,
	pub outs: u8, // BYTE, number of OUT nodes, so that we can guarantee enough IN nodes
	pub ordNumb: i16, // it is in fact ord number *2; length of vectors below - INs & OUTs
	pub ordIDs : [i32; MAXORDID], // we could get rid of it to gain on memory (key stores this too); but we would lose time on parsing
	pub ordActions: [i8; MAXORDID],
	pub ordIDsSorted: [i32; MAXORDID],
	pub ordActionsSorted: [i8; MAXORDID],
	pub cab :i32
}

impl Branch {
    pub fn new() -> Self {
        Self {
            cost: 0,
			outs: 0,
			ordNumb: 0,
			ordIDs: [0; MAXORDID],
			ordActions: [0; MAXORDID],
			ordIDsSorted: [0; MAXORDID],
			ordActionsSorted: [0; MAXORDID],
			cab : -1
        }
    }
}
