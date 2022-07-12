use std::time::{SystemTime};

pub struct Stop {
    pub id: i64,
	pub latitude: f64,
    pub longitude: f64,
	pub bearing: i16
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
    pub received: SystemTime,
    pub started: Option<SystemTime>,
    pub completed: Option<SystemTime>,
    pub at_time: Option<SystemTime>,
    pub eta: i32,
  //  cab: Cab,
  //  customer: Customer
}

#[derive(Copy, Clone)]
pub struct Cab {
    pub id: i32,
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
