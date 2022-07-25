use std::{time::{Instant}, fmt};
use self::Stat::*;
use std::slice::Iter;

pub static mut stats: [i64; Stat::TOTAL_PICKUP_DISTANCE as usize + 1] 
                        = [0; Stat::TOTAL_PICKUP_DISTANCE as usize + 1];
const int: Vec<i64> = vec![];
pub static mut avgElements: [Vec<i64>; Stat::TOTAL_PICKUP_DISTANCE as usize + 1] 
                        = [int; Stat::TOTAL_PICKUP_DISTANCE as usize + 1];

#[derive(Debug,Copy,Clone)]
pub enum Stat {
    AVG_EXTENDER_TIME,
    AVG_POOL_TIME,
    AVG_POOL3_TIME,
    AVG_POOL4_TIME,
    AVG_LCM_TIME,
    AVG_SOLVER_TIME,
    AVG_SHEDULER_TIME,

    MAX_EXTENDER_TIME,
    MAX_POOL_TIME,
    MAX_POOL3_TIME,
    MAX_POOL4_TIME,
    MAX_LCM_TIME,
    MAX_SOLVER_TIME,
    MAX_SHEDULER_TIME,

    AVG_DEMAND_SIZE, // at start
    AVG_POOL_DEMAND_SIZE,  // after extender
    AVG_SOLVER_DEMAND_SIZE, // after pool

    MAX_DEMAND_SIZE, // at start
    MAX_POOL_DEMAND_SIZE,  // after extender
    MAX_SOLVER_DEMAND_SIZE, // after pool

    AVG_ORDER_ASSIGN_TIME,
    AVG_ORDER_PICKUP_TIME,
    AVG_ORDER_COMPLETE_TIME,

    TOTAL_LCM_USED, // do we need this
    TOTAL_PICKUP_DISTANCE // !! must be the last position cause it is used for sizing of an array :)
}

impl Stat {
    pub fn from_u32(value: u32) -> Stat {
        match value {
            0 => Stat::AVG_EXTENDER_TIME,
            1 => Stat::AVG_POOL_TIME,
            2 => Stat::AVG_POOL3_TIME,
            3 => Stat::AVG_POOL4_TIME,
            4 => Stat::AVG_LCM_TIME,
            5 => Stat::AVG_SOLVER_TIME,
            6 => Stat::AVG_SHEDULER_TIME,
            7 => Stat::MAX_EXTENDER_TIME,
            8 => Stat::MAX_POOL_TIME,
            9 => Stat::MAX_POOL3_TIME,
            10 => Stat::MAX_POOL4_TIME,
            11 => Stat::MAX_LCM_TIME,
            12 => Stat::MAX_SOLVER_TIME,
            13 => Stat::MAX_SHEDULER_TIME,
            14 => Stat::AVG_DEMAND_SIZE, 
            15 => Stat::AVG_POOL_DEMAND_SIZE,
            16 => Stat::AVG_SOLVER_DEMAND_SIZE,
            17 => Stat::MAX_DEMAND_SIZE,
            18 => Stat::MAX_POOL_DEMAND_SIZE,
            19 => Stat::MAX_SOLVER_DEMAND_SIZE,
            20 => Stat::AVG_ORDER_ASSIGN_TIME,
            21 => Stat::AVG_ORDER_PICKUP_TIME,
            22 => Stat::AVG_ORDER_COMPLETE_TIME,
            23 => Stat::TOTAL_LCM_USED,
            24 => Stat::TOTAL_PICKUP_DISTANCE,
            _ => panic!("Unknown value: {}", value),
        }
    }

    pub fn iterator() -> Iter<'static, Stat> {
        static ret: [Stat; 25] = [
            AVG_EXTENDER_TIME,
            AVG_POOL_TIME,
            AVG_POOL3_TIME,
            AVG_POOL4_TIME,
            AVG_LCM_TIME,
            AVG_SOLVER_TIME,
            AVG_SHEDULER_TIME,

            MAX_EXTENDER_TIME,
            MAX_POOL_TIME,
            MAX_POOL3_TIME,
            MAX_POOL4_TIME,
            MAX_LCM_TIME,
            MAX_SOLVER_TIME,
            MAX_SHEDULER_TIME,

            AVG_DEMAND_SIZE,
            AVG_POOL_DEMAND_SIZE,
            AVG_SOLVER_DEMAND_SIZE,

            MAX_DEMAND_SIZE,
            MAX_POOL_DEMAND_SIZE, 
            MAX_SOLVER_DEMAND_SIZE,

            AVG_ORDER_ASSIGN_TIME,
            AVG_ORDER_PICKUP_TIME,
            AVG_ORDER_COMPLETE_TIME,

            TOTAL_LCM_USED,
            TOTAL_PICKUP_DISTANCE
        ];
        ret.iter()
    }
}

impl fmt::Display for Stat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
        // or, alternatively:
        // fmt::Debug::fmt(self, f)
    }
}


#[derive(Copy, Clone)]
pub struct Val {
    val: i64
}

pub fn updateMaxIntVal(key: Stat, value: i64) {
    unsafe { 
        if value > stats[key as usize] {
            stats[key as usize] = value;
    }}
}

pub fn addToIntVal(key: Stat, value: i64) {
    unsafe { stats[key as usize] += value; }
}

pub fn updateIntVal(key: Stat, value: i64) {
    unsafe { stats[key as usize] = value; }
}

pub fn incrementIntVal(key: Stat) {
    unsafe { stats[key as usize] += 1; }
}

pub fn addAverageElement(key: Stat, time: i64) {
    unsafe { avgElements[key as usize].push(time);}
}

pub fn countAverage(key: Stat) -> i64 {   
    unsafe {
        let list: Vec<i64> = avgElements[key as usize].to_vec();
        let length = list.len();
        if length == 0 {
            return 0;
        }
        let mut suma: i64 = 0;
        for i in list {
            suma += i;
        }
        
        return (suma / length as i64) as i64;
    } 
}

pub fn updateMaxAndAvgTime(keyAvg: Stat, keyMax: Stat, start: Instant) {
    let totalTime = start.elapsed().as_secs() as i64;
    addAverageElement(keyAvg, totalTime);
    updateMaxIntVal(keyMax, totalTime);
}

pub fn updateMaxAndAvgStats(keyAvg: Stat, keyMax: Stat, val: i64) {
    addAverageElement(keyAvg, val);
    updateMaxIntVal(keyMax, val);
}
