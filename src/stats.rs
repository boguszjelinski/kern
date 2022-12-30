use std::{time::{Instant}, fmt};
use self::Stat::*;
use std::slice::Iter;

pub static mut STATS: [i64; Stat::TotalPickupDistance as usize + 1] 
                        = [0; Stat::TotalPickupDistance as usize + 1];
const INT: Vec<i64> = vec![];
pub static mut AVG_ELEMENTS: [Vec<i64>; Stat::TotalPickupDistance as usize + 1] 
                        = [INT; Stat::TotalPickupDistance as usize + 1];

#[derive(Debug,Copy,Clone)]
pub enum Stat {
    AvgExtenderTime,
    AvgPoolTime,
    AvgPool3Time, // not updated as it runs in C
    AvgPool4Time, // not updated
    AvgLcmTime,
    AvgSolverTime,
    AvgShedulerTime,

    MaxExtenderTime,
    MaxPoolTime,
    MaxPool3Time,
    MaxPool4Time,
    MaxLcmTime,
    MaxSolverTime,
    MaxShedulerTime,

    AvgDemandSize, // at start
    AvgPoolDemandSize,  // after extender
    AvgSolverDemandSize, // after pool

    MaxDemandSize, // at start
    MaxPoolDemandSize,  // after extender
    MaxSolverDemandSize, // after pool

    AvgOrderAssignTime,

    TotalLcmUsed, // do we need this
    TotalPickupDistance, // !! must be the last position cause it is used for sizing of an array :)
}

impl Stat {
   /* pub fn from_u32(value: u32) -> Stat {
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
    */
    pub fn iterator() -> Iter<'static, Stat> {
        static RET: [Stat; 23] = [
            AvgExtenderTime,
            AvgPoolTime,
            AvgPool3Time, // not updated as it runs in C
            AvgPool4Time, // not updated
            AvgLcmTime,
            AvgSolverTime,
            AvgShedulerTime,
        
            MaxExtenderTime,
            MaxPoolTime,
            MaxPool3Time,
            MaxPool4Time,
            MaxLcmTime,
            MaxSolverTime,
            MaxShedulerTime,
        
            AvgDemandSize, // at start
            AvgPoolDemandSize,  // after extender
            AvgSolverDemandSize, // after pool
        
            MaxDemandSize, // at start
            MaxPoolDemandSize,  // after extender
            MaxSolverDemandSize, // after pool
        
            AvgOrderAssignTime,
        
            TotalLcmUsed, // do we need this
            TotalPickupDistance,
        ];
        RET.iter()
    }
}

impl fmt::Display for Stat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
        // or, alternatively:
        // fmt::Debug::fmt(self, f)
    }
}

pub fn update_max(key: Stat, value: i64) {
    unsafe { 
        if value > STATS[key as usize] {
            STATS[key as usize] = value;
    }}
}

pub fn update_val(key: Stat, value: i64) {
    unsafe { STATS[key as usize] = value; }
}

pub fn incr_val(key: Stat) {
    unsafe { STATS[key as usize] += 1; }
}

pub fn add_avg_element(key: Stat, time: i64) {
    unsafe { AVG_ELEMENTS[key as usize].push(time);}
}

pub fn count_average(key: Stat) -> i64 {   
    unsafe {
        let list: Vec<i64> = AVG_ELEMENTS[key as usize].to_vec();
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

pub fn update_max_and_avg_time(key_avg: Stat, key_max: Stat, start: Instant) {
    let total_time = start.elapsed().as_secs() as i64;
    update_max_and_avg_stats(key_avg, key_max, total_time);
}

pub fn update_max_and_avg_stats(key_avg: Stat, key_max: Stat, val: i64) {
    add_avg_element(key_avg, val);
    update_val(key_avg, count_average(key_avg));
    update_max(key_max, val);
}
