use log::{info, warn};
use crate::model::{Stop,MAXSTOPSNUMB};
pub static mut DIST : [[i16; MAXSTOPSNUMB]; MAXSTOPSNUMB] = [[0; MAXSTOPSNUMB]; MAXSTOPSNUMB];
const M_PI : f64 = 3.14159265358979323846264338327950288;
const M_PI_180 : f64 = M_PI / 180.0;
const REV_M_PI_180 : f64 = 180.0 / M_PI;

fn deg2rad(deg: f64) -> f64 { return deg * M_PI_180; }
fn rad2deg(rad: f64) -> f64 { return rad * REV_M_PI_180; }

// https://dzone.com/articles/distance-calculation-using-3
fn dist(lat1:f64, lon1:f64, lat2: f64, lon2: f64) -> f64 {
    let theta = lon1 - lon2;
    let mut dist = deg2rad(lat1).sin() * deg2rad(lat2).sin() + deg2rad(lat1).cos()
                  * deg2rad(lat2).cos() * deg2rad(theta).cos();
    dist = dist.acos();
    dist = rad2deg(dist);
    dist = dist * 60.0 * 1.1515;
    dist = dist * 1.609344;
    return dist;
}

pub fn init_distance(stops: & Vec<Stop>, cab_speed: i8) {
    unsafe {
    for i in 0 .. stops.len() {
        DIST[i][i] = 0;
        for j in i+1 .. stops.len() {
            let mut d = dist(stops[i].latitude, stops[i].longitude, stops[j].latitude, stops[j].longitude)
                         * (60.0 / cab_speed as f64);
            if d as i16 == 0 { d = 1.0; } // a transfer takes at least one minute. 
            DIST[stops[i].id as usize][stops[j].id as usize] = d as i16; // TASK: we might need a better precision - meters/seconds
            DIST[stops[j].id as usize][stops[i].id as usize] 
                = DIST[stops[i].id as usize][stops[j].id as usize];
        }
    }
    }
}

/*
pub fn dump_dist(file_name: &str, size: usize) {
    let file = File::create(file_name);
    info!("Dumping av distance started");
    match file {
        Ok(mut f) => {
            writeln!(f, "{}", size);
            for i in 0..size {
                for j in 0 .. size {
                    write!(f, "{} ", unsafe {DIST[i][j]});
                }
                writeln!(f);
            }

        } 
        Err(err) => {
            warn!("Writing to {} failed: {}", file_name, err);
        }
    }
    info!("Dumping av distance completed");
}
*/

pub fn read_dist(file_name: &String, stop_size: usize){
    match std::fs::read_to_string(file_name) {
        Ok(txt) => {
            let data = txt
                        .trim_end()// remove any new line at the end
                        .split([' ', '\n'])
                        .map(|num| num.parse::<i16>())
                        .collect::<Result<Vec<i16>, _>>()
                        .expect("Number parsing error");
            let size = data[0] as usize;
            if size > stop_size {
                panic!("Number of stops {} is bigger than allowed: {}", size, stop_size);
            }
            if size > MAXSTOPSNUMB {
                panic!("Requested size {} is bigger than matrix size: {}", stop_size, MAXSTOPSNUMB);
            }
            if size == 0 || data.len() != size*size + 1 {
                panic!("Requested size {} is strange as data length is {}", size, data.len());
            }
            for i in 0 .. size {
                for j in 0 .. size {
                    unsafe { DIST[i][j] = data[1 + i*size + j] }; // 1 as the first number is size
                }
            }
        }
        Err(err) => {
            warn!("Opening file {} failed: {}", file_name, err);
        }
    } 
}
