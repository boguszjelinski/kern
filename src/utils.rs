use std::time::SystemTime;

pub fn get_elapsed(val: Option<SystemTime>) -> i64 {
    match val {
        Some(x) => { 
            match x.elapsed() {
                Ok(elapsed) => elapsed.as_secs() as i64,
                Err(_) => -1
            }
        }
        None => -1
    }
}