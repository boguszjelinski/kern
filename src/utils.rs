use chrono::{NaiveDateTime, Local};

pub fn get_elapsed(val: Option<NaiveDateTime>) -> i64 {
    match val {
        Some(x) => { 
            let now = Local::now().naive_local();
            (now - x).num_seconds()
        }
        None => -1
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime};
    use super::*;

  #[test]
  fn test_elapsed() {
    let day = NaiveDate::from_ymd_opt(2014, 7, 8).unwrap();
    let time = NaiveTime::from_hms_opt(10, 02, 0).unwrap();
    let past = NaiveDateTime::new(day, time);
    let x = get_elapsed(Some(past));
    let sant = x > 0;
    assert_eq!(sant, true);
  }
}
