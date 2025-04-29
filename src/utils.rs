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
    use chrono::NaiveDate;
    use super::*;

  #[test]
  fn test_elapsed() {
    let past = Some(NaiveDate::from_ymd(2018, 3, 26).and_hms(10, 02, 0));
    let x = get_elapsed(past);
    let sant = x > 0;
    assert_eq!(sant, true);
  }
}
