use chrono::Utc;
use reqwest::StatusCode;
use std::time::Instant;
use sysinfo::{ProcessorExt, System, SystemExt};

pub struct InfinoTsClient {}

impl InfinoTsClient {
  pub fn new() -> InfinoTsClient {
    // Start a background task to update the CPU usage gauge
    #[allow(unused)]
    let cpu_usage_task = Some(tokio::spawn(async move {
      loop {
        let system = System::new();
        // Get the current CPU usage percentage
        let cpu_usage_percent = system.get_processors()[0].get_cpu_usage();
        // Update the Prometheus Gauge with the CPU usage percentage

        let time = Utc::now().timestamp_millis() as u64;
        let value = cpu_usage_percent as f64;

        let json_str = format!("{{\"date\": {}, \"{}\":{}}}", time, "cpu_usage", value);
        let client = reqwest::Client::new();
        let res = client
          .post(&format!("http://localhost:3000/append_ts"))
          .header("Content-Type", "application/json")
          .body(json_str)
          .send()
          .await;

        match res {
          Ok(response) => {
            if response.status() != StatusCode::OK {
              println!("Error while pushing ts to infino {:?}", response)
            }
          }
          Err(e) => {
            println!("Error while pushing ts to infino {:?}", e)
          }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
      }
    }));

    InfinoTsClient {}
  }

  pub async fn search(&self) -> u128 {
    let query_url =
      "http://localhost:3000/search_ts?label_name=__name__&&label_value=cpu_usage&start_time=0";
    let now = Instant::now();
    let response = reqwest::get(query_url).await;
    let elapsed = now.elapsed();
    println!(
      "Infino Time series time required for searching {:.2?}",
      elapsed
    );

    // println!("Response {:?}", response);
    match response {
      Ok(res) => {
        #[allow(unused)]
        let text = res.text().await.unwrap();
        // println!("Result {}", text);
        elapsed.as_micros()
      }
      Err(err) => {
        println!("Error while fetching from prometheus: {}", err);
        elapsed.as_micros()
      }
    }
  }
}
