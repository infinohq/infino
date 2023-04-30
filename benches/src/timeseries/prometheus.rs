use prometheus::{Encoder, Gauge, Opts, Registry, TextEncoder};
use serde::{Deserialize, Serialize};
use serde_json;
use std::rc::Rc;
use std::{net::SocketAddr, time::Instant};
use sysinfo::{ProcessorExt, System, SystemExt};
use tokio::task::JoinHandle;
use warp::{http::Response, Filter};

#[derive(Serialize, Deserialize, Debug)]
struct Metric {
  __name__: String,
  instance: String,
  job: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Value {
  #[serde(rename = "0")]
  timestamp: f64,
  #[serde(rename = "1")]
  value: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Result {
  metric: Metric,
  value: Value,
}

#[derive(Serialize, Deserialize, Debug)]
struct QueryResult {
  status: String,
  data: Data,
}

#[derive(Serialize, Deserialize, Debug)]
struct Data {
  resultType: String,
  result: Vec<Result>,
}

#[derive(Clone, Copy)]
pub struct PrometheusClient {
  stop: bool,
}

impl PrometheusClient {
  pub fn new() -> Self {
    PrometheusClient { stop: false }
  }

  pub async fn append_ts(&self) {
    let registry = Registry::new();

    let cpu_usage_opts = Opts::new("cpu_usage", "CPU usage percentage");
    let cpu_usage_gauge = Gauge::with_opts(cpu_usage_opts).unwrap();
    registry
      .register(Box::new(cpu_usage_gauge.clone()))
      .unwrap();

    let addr = SocketAddr::from(([0, 0, 0, 0], 9000));
    let prometheus_route = warp::path("metrics").map(move || {
      let encoder = TextEncoder::new();
      let metric_families = registry.gather();
      let mut buffer = vec![];
      encoder.encode(&metric_families, &mut buffer).unwrap();
      Response::builder()
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(buffer)
        .unwrap()
    });

    let query_route = warp::path("query").and(warp::get()).and_then(|| {
      // Query the Prometheus API to get the CPU usage gauge value
      async move {
        let query_url = "http://localhost:9090/api/v1/query?query=cpu_usage";
        let response = reqwest::get(query_url).await;

        println!("Response {:?}", response);
        match response {
          Ok(res) => {
            let text = res.text().await.unwrap();
            println!("Result {}", text);
            let result: QueryResult = serde_json::from_str(&text).unwrap();
            println!("Result {:?}", result);
            Ok::<_, warp::Rejection>(warp::reply::json(&result))
          }
          Err(_) => Err(warp::reject::reject()),
        }
      }
    });

    // Start a background task to update the CPU usage gauge
    let _cpu_usage_task = tokio::spawn(async move {
      loop {
        let system = System::new();
        // Get the current CPU usage percentage
        let cpu_usage_percent = system.get_processors()[0].get_cpu_usage();
        // Update the Prometheus Gauge with the CPU usage percentage
        cpu_usage_gauge.set(cpu_usage_percent as f64);
        // println!("Cpu usage {}", cpu_usage_percent);
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
      }
    });

    let routes = prometheus_route.or(query_route);
    let server = warp::serve(routes);

    // Start the server in a background task
    let server_task = tokio::spawn(server.run(addr));

    // Hack for now
    tokio::time::sleep(tokio::time::Duration::from_secs(10000)).await;
    server_task.abort();
  }

  pub async fn search(&self) -> u128 {
    let query_url = "http://localhost:9090/api/v1/query?query=cpu_usage";
    let now = Instant::now();
    let response = reqwest::get(query_url).await;
    let elapsed = now.elapsed();
    println!(
      "PrometheusClient time required for searching {:.2?}",
      elapsed
    );

    // println!("Response {:?}", response);
    match response {
      Ok(res) => {
        let text = res.text().await.unwrap();
        // println!("Result {}", text);
        let result: QueryResult = serde_json::from_str(&text).unwrap();
        // println!("Result {:?}", result);
        elapsed.as_nanos()
      }
      Err(err) => {
        println!("Error while fetching from prometheus: {}", err);
        elapsed.as_nanos()
      }
    }
  }

  pub fn stop(mut self) {
    self.stop = true;
  }
}
