// This code is licensed under Apache License 2.0
// https://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::time::Instant;

use chrono::Utc;
use elasticsearch::cert::CertificateValidation;
use elasticsearch::http::request::JsonBody;
use elasticsearch::SearchParts;
use elasticsearch::{
    cat::CatIndicesParts,
    http::headers::HeaderMap,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    http::Method,
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts, IndicesFlushParts},
    params::Refresh,
    BulkParts, Elasticsearch, Error, IndexParts,
};
use serde_json::{json, Value};
use url::Url;

use crate::utils::io;

const FLUSH_LIMIT_BYTES: u64 = 524288000; // 500MB

static INDEX_NAME: &str = "perftest";

pub struct ElasticsearchEngine {
    client: Elasticsearch,
}

impl ElasticsearchEngine {
    pub async fn new() -> ElasticsearchEngine {
        let client = ElasticsearchEngine::create_client().unwrap();

        let exists = client
            .indices()
            .exists(IndicesExistsParts::Index(&[INDEX_NAME]))
            .send()
            .await
            .unwrap();
        if exists.status_code().is_success() {
            println!("Index {} already exists. Now deleting it.", INDEX_NAME);
            let delete = client
                .indices()
                .delete(IndicesDeleteParts::Index(&[INDEX_NAME]))
                .send()
                .await
                .unwrap();
            if !delete.status_code().is_success() {
                panic!("Problem deleting index: {}", INDEX_NAME);
            }
        }
        println!("Creating test index {}", INDEX_NAME);
        let response = client
            .indices()
            .create(IndicesCreateParts::Index(INDEX_NAME))
            .body(json!(
                {
                  "mappings": {
                    "properties": {
                      "message": {
                        "type": "text"
                      }
                    }
                  },
                  "settings": {
                    "index.number_of_shards": 1,
                    "index.number_of_replicas": 0
                  }
                }
            ))
            .send()
            .await
            .unwrap();
        if !response.status_code().is_success() {
            println!("Error while creating index {:?}", response);
        }

        ElasticsearchEngine { client }
    }

    /// Indexes input data and returns the time required for insertion as microseconds.
    pub async fn index_lines(&self, input_data_path: &str, max_docs: i32) -> u128 {
        let mut num_docs = 0;
        let num_docs_per_batch = 100;
        let mut num_docs_in_this_batch = 0;
        let mut logs_batch = Vec::new();
        let mut bytes_sent_since_flush: u64 = 0;
        let now = Instant::now();

        if let Ok(lines) = io::read_lines(input_data_path) {
            for line in lines {
                num_docs += 1;

                // If max_docs is less than 0, we index all the documents.
                // Otherwise, do not indexing more than max_docs documents.
                if max_docs > 0 && num_docs > max_docs {
                    println!(
                        "Already indexed {} documents. Not indexing anymore.",
                        max_docs
                    );
                    break;
                }
                if let Ok(message) = line {
                    let mut log = HashMap::new();
                    log.insert("id", json!(num_docs));
                    log.insert("date", json!(Utc::now().timestamp_millis() as u64));
                    log.insert("message", json!(message));
                    logs_batch.push(log);
                    num_docs_in_this_batch += 1;

                    if num_docs_in_this_batch == num_docs_per_batch {
                        let mut body: Vec<JsonBody<_>> = Vec::with_capacity(num_docs_per_batch);
                        for log in &logs_batch {
                            let log_json = json!({
                                  "date": log.get("date").unwrap(),
                                  "message": log.get("message").unwrap(),
                              });
                            bytes_sent_since_flush += log_json.to_string().len() as u64;
                            body.push(json!({"index": {"_id": log.get("id").unwrap()}}).into());
                            body.push(JsonBody::from(log_json));
                        }
                        #[allow(unused)]
                        let insert = self
                            .client
                            .bulk(BulkParts::Index(INDEX_NAME))
                            .body(body)
                            .send()
                            .await
                            .unwrap();
                        //println!("#{} {:?}", num_docs, insert);
                        num_docs_in_this_batch = 0;
                        logs_batch.clear();

                        // Check if we need to flush
                        if bytes_sent_since_flush > FLUSH_LIMIT_BYTES {
                            #[allow(unused)]
                            let flush = self
                                .client
                                .indices()
                                .flush(IndicesFlushParts::Index(&[INDEX_NAME]))
                                .send()
                                .await
                                .unwrap();
                            println!("Flushed index {} after writing {} bytes", INDEX_NAME, bytes_sent_since_flush);
                            bytes_sent_since_flush = 0;
                        }

                    } //end if num_docs_in_this_batch == num_docs_per_batch
                }
            }
        }

        #[allow(unused)]
        let refresh_response = self
            .client
            .index(IndexParts::Index(INDEX_NAME))
            .refresh(Refresh::True)
            .send()
            .await
            .unwrap();
        //println!("Refresh response: {:?}", refresh_response);

        let elapsed = now.elapsed().as_micros();
        println!(
            "Elasticsearch time required for insertion: {} microseconds",
            elapsed
        );

        elapsed
    }

    pub async fn forcemerge(&self) {
        let empty_query_string: HashMap<String, String> = HashMap::new();
        let merge = self
            .client
            .transport()
            .send(
                Method::Post,
                "/_forcemerge",
                HeaderMap::new(),
                Some(&empty_query_string),
                Some(""),
                None,
            )
            .await
            .unwrap();

        if merge.status_code() != 200 {
            panic!("Could not forcemerge: {:?}", merge);
        }
    }

    pub async fn get_index_size(&self) -> u64 {
      let cat = self
          .client
          .cat()
          .indices(CatIndicesParts::Index(&[INDEX_NAME]))
          .format("json")
          .send()
          .await
          .unwrap();
  
      if cat.status_code() != 200 {
          panic!("Could not get stats for index {}: {:?}", INDEX_NAME, cat);
      }
  
      let cat_response: String = cat.text().await.unwrap();
      println!(
          "Elasticsearch cat response to figure out index size: {}",
          cat_response
      );
  
      // Parse the JSON response
      let cat_json: Value = serde_json::from_str(&cat_response).unwrap();
  
      // Extract the index size in bytes
      if let Some(index) = cat_json.as_array().and_then(|arr| arr.first()) {
          if let Some(size) = index.get("store.size") {
              if let Some(size_str) = size.as_str() {
                  // Parse the size value with the unit suffix
                  let size_bytes = Self::parse_size_with_unit(size_str);
                  println!("Index Size (in bytes): {}", size_bytes);
                  return size_bytes;
              }
          }
      }
  
      println!("Could not programmatically figure out elasticsearch index size. Figure it out from the cat response printed above");
      0
  }
  
  fn parse_size_with_unit(size_str: &str) -> u64 {
      let size_parts: Vec<&str> = size_str.split_inclusive(char::is_alphabetic).collect();
      if size_parts.len() == 2 {
          let size_val = size_parts[0].parse::<u64>().unwrap_or(0);
          let unit = size_parts[1].trim();
          match unit {
              "b" => size_val,
              "kb" => size_val * 1024,
              "mb" => size_val * 1024 * 1024,
              "gb" => size_val * 1024 * 1024 * 1024,
              _ => 0,
          }
      } else {
          0
      }
  }
    fn create_client() -> Result<Elasticsearch, Error> {
        let url = Url::parse("http://localhost:9200").unwrap();
        let conn_pool = SingleNodeConnectionPool::new(url);
        let builder = TransportBuilder::new(conn_pool).cert_validation(CertificateValidation::None);
        let transport = builder.build()?;
        Ok(Elasticsearch::new(transport))
    }

    /// Searches the given term and returns the time required in microseconds
    pub async fn search(&self, query: &str) -> u128 {
        let now = Instant::now();

        let response = self
            .client
            .search(SearchParts::Index(&[INDEX_NAME]))
            // .from(0)
            // .size(10000)
            .body(json!({
                "query": {
                    "match": {
                        "message": query
                    }
                }
            }))
            .send()
            .await
            .unwrap();

        let response_body = response.json::<Value>().await.unwrap();

        let elapsed = now.elapsed().as_micros();

        // let took = response_body["took"].as_i64().unwrap();
        #[allow(unused)]
        let search_hits = response_body["hits"]["total"]["value"].as_i64().unwrap();

        println!(
            "Elasticsearch time required for searching query {} is : {} microseconds",
            query, elapsed
        );

        elapsed
    }

    /// Runs multiple queries and returns the sum of time needed to run them in microseconds.
    pub async fn search_multiple_queries(&self, queries: &[&str]) -> u128 {
        let mut time = 0;
        for query in queries {
            time += self.search(query).await;
        }
        time
    }
}