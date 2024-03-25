// This code is licensed under Apache License 2.0
// https://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::time::Instant;

use chrono::Utc;
use elasticsearch::cert::CertificateValidation;
use elasticsearch::http::request::JsonBody;
use elasticsearch::SearchParts;
use elasticsearch::{
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts, IndicesFlushParts},
    params::Refresh,
    BulkParts, Elasticsearch, Error, IndexParts,
};
use serde_json::{json, Value};
use url::Url;

use crate::utils::io;

const FLUSH_LIMIT_BYTES: u64 = 524288000; // 500MB

static INDEX_NAME: &str = "perftest";

pub struct InfinoOpenSearchEngine {
    client: Elasticsearch,
}

impl InfinoOpenSearchEngine {
    pub async fn new() -> InfinoOpenSearchEngine {
        let client = InfinoOpenSearchEngine::create_client().unwrap();

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

        InfinoOpenSearchEngine { client }
    }

    /// Indexes input data and returns the time required for insertion as ms.
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

        let elapsed = now.elapsed().as_micros();
        println!(
            "Infino OpenSearch time required for insertion: {} microseconds",
            elapsed
        );

        elapsed / 1000
    }
  
    fn create_client() -> Result<Elasticsearch, Error> {
        let url = Url::parse("http://localhost:9200").unwrap();
        let conn_pool = SingleNodeConnectionPool::new(url);
        let builder = TransportBuilder::new(conn_pool).cert_validation(CertificateValidation::None);
        let transport = builder.build()?;
        Ok(Elasticsearch::new(transport))
    }

    /// Searches the given term and returns the time required in ms
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

        #[allow(unused)]
        let search_hits = response_body["hits"]["total"]["value"].as_i64().unwrap();

        println!(
            "Infino OpenSearch time required for searching query {} is : {} microseconds",
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