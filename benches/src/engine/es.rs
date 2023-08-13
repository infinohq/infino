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
  indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts},
  params::Refresh,
  BulkParts, Elasticsearch, Error, IndexParts,
};
use serde_json::{json, Value};
use url::Url;

use crate::utils::io;

static INDEX_NAME: &'static str = "perftest";

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

    ElasticsearchEngine { client: client }
  }

  pub async fn index_lines(&self, input_data_path: &str, max_docs: i32) {
    let mut num_docs = 0;
    let num_docs_per_batch = 100;
    let mut num_docs_in_this_batch = 0;
    let mut logs_batch = Vec::new();
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
              body.push(json!({"index": {"_id": log.get("id").unwrap()}}).into());
              body.push(
                json!({
                    "date": log.get("date").unwrap(),
                    "message": log.get("message").unwrap(),
                })
                .into(),
              );
            }
            #[allow(unused)]
            let insert = self
              .client
              .bulk(BulkParts::Index(INDEX_NAME))
              .body(body)
              .send()
              .await
              .unwrap();
            println!("#{} {:?}", num_docs, insert);

            let refresh_response = self
              .client
              .index(IndexParts::Index(INDEX_NAME))
              .refresh(Refresh::True)
              .send()
              .await
              .unwrap();
            println!("Refresh response: {:?}", refresh_response);

            num_docs_in_this_batch = 0;
          } //end if num_docs_in_this_batch == num_docs_per_batch
        }
      }
    }
    let elapsed = now.elapsed();
    println!("Elasticsearch time required for insertion: {:.2?}", elapsed);
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

  pub async fn get_index_size(&self) -> String {
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

    cat.text().await.unwrap()
  }

  fn create_client() -> Result<Elasticsearch, Error> {
    let url = Url::parse("http://localhost:9200".as_ref()).unwrap();
    let conn_pool = SingleNodeConnectionPool::new(url);
    let builder = TransportBuilder::new(conn_pool).cert_validation(CertificateValidation::None);
    let transport = builder.build()?;
    Ok(Elasticsearch::new(transport))
  }

  pub async fn search(&self, query: &str) -> usize {
    let num_words = query.split_whitespace().count();
    let response = self
      .client
      .search(SearchParts::Index(&[INDEX_NAME]))
      .from(0)
      // FIXME: ES not returning more than 10k results, if there are more than 10k results it panics
      .size(10000)
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
    let took = response_body["took"].as_i64().unwrap();
    #[allow(unused)]
    let search_hits = response_body["hits"]["total"]["value"].as_i64().unwrap();

    println!(
      "Elasticsearch time required for searching {} word query is : {:.2?} ms",
      num_words, took
    );
    return search_hits.try_into().unwrap();
  }

  pub async fn search_multiple_queries(&self, queries: &[&str]) -> usize {
    let mut search_results = vec![];

    for query in queries {
      let result = self.search(query).await;
      search_results.push(result);
    }

    search_results.len()
  }
}
