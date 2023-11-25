use std::env;

use log::error;
use openai_api_rs::v1::api::Client;
use openai_api_rs::v1::chat_completion::{self, ChatCompletionRequest};
use openai_api_rs::v1::common::GPT3_5_TURBO_16K;

use coredb::log::log_message::LogMessage;

pub struct OpenAIHelper {
  client: Option<Client>,
}

impl OpenAIHelper {
  pub fn new() -> Self {
    let key = env::var("OPENAI_API_KEY");
    let client = match key {
      Ok(key) => Some(Client::new(key)),
      Err(err) => {
        error!("Could not get OpenAI API key, APIs such as summarization and chat with your logs will not work: {}", err);
        None
      }
    };
    Self { client }
  }

  /// Summarize the given logs messages (first k) and retruns the summary, or return None in
  /// case of any error.
  pub fn summarize(&self, logs: &[LogMessage], k: u32) -> Option<String> {
    if self.client.is_none() {
      error!("OpenAI API client is not initialized, summarization will not work");
      return None;
    }
    let client = self.client.as_ref().unwrap();

    let mut first_k_logs = logs;
    if logs.len() > k as usize {
      // Limit the summary to top k logs.
      first_k_logs = &logs[..k as usize];
    }
    let logs_json = serde_json::to_string(first_k_logs).expect("Could not covert logs to json");
    let prompt = "Summarize the log messages below: ".to_owned() + &logs_json;

    let req = ChatCompletionRequest::new(
      GPT3_5_TURBO_16K.to_string(),
      vec![chat_completion::ChatCompletionMessage {
        role: chat_completion::MessageRole::user,
        content: prompt,
        name: None,
        function_call: None,
      }],
    );

    let result = client.chat_completion(req);

    match result {
      Ok(result) => result.choices[0].message.content.clone(),
      Err(err) => {
        error!("Error calling OpenAI API: {:?}", err);
        None
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::utils::environment;

  #[test]
  fn test_summarize_no_openai_key() {
    environment::with_env_vars(vec![], || {
      let openai_helper = OpenAIHelper::new();
      let logs = Vec::new();
      let result = openai_helper.summarize(&logs, 100);
      assert!(result.is_none());
    });
  }

  #[test]
  fn test_summarize_invalid_openai_key() {
    environment::with_env_vars(vec![("OPENAI_API_KEY", Some("invalid_key"))], || {
      let openai_helper = OpenAIHelper::new();
      let logs = Vec::new();
      let result = openai_helper.summarize(&logs, 100);
      assert!(result.is_none());
    });
  }

  // TODO: add a test for summarize for valid key and logs, using mocked OpenAI API client.
}
