use lapin::{
  self,
  options::{BasicPublishOptions, BasicQosOptions, ExchangeDeclareOptions, QueueDeclareOptions},
  types::{AMQPValue, FieldTable, ShortString},
  BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};
use rabbitmq_stream_client::{types::OffsetSpecification, Environment};
use tokio::time::Instant;
use tokio_stream::StreamExt;

use crate::utils::{docker, error::InfinoError};

/// Represents rammitmq for storing append requests, before they are added to the index.
pub struct RabbitMQ {
  container_name: String,
  image_name: String,
  image_tag: String,
  channel: Channel,
  environment: Environment,
}

impl RabbitMQ {
  /// Create a new queue instance.
  pub async fn new(container_name: &str, image_name: &str, image_tag: &str) -> Self {
    let channel =
      Self::create_rmq_connection("amqp://guest:guest@localhost:5672", "connection_name").await;
    Self::create_stream(&channel).await;
    let environment = Environment::builder()
      .host("localhost")
      .username("guest")
      .password("guest")
      .port(5552)
      .build()
      .await
      .unwrap();

    Self {
      container_name: container_name.to_owned(),
      image_name: image_name.to_owned(),
      image_tag: image_tag.to_owned(),
      channel,
      environment,
    }
  }

  #[allow(dead_code)]
  /// Get container name.
  pub fn get_container_name(&self) -> &str {
    &self.container_name
  }

  #[allow(dead_code)]
  /// Get image name.
  pub fn get_image_name(&self) -> &str {
    &self.image_name
  }

  #[allow(dead_code)]
  /// Get image tag.
  pub fn get_image_tag(&self) -> &str {
    &self.image_tag
  }

  /// Helper function to declare stream arguments.
  fn stream_declare_args() -> FieldTable {
    let mut queue_args = FieldTable::default();
    queue_args.insert(
      ShortString::from("x-queue-type"),
      AMQPValue::LongString("stream".into()),
    );
    queue_args.insert(
      ShortString::from("x-max-length-bytes"),
      AMQPValue::LongLongInt(600000000),
    );
    queue_args.insert(
      ShortString::from("x-max-age"),
      AMQPValue::LongString("30m".into()),
    );
    queue_args.insert(
      ShortString::from("x-queue-leader-locator"),
      AMQPValue::LongString("least-leaders".into()),
    );
    queue_args.insert(
      ShortString::from("x-stream-max-segment-size-bytes"),
      AMQPValue::LongLongInt(500000000),
    );
    queue_args
  }

  /// Helper function to create rabbitmq connection.
  async fn create_rmq_connection(connection_string: &str, connection_name: &str) -> Channel {
    let start_time = Instant::now();
    let options = ConnectionProperties::default()
      .with_connection_name(connection_name.into())
      .with_executor(tokio_executor_trait::Tokio::current())
      .with_reactor(tokio_reactor_trait::Tokio);
    loop {
      let connection = Connection::connect(connection_string, options.clone())
        .await
        .unwrap();
      if let Ok(channel) = connection.create_channel().await {
        return channel;
      }
      assert!(
        start_time.elapsed() < std::time::Duration::from_secs(2 * 60),
        "Failed to connect to RabbitMQ"
      );
      tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
  }

  /// Helper function to create a stream.
  async fn create_stream(channel: &Channel) {
    //creating exchange
    let declare_options = ExchangeDeclareOptions {
      durable: true,
      auto_delete: false,
      ..Default::default()
    };
    channel
      .exchange_declare(
        "exchange",
        ExchangeKind::Topic,
        declare_options,
        FieldTable::default(),
      )
      .await
      .unwrap();
    channel
      .basic_qos(1000u16, BasicQosOptions { global: false })
      .await
      .unwrap();
    //creating stream
    channel
      .queue_declare(
        "stream",
        QueueDeclareOptions {
          durable: true,
          auto_delete: false,
          ..Default::default()
        },
        Self::stream_declare_args(),
      )
      .await
      .unwrap();

    // Sometimes it takes a couple seconds for the stream to come online
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    channel
      .queue_bind(
        "stream",
        "exchange",
        "#",
        Default::default(),
        FieldTable::default(),
      )
      .await
      .unwrap();
  }

  /// Publish a message to the queue.
  pub async fn publish(&self, message: &str) -> Result<(), InfinoError> {
    // publishing a message to stream
    let properties = BasicProperties::default().with_delivery_mode(2);
    self
      .channel
      .basic_publish(
        "exchange",
        "#",
        BasicPublishOptions::default(),
        message.as_bytes(),
        properties,
      )
      .await
      .unwrap();

    Ok(())
  }

  // TODO: once this function starts getting used in recovery at the startup, remove the dead_code annotation.
  #[allow(dead_code)]
  /// Consume a message from the queue.
  pub async fn consume_next(&self) -> Option<String> {
    let mut consumer = self
      .environment
      .consumer()
      .offset(OffsetSpecification::First)
      .build("stream")
      .await
      .unwrap();

    let handle = consumer.handle();
    let next = consumer.next().await;
    let retval = if next.is_none() {
      None
    } else {
      let delivery = Some(next)
        .unwrap()
        .unwrap()
        .expect("Could not get delivery");
      let data = delivery.message().data().unwrap();
      let str_data = std::str::from_utf8(data).unwrap();
      Some(str_data.to_owned())
    };
    handle.close().await.unwrap();

    retval
  }

  /// Start a rabbitmq container.
  pub async fn start_queue_container(
    container_name: &str,
    image_name: &str,
    image_tag: &str,
  ) -> Result<(), InfinoError> {
    let result = docker::start_docker_container(
      container_name,
      image_name,
      image_tag,
      &[
        "-p",
        "5672:5672",
        "-p",
        "5552:5552",
        "--user",
        "rabbitmq",
        "-e",
        "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=-rabbitmq_stream\\ advertised_host\\ localhost",
      ],
    );
    if result.is_err() {
      return Err(InfinoError::QueueIOError(result.err().unwrap()));
    }
    let result = docker::exec_command(
      container_name,
      &["rabbitmq-plugins", "enable", "rabbitmq_stream"],
    );
    if result.is_err() {
      return Err(InfinoError::QueueIOError(result.err().unwrap()));
    }

    Ok(())
  }

  /// Stop the rabbitmq container.
  pub fn stop_queue_container(container_name: &str) -> Result<(), InfinoError> {
    let result = docker::stop_docker_container(container_name);
    if let Err(err) = result {
      return Err(InfinoError::QueueIOError(err));
    }

    let result = docker::remove_docker_container(container_name);
    if let Err(err) = result {
      return Err(InfinoError::QueueIOError(err));
    }

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use serial_test::serial;

  #[ignore = "avoid port conflict in case the machine running tests also has infino running in production"]
  #[serial]
  #[tokio::test]
  async fn test_queue() {
    let container_name = "infino-test";
    let image_name = "rabbitmq";
    let image_tag = "3";

    // Stop any container from a prior test - useful in case of test failures if the container is
    // left around without terminating it.
    let _ = RabbitMQ::stop_queue_container(container_name);

    let start_result = RabbitMQ::start_queue_container(container_name, image_name, image_tag).await;
    assert!(start_result.is_ok());
    // The container is not immediately ready to accept connections - hence sleep for some time.
    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;

    let rmq = RabbitMQ::new(container_name, "rabbitmq", "3").await;
    assert_eq!(rmq.get_container_name(), container_name);
    assert_eq!(rmq.get_image_name(), image_name);
    assert_eq!(rmq.get_image_tag(), image_tag);

    let expected = "This is my message";
    rmq.publish(expected).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let received = rmq.consume_next().await.unwrap();
    assert_eq!(expected, received);

    let stop_result = RabbitMQ::stop_queue_container(container_name);
    assert!(stop_result.is_ok());
  }
}
