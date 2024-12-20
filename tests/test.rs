use std::sync::Arc;

use futures_lite::stream::StreamExt;
use lapin::{options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties};
use tracing::{error, info};

#[tokio::test]
async fn test() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    tracing_subscriber::fmt::init();

    let addr = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672".into());

    let conn = Connection::connect(
        &addr,
        ConnectionProperties::default().with_executor(tokio_executor_trait::Tokio::current()),
    )
    .await
    .unwrap();

    info!("Connected");

    let publish_channel = conn.create_channel().await.unwrap();

    let queue = publish_channel
        .queue_declare(
            "queue_name",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    info!(?queue, "Declared queue");

    #[derive(Clone, Default)]
    struct Context {
        message_received: Arc<tokio::sync::Notify>,
        finish_message_handler: Arc<tokio::sync::Notify>,
        ack_sent: Arc<tokio::sync::Notify>,
    }

    let context = Context::default();

    let stop_consumer = Arc::new(tokio::sync::Notify::new());

    {
        let context = context.clone();
        let stop_consumer = stop_consumer.clone();

        let consume_channel = conn.create_channel().await.unwrap();

        let mut consumer = consume_channel
            .basic_consume(
                "queue_name",
                "my_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop_consumer.notified() => {
                        consume_channel
                            .basic_cancel(consumer.tag().as_str(), BasicCancelOptions::default())
                            .await
                            .unwrap();
                    }

                    message = consumer.next() => {
                        match message {
                            None => { break; }
                            Some(Err(e)) => {
                                error!("Consumer error: {}", e);
                                break;
                            }
                            Some(Ok(delivery)) => {
                                let context = context.clone();
                                tokio::spawn(async move {
                                    info!("Message received");
                                    context.message_received.notify_one();

                                    context.finish_message_handler.notified().await;
                                    info!("Message handling finished");

                                    delivery.nack(BasicNackOptions {
                                        multiple: false,
                                        requeue: true,
                                    }).await
                                    .expect("Nack should fail successfully");

                                    info!("Acked message");
                                    context.ack_sent.notify_one();
                                });
                            }
                        }
                    }
                }
            }

            info!("Consumer finished, consumer and channel will be dropped");
        });
    }

    let payload = b"Hello world!";

    let _confirm = publish_channel
        .basic_publish(
            "",
            "queue_name",
            BasicPublishOptions::default(),
            payload,
            BasicProperties::default(),
        )
        .await
        .unwrap()
        .await
        .unwrap();

    context.message_received.notified().await;
    stop_consumer.notify_one();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    context.finish_message_handler.notify_one();

    context.ack_sent.notified().await;
}
