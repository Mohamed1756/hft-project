use log::info;
use hyperliquid_rust_sdk::{BaseUrl, InfoClient, Message, Subscription, L2Book};
use tokio::{
    spawn,
    sync::mpsc::{unbounded_channel, UnboundedSender},
    time::{sleep, Duration},
    task::JoinHandle,
};
use std::time::Instant;

// Number of parallel workers
const WORKER_COUNT: usize = 4;

struct Worker {
    sender: UnboundedSender<L2Book>,
    handle: JoinHandle<()>,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // Create worker pool
    let workers: Vec<Worker> = (0..WORKER_COUNT)
        .map(|id| {
            let (tx, mut rx) = unbounded_channel();
            let handle = spawn(async move {
                while let Some(l2_book) = rx.recv().await {
                    let start = Instant::now();
                    process_update(&l2_book);
                    println!("Worker {}: {:?}", id, start.elapsed());
                }
            });
            Worker {
                sender: tx,
                handle,
            }
        })
        .collect();

    let mut info_client = InfoClient::new(None, Some(BaseUrl::Testnet)).await.unwrap();
    let (sender, mut receiver) = unbounded_channel();
    let subscription_id = info_client
        .subscribe(
            Subscription::L2Book {
                coin: "ETH".to_string(),
            },
            sender,
        )
        .await
        .unwrap();

    spawn(async move {
        sleep(Duration::from_secs(30)).await;
        info!("Unsubscribing from l2 book data");
        info_client.unsubscribe(subscription_id).await.unwrap()
    });

    let mut worker_index = 0;
    let start_time = Instant::now();

    while let Some(Message::L2Book(l2_book)) = receiver.recv().await {
        // Distribute work round-robin to workers
        workers[worker_index].sender.send(l2_book).unwrap();
        worker_index = (worker_index + 1) % WORKER_COUNT;
    }

    println!("Total run time: {:?}", start_time.elapsed());

    // Wait for all workers to complete
    for worker in workers {
        worker.handle.await.unwrap();
    }
}

fn process_update(l2_book: &L2Book) {
    info!("Processed order book update: {:?}", l2_book);
}