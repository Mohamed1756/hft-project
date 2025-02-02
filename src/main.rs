use log::{error, info, warn};
use hyperliquid_rust_sdk::{BaseUrl, InfoClient, Message, Subscription, L2Book};
use tokio::{
    spawn,
    sync::mpsc::{unbounded_channel, UnboundedSender},
    task::JoinHandle,
    signal,
};
use std::{
    time::Instant,
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
};

const WORKER_COUNT: usize = 2;

struct WorkerStats {
    updates_processed: AtomicUsize,
    total_processing_time_ms: AtomicUsize,
}

struct Worker {
    sender: UnboundedSender<L2Book>,
    handle: JoinHandle<()>,
    stats: Arc<WorkerStats>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let workers: Vec<Worker> = (0..WORKER_COUNT)
        .map(|id| {
            let (tx, mut rx) = unbounded_channel();
            let stats = Arc::new(WorkerStats {
                updates_processed: AtomicUsize::new(0),
                total_processing_time_ms: AtomicUsize::new(0),
            });
            let stats_clone = Arc::clone(&stats);
            
            let handle = spawn(async move {
                while let Some(l2_book) = rx.recv().await {
                    let start = Instant::now();
                    if let Err(e) = process_update(&l2_book) {
                        error!("Worker {}: Processing error: {}", id, e);
                        continue;
                    }
                    let elapsed = start.elapsed().as_nanos() as usize;
                    stats_clone.updates_processed.fetch_add(1, Ordering::Relaxed);
                    stats_clone.total_processing_time_ms.fetch_add(elapsed, Ordering::Relaxed);
                    info!("Worker {}: Processed in {}ns", id, elapsed);
                }
            });
            
            Worker {
                sender: tx,
                handle,
                stats,
            }
        })
        .collect();

    let mut info_client = InfoClient::new(None, Some(BaseUrl::Testnet)).await?;
    let (sender, mut receiver) = unbounded_channel();
    let subscription_id = info_client
        .subscribe(
            Subscription::L2Book {
                coin: "ETH".to_string(),
            },
            sender,
        )
        .await?;

    // Graceful shutdown handler
    let shutdown = spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for ctrl+c");
        info!("Shutting down...");
        info_client.unsubscribe(subscription_id).await.unwrap_or_else(|e| {
            warn!("Error unsubscribing: {}", e);
        });
    });

    let mut worker_index = 0;
    let start_time = Instant::now();

    while let Some(Message::L2Book(l2_book)) = receiver.recv().await {
        workers[worker_index].sender.send(l2_book)?;
        worker_index = (worker_index + 1) % WORKER_COUNT;
    }

    // Print statistics
    for (i, worker) in workers.iter().enumerate() {
        let updates = worker.stats.updates_processed.load(Ordering::Relaxed);
        let total_time = worker.stats.total_processing_time_ms.load(Ordering::Relaxed);
        let avg_time = if updates > 0 { total_time / updates } else { 0 };
        println!("Worker {}: {} updates, {}ns avg processing time", i, updates, avg_time);
    }

    println!("Total run time: {:?}", start_time.elapsed());
    
    // Wait for all workers and shutdown signal
    for worker in workers {
        worker.handle.await?;
    }
    shutdown.await?;

    Ok(())
}

fn process_update(l2_book: &L2Book) -> Result<(), Box<dyn std::error::Error>> {
    info!("Processing order book update: {:?}", l2_book);
    // Add your processing logic here
    Ok(())
}