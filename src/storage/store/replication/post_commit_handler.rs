use std::sync::Arc;

use tokio::select;
use tracing::error;

use crate::storage::store::{engine::PostCommitMessage, replication::replicator::Replicator};

struct PostCommitActor {
    replicator: Arc<Replicator>,
}

impl PostCommitActor {
    pub fn new(replicator: Arc<Replicator>) -> Self {
        Self { replicator }
    }

    pub async fn run(&self, mut receive: tokio::sync::mpsc::Receiver<PostCommitMessage>) {
        loop {
            select! {
                Some(msg) = receive.recv() => {
                    if let Err(e) = self.replicator.handle_post_commit_message(&msg) {
                        error!("Failed to handle post commit message: {}", e);
                    }

                    if let Err(e) = msg.channel.send(true) {
                        error!("Failed to send post commit response: {}", e);
                    }
                }
                else => break, // Exit loop if the channel is closed
            }
        }
    }
}

pub struct PostCommitHandler {
    pub sender: tokio::sync::mpsc::Sender<PostCommitMessage>,
    handle: tokio::task::JoinHandle<()>,
}

impl PostCommitHandler {
    pub fn spawn(replicator: Arc<Replicator>) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let actor = PostCommitActor::new(replicator);
        let handle = tokio::spawn(async move {
            actor.run(receiver).await;
        });
        Self { sender, handle }
    }

    fn stop(&self) {
        self.handle.abort(); // Abort the actor task
    }
}

impl Drop for PostCommitHandler {
    fn drop(&mut self) {
        self.stop();
    }
}
