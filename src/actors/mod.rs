use std::future::Future;
use puppet::Executor;

mod indexer;
mod merger;
mod exporter;
pub mod messages;
mod writer;

pub use writer::DirectoryStreamWriter;

pub struct ThreadedExecutor;
impl Executor for ThreadedExecutor {
    fn spawn(&self, fut: impl Future<Output=()> + Send + 'static) {
        std::thread::spawn(move || {
            futures_lite::future::block_on(fut);
        });
    }
}