use std::future::Future;
use puppet::Executor;

pub mod messages;
mod writer;
mod aio_writer;

pub use writer::DirectoryStreamWriter;
pub use aio_writer::AioDirectoryStreamWriter;

pub struct ThreadedExecutor;
impl Executor for ThreadedExecutor {
    fn spawn(&self, fut: impl Future<Output=()> + Send + 'static) {
        std::thread::spawn(move || {
            futures_lite::future::block_on(fut);
        });
    }
}