use std::future::Future;

use puppet::Executor;

mod aio_writer;
pub mod messages;
mod writer;

pub use aio_writer::AioDirectoryStreamWriter;
pub use writer::DirectoryStreamWriter;

pub struct ThreadedExecutor;
impl Executor for ThreadedExecutor {
    fn spawn(&self, fut: impl Future<Output = ()> + Send + 'static) {
        std::thread::spawn(move || {
            futures_lite::future::block_on(fut);
        });
    }
}
