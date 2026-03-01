use slog::{o, Drain, Level, Logger};
use std::{fs::OpenOptions, sync::Mutex};

/// Creates an asynchronous logger which outputs to both the terminal and a specified file_path.
pub fn create_logger(file_path: &str, level: Level) -> Logger {
    let path = std::path::Path::new(file_path);
    let prefix = path.parent().unwrap(); // todo change unwrap
    std::fs::create_dir_all(prefix).unwrap(); // todo change unwrap

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_path)
        .unwrap(); // todo change unwrap

    let term_decorator = slog_term::TermDecorator::new().build();
    let file_decorator = slog_term::PlainSyncDecorator::new(file);

    let term_drain = slog_term::FullFormat::new(term_decorator).build().fuse();
    let file_drain = slog_term::FullFormat::new(file_decorator).build().fuse();

    let drain = slog::Duplicate::new(term_drain, file_drain).fuse();
    let drain = slog::LevelFilter::new(drain, level).fuse();
    let drain = Mutex::new(drain).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    Logger::root(drain, o!())
}
