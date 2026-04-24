use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use argh::FromArgs;

static SIZE_PREFS: &[&'static str] = &["B", "KB", "MB", "GB", "TB"];
static DEFAULT_COPY_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

fn human_size(size: u64) -> String {
    let mut size_pref = 0;
    let mut size_div = size;
    let mut size_rem = 0;
    while size_div >= 1024 {
        (size_div, size_rem) = (size_div / 1024, size_div % 1024);
        size_pref += 1;
    }
    format!(
        "{size_div}.{}{}",
        size_rem * 100 / 1024,
        SIZE_PREFS[size_pref]
    )
}

#[derive(Debug, FromArgs, PartialEq)]
/// Compute the size of files/folders. Glob/Wildcard strings are supported
#[argh(subcommand, name = "size")]
pub struct SizeArgs {
    #[argh(positional)]
    input_path: String,
    /// number of workers(OS threads) to use
    #[argh(option, short = 'w')]
    workers: Option<usize>,
    /// number of async threads to use
    #[argh(option, short = 't')]
    threads: Option<usize>,
}

#[derive(Debug, FromArgs, PartialEq)]
/// Copy files/folders from one place to other
#[argh(subcommand, name = "copy")]
pub struct CopyArgs {
    #[argh(positional)]
    src: String,
    #[argh(positional)]
    dst: String,
    /// chunk size to use when copying. Default: DEFAULT_COPY_CHUNK_SIZE
    #[argh(option, short = 'c')]
    chunk_size: Option<u64>,
    /// number of workers(OS threads) to use
    #[argh(option, short = 'w')]
    workers: Option<usize>,
    /// number of async threads to use
    #[argh(option, short = 't')]
    threads: Option<usize>,
}

#[derive(Debug, FromArgs, PartialEq)]
/// Remove files/folders. Glob/Wildcard strings are supported
#[argh(subcommand, name = "remove")]
pub struct RemoveArgs {
    #[argh(positional)]
    input_path: String,
    /// number of workers(OS threads) to use
    #[argh(option, short = 'w')]
    workers: Option<usize>,
    /// number of async threads to use
    #[argh(option, short = 't')]
    threads: Option<usize>,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum CommandEnum {
    Size(SizeArgs),
    Copy(CopyArgs),
    Remove(RemoveArgs),
}

#[derive(FromArgs, PartialEq, Debug)]
/// Top-level command.
struct CliArgs {
    #[argh(subcommand)]
    nested: CommandEnum,
}

fn init_thread_pool(
    workers: Option<usize>,
    threads: Option<usize>,
) -> Result<tokio::runtime::Runtime, String> {
    let mut thread_pool = tokio::runtime::Builder::new_multi_thread();
    thread_pool.enable_time();
    if let Some(workers) = workers {
        thread_pool.worker_threads(workers);
    }
    if let Some(threads) = threads {
        thread_pool.max_blocking_threads(threads);
    }
    thread_pool
        .build()
        .map_err(|e| format!("async runtime init failed: {e}"))
}

pub fn main() {
    let args: CliArgs = argh::from_env();

    match args.nested {
        CommandEnum::Size(size_args) => {
            let globbed_paths: Vec<_> = match glob::glob(&size_args.input_path) {
                Ok(glob_res) => glob_res.into_iter().filter_map(|x| x.ok()).collect(),
                Err(e) => {
                    eprintln!("globbing {} failed: {e}", size_args.input_path);
                    std::process::exit(1);
                }
            };
            let thread_pool = match init_thread_pool(size_args.workers, size_args.threads) {
                Ok(tp) => tp,
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(1);
                }
            };
            for path in globbed_paths {
                thread_pool.block_on(async move {
                    let curr_size = Arc::new(AtomicU64::new(0));
                    let stop_print = Arc::new(AtomicBool::new(false));
                    let stop_print_pt = stop_print.clone();
                    let curr_size_pt = curr_size.clone();
                    let path_pt = path.clone();
                    let jh = tokio::spawn(async move {
                        loop {
                            let stop = stop_print_pt.load(Ordering::Acquire);
                            if stop {
                                break;
                            }
                            let size = curr_size_pt.load(Ordering::Relaxed);
                            print!(
                                "{} - {} (still computing)\r",
                                human_size(size),
                                path_pt.to_string_lossy()
                            );
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    });
                    match amtio_lib::size(&path.to_string_lossy(), curr_size).await {
                        Ok(size) => {
                            stop_print.store(true, Ordering::Release);
                            let _ = jh.await;
                            println!("");
                            println!("{} - {}", human_size(size), path.to_string_lossy())
                        }
                        Err(e) => {
                            eprintln!("finding size of {} failed: {e}", path.to_string_lossy())
                        }
                    }
                });
            }
        }
        CommandEnum::Copy(copy_args) => {
            let thread_pool = match init_thread_pool(copy_args.workers, copy_args.threads) {
                Ok(tp) => tp,
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(1);
                }
            };
            if let Err(e) = thread_pool.block_on(amtio_lib::copy(
                &copy_args.src,
                &copy_args.dst,
                copy_args.chunk_size.unwrap_or(DEFAULT_COPY_CHUNK_SIZE),
            )) {
                eprintln!("copying {} to {} failed: {e}", copy_args.src, copy_args.dst);
            };
        }
        CommandEnum::Remove(remove_args) => {
            let globbed_paths: Vec<_> = match glob::glob(&remove_args.input_path) {
                Ok(glob_res) => glob_res.into_iter().filter_map(|x| x.ok()).collect(),
                Err(e) => {
                    eprintln!("globbing {} failed: {e}", remove_args.input_path);
                    std::process::exit(1);
                }
            };
            let thread_pool = match init_thread_pool(remove_args.workers, remove_args.threads) {
                Ok(tp) => tp,
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(1);
                }
            };
            for path in globbed_paths {
                if let Err(e) = thread_pool.block_on(amtio_lib::remove(&path.to_string_lossy())) {
                    eprintln!("finding size of {} failed: {e}", path.to_string_lossy());
                };
            }
        }
    }
}
