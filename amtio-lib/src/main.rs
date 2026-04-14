use argh::FromArgs;

static SIZE_PREFS: &[&'static str] = &["B", "KB", "MB", "GB", "TB"];

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

#[derive(Debug, FromArgs)]
/// Compute the size of files/folders. Glob/Wildcard strings are supported
pub struct SizeArgs {
    #[argh(positional)]
    input_path: String,
    /// number of workers(OS threads) to use
    #[argh(option)]
    workers: Option<usize>,
    /// number of async threads to use
    #[argh(option)]
    threads: Option<usize>,
}

pub fn main() {
    let args: SizeArgs = argh::from_env();

    let globbed_paths: Vec<_> = match glob::glob(&args.input_path) {
        Ok(glob_res) => glob_res.into_iter().filter_map(|x| x.ok()).collect(),
        Err(e) => {
            eprintln!("globbing {} failed: {e}", &args.input_path);
            std::process::exit(1);
        }
    };
    let mut thread_pool = tokio::runtime::Builder::new_multi_thread();
    if let Some(workers) = args.workers {
        thread_pool.worker_threads(workers);
    }
    if let Some(threads) = args.threads {
        thread_pool.max_blocking_threads(threads);
    }
    let thread_pool = match thread_pool.build() {
        Ok(tp) => tp,
        Err(e) => {
            eprintln!("async runtime init failed: {e}");
            std::process::exit(1);
        }
    };
    for path in globbed_paths {
        match thread_pool.block_on(amtio_lib::size(&path.to_string_lossy())) {
            Ok(size) => println!("{} - {}", human_size(size), path.to_string_lossy()),
            Err(e) => eprintln!("finding size of {} failed: {e}", path.to_string_lossy()),
        };
    }
}
