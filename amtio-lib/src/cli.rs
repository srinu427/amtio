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

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum CommandEnum {
    Size(SizeArgs),
    Copy(CopyArgs),
}

#[derive(FromArgs, PartialEq, Debug)]
/// Top-level command.
struct CliArgs {
    #[argh(subcommand)]
    nested: CommandEnum,
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
            let mut thread_pool = tokio::runtime::Builder::new_multi_thread();
            if let Some(workers) = size_args.workers {
                thread_pool.worker_threads(workers);
            }
            if let Some(threads) = size_args.threads {
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
        CommandEnum::Copy(copy_args) => {
            let dst_path = if copy_args.dst.ends_with("/") {
                let src_path = std::path::Path::new(&copy_args.src);
                let src_bname = match src_path.file_name() {
                    Some(bname_oss) => bname_oss.to_string_lossy().to_string(),
                    None => {
                        eprintln!("cant find basename of {:?}", src_path);
                        std::process::exit(1);
                    }
                };
                std::path::Path::new(&copy_args.dst).join(src_bname)
            } else {
                std::path::PathBuf::from(&copy_args.dst)
            };
            let mut thread_pool = tokio::runtime::Builder::new_multi_thread();
            if let Some(workers) = copy_args.workers {
                thread_pool.worker_threads(workers);
            }
            if let Some(threads) = copy_args.threads {
                thread_pool.max_blocking_threads(threads);
            }
            let thread_pool = match thread_pool.build() {
                Ok(tp) => tp,
                Err(e) => {
                    eprintln!("async runtime init failed: {e}");
                    std::process::exit(1);
                }
            };
            if let Err(e) = thread_pool.block_on(amtio_lib::copy(
                std::path::PathBuf::from(&copy_args.src),
                dst_path,
                copy_args.chunk_size.unwrap_or(DEFAULT_COPY_CHUNK_SIZE),
            )) {
                eprintln!("copying {} to {} failed: {e}", copy_args.src, copy_args.dst);
            };
        }
    }
}
