use glob::glob;
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
pub struct SizeInfo {
    pub name: String,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct SizeRes {
    pub sizes: Vec<SizeInfo>,
}

fn wrap_io_err(e: std::io::Error, path: &str) -> std::io::Error {
    std::io::Error::new(e.kind(), format!("io error at path {path}: {e}"))
}

async fn do_work(path: String) -> std::io::Result<(u64, Vec<String>)> {
    let meta = tokio::fs::metadata(&path)
        .await
        .map_err(|e| wrap_io_err(e, &path))?;
    if meta.is_dir() {
        let mut dir_reader = tokio::fs::read_dir(&path)
            .await
            .map_err(|e| wrap_io_err(e, &path))?;
        let mut entries = vec![];
        loop {
            let e_res = dir_reader
                .next_entry()
                .await
                .map_err(|e| wrap_io_err(e, &path))?;
            match e_res {
                Some(x) => {
                    let e_str = x.path().to_string_lossy().to_string();
                    entries.push(e_str);
                }
                None => break,
            }
        }
        Ok((meta.len(), entries))
    } else {
        Ok((meta.len(), vec![]))
    }
}

async fn size_inner(path: &str) -> std::io::Result<u64> {
    let mut total_size = 0;
    let mut work = vec![path.to_string()];
    while !work.is_empty() {
        let mut js = JoinSet::new();
        for p in work.drain(..) {
            js.spawn(do_work(p));
        }
        loop {
            let Some(res) = js.join_next().await else {
                break;
            };
            let res = match res {
                Ok(r) => r,
                Err(e) => {
                    log::warn!("async task wait failed: {e}");
                    continue;
                }
            };
            match res {
                Ok((size, new_work)) => {
                    total_size += size;
                    work.extend(new_work);
                }
                Err(e) => {
                    log::warn!("getting size of entry failed: {e}")
                }
            }
        }
    }
    Ok(total_size)
}

pub fn size(path: &str, threads: usize, tasks: usize) -> std::io::Result<SizeRes> {
    let globbed_paths: Vec<_> = glob(path)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.msg))?
        .into_iter()
        .filter_map(|x| x.ok())
        .collect();
    let thread_pool = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .max_blocking_threads(tasks)
        .build()
    {
        Ok(tp) => tp,
        Err(e) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                format!("tokio threadpool creation failed: {e}"),
            ));
        }
    };
    let mut out = SizeRes { sizes: vec![] };
    for p in globbed_paths.into_iter() {
        let res = thread_pool.block_on(size_inner(p.to_string_lossy().as_ref()));
        match res {
            Ok(size) => out.sizes.push(SizeInfo {
                name: p.to_string_lossy().to_string(),
                size,
            }),
            Err(e) => log::warn!("size calculation of {:?} failed: {e}", &p),
        }
    }
    Ok(out)
}
