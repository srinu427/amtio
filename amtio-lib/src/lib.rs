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

async fn do_size_work(path: String) -> std::io::Result<(u64, Vec<String>)> {
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

pub async fn size(path: &str) -> std::io::Result<u64> {
    let mut total_size = 0;
    let mut work = vec![path.to_string()];
    while !work.is_empty() {
        let mut js = JoinSet::new();
        for p in work.drain(..) {
            js.spawn(do_size_work(p));
        }
        for res in js.join_all().await {
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
