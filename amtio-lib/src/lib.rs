#[derive(Debug, Clone)]
pub struct SizeInfo {
    pub name: String,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct SizeRes {
    pub sizes: Vec<SizeInfo>,
}

fn wrap_io_err(e: std::io::Error, path: &std::path::Path) -> std::io::Error {
    std::io::Error::new(e.kind(), format!("io error at path {path:?}: {e}"))
}

async fn do_size_work(path: std::path::PathBuf) -> std::io::Result<(u64, Vec<std::path::PathBuf>)> {
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
                    entries.push(x.path());
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
    let mut js = tokio::task::JoinSet::new();
    js.spawn(do_size_work(std::path::PathBuf::from(path)));
    loop {
        let Some(join_res) = js.join_next().await else {
            break;
        };
        match join_res {
            Ok(task_res) => match task_res {
                Ok((size, work)) => {
                    total_size += size;
                    for path in work {
                        js.spawn(do_size_work(path));
                    }
                }
                Err(e) => log::warn!("getting size of entry failed: {e}"),
            },
            Err(e) => log::warn!("async wait while getting size of entry failed: {e}"),
        }
    }
    Ok(total_size)
}
