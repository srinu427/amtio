use std::io::{Read, Seek, Write};

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
    let meta = std::fs::metadata(&path).map_err(|e| wrap_io_err(e, &path))?;
    if meta.is_dir() {
        let dir_reader = std::fs::read_dir(&path).map_err(|e| wrap_io_err(e, &path))?;
        let mut entries = vec![];
        for ls_res in dir_reader {
            let e_res = ls_res.map_err(|e| wrap_io_err(e, &path))?;
            entries.push(e_res.path());
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
    while let Some(join_res) = js.join_next().await {
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

async fn copy_file_chunk(
    src: std::path::PathBuf,
    dst: std::path::PathBuf,
    offset: u64,
    size: u64,
) -> std::io::Result<()> {
    let mut fr = std::fs::File::open(&src).map_err(|e| wrap_io_err(e, &src))?;
    fr.seek(std::io::SeekFrom::Start(offset))
        .map_err(|e| wrap_io_err(e, &src))?;
    let mut data = vec![0u8; size as _];
    fr.read(&mut data).map_err(|e| wrap_io_err(e, &src))?;
    let mut fw = std::fs::File::options()
        .append(true)
        .open(&dst)
        .map_err(|e| wrap_io_err(e, &dst))?;
    fw.seek(std::io::SeekFrom::Start(offset))
        .map_err(|e| wrap_io_err(e, &dst))?;
    fw.write(&data).map_err(|e| wrap_io_err(e, &dst))?;
    Ok(())
}

async fn copy_file(
    src: std::path::PathBuf,
    dst: std::path::PathBuf,
    chunk_size: u64,
) -> std::io::Result<()> {
    let src_meta = src.metadata().map_err(|e| wrap_io_err(e, &src))?;
    let src_len = src_meta.len();
    let fw = std::fs::File::options()
        .write(true)
        .truncate(true)
        .open(&dst)
        .map_err(|e| wrap_io_err(e, &dst))?;
    fw.set_len(src_len).map_err(|e| wrap_io_err(e, &dst))?;
    if src_len == 0 {
        return Ok(());
    }
    let chunk_count = ((src_len - 1) / chunk_size) + 1;
    let mut join_set = tokio::task::JoinSet::new();
    for i in 0..chunk_count {
        let offset = chunk_size * i;
        let copy_size = if i + 1 == chunk_count {
            src_len - offset
        } else {
            chunk_size
        };
        join_set.spawn(copy_file_chunk(src.clone(), dst.clone(), offset, copy_size));
    }
    while let Some(join_res) = join_set.join_next().await {
        match join_res {
            Ok(copy_res) => {
                if let Err(e) = copy_res {
                    join_set.abort_all();
                    return Err(e);
                }
            }
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    format!(
                        "async wait failed while copying data from {:?} to {:?}: {e}",
                        &src, &dst
                    ),
                ));
            }
        }
    }
    Ok(())
}

pub async fn copy(
    src: std::path::PathBuf,
    dst: std::path::PathBuf,
    chunk_size: u64,
) -> std::io::Result<()> {
    let src_meta = src.metadata().map_err(|e| wrap_io_err(e, &src))?;
    todo!()
}
