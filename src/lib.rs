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

fn wrap_join_err(e: tokio::task::JoinError, ctx: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        format!("async wait failed. ctx: {:?}: {e}", ctx),
    )
}

async fn list_dir(path: std::path::PathBuf) -> std::io::Result<Vec<std::fs::DirEntry>> {
    tokio::task::spawn_blocking(move || {
        let dir_reader = std::fs::read_dir(&path).map_err(|e| wrap_io_err(e, &path))?;
        let mut entries = vec![];
        for ls_res in dir_reader {
            let e_res = ls_res.map_err(|e| wrap_io_err(e, &path))?;
            entries.push(e_res);
        }
        Ok(entries)
    })
    .await
    .map_err(|e| wrap_join_err(e, "while listing"))?
}

async fn get_metadata(path: std::path::PathBuf) -> std::io::Result<std::fs::Metadata> {
    tokio::task::spawn_blocking(move || path.metadata().map_err(|e| wrap_io_err(e, &path)))
        .await
        .map_err(|e| wrap_join_err(e, "while getting metadata"))?
}

async fn get_metadata_de(de: std::fs::DirEntry) -> std::io::Result<std::fs::Metadata> {
    tokio::task::spawn_blocking(move || de.metadata().map_err(|e| wrap_io_err(e, &de.path())))
        .await
        .map_err(|e| wrap_join_err(e, "while getting metadata"))?
}

async fn rmdir(path: std::path::PathBuf) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || {
        std::fs::remove_dir(&path).map_err(|e| wrap_io_err(e, &path))
    })
    .await
    .map_err(|e| wrap_join_err(e, "while removing"))?
}

async fn rmfile(path: std::path::PathBuf) -> std::io::Result<()> {
    tokio::task::spawn_blocking(move || {
        std::fs::remove_file(&path).map_err(|e| wrap_io_err(e, &path))
    })
    .await
    .map_err(|e| wrap_join_err(e, "while removing"))?
}

async fn do_size_work(
    path: std::path::PathBuf,
    meta: std::fs::Metadata,
) -> std::io::Result<(u64, Vec<std::fs::DirEntry>)> {
    if meta.is_dir() && !meta.is_symlink() {
        let entries = list_dir(path).await?;
        let mut work = Vec::with_capacity(entries.len());
        for e_res in entries {
            work.push(e_res);
        }
        Ok((meta.len(), work))
    } else {
        Ok((meta.len(), vec![]))
    }
}

pub async fn size(
    path: &str,
    tracker: std::sync::Arc<std::sync::atomic::AtomicU64>,
) -> std::io::Result<u64> {
    let mut total_size = 0;
    let path_pb = std::path::PathBuf::from(path);
    let mut js = tokio::task::JoinSet::new();
    js.spawn(async move {
        let path_meta = get_metadata(path_pb.clone()).await?;
        do_size_work(path_pb, path_meta).await
    });
    while let Some(join_res) = js.join_next().await {
        match join_res {
            Ok(task_res) => match task_res {
                Ok((size, work)) => {
                    total_size += size;
                    tracker.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
                    for dir_entry in work {
                        js.spawn(async move {
                            let e_path = dir_entry.path();
                            let e_meta = get_metadata_de(dir_entry).await?;
                            do_size_work(e_path, e_meta).await
                        });
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
    src_meta: std::fs::Metadata,
    dst: std::path::PathBuf,
    chunk_size: u64,
) -> std::io::Result<()> {
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
        let copy_res = join_res.map_err(|e| {
            wrap_join_err(
                e,
                &format!("while copying data from {:?} to {:?}", &src, &dst),
            )
        })?;

        if let Err(e) = copy_res {
            join_set.abort_all();
            return Err(e);
        }
    }
    Ok(())
}

async fn do_copy_work(
    src: std::path::PathBuf,
    src_meta: std::fs::Metadata,
    dst: std::path::PathBuf,
    chunk_size: u64,
) -> std::io::Result<Vec<(std::fs::Metadata, std::path::PathBuf, std::path::PathBuf)>> {
    if src_meta.is_dir() && !src_meta.is_symlink() {
        std::fs::create_dir_all(&dst).map_err(|e| wrap_io_err(e, &src))?;
        let entries = list_dir(src).await?;
        let mut work = Vec::with_capacity(entries.len());

        for entry in entries {
            let e_path = entry.path();
            let e_base_name = entry.file_name();
            let e_dst_path = dst.join(e_base_name);
            let e_meta = get_metadata_de(entry).await?;
            work.push((e_meta, e_path, e_dst_path));
        }
        Ok(work)
    } else {
        copy_file(src, src_meta, dst, chunk_size).await?;
        Ok(vec![])
    }
}

pub async fn copy(src: &str, dst: &str, chunk_size: u64) -> std::io::Result<()> {
    let src_path = std::path::Path::new(&src);
    let src_meta = get_metadata(src_path.to_path_buf()).await?;
    let dst_pathbuf = if dst.ends_with("/") {
        let src_bname = match src_path.file_name() {
            Some(bname_oss) => bname_oss.to_string_lossy().to_string(),
            None => {
                eprintln!("cant find basename of {:?}", src_path);
                std::process::exit(1);
            }
        };
        std::path::Path::new(&dst).join(src_bname)
    } else {
        std::path::PathBuf::from(&dst)
    };
    let mut js = tokio::task::JoinSet::new();
    js.spawn(do_copy_work(
        src_path.to_path_buf(),
        src_meta,
        dst_pathbuf,
        chunk_size,
    ));
    while let Some(join_res) = js.join_next().await {
        match join_res {
            Ok(task_res) => match task_res {
                Ok(work) => {
                    for (w_src_meta, w_src, w_dst) in work {
                        js.spawn(do_copy_work(w_src, w_src_meta, w_dst, chunk_size));
                    }
                }
                Err(e) => log::warn!("copying entry failed: {e}"),
            },
            Err(e) => log::warn!("async wait while copying entry failed: {e}"),
        }
    }
    Ok(())
}

fn do_remove_work(
    path: std::path::PathBuf,
    meta: std::fs::Metadata,
) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> {
    Box::pin(async move {
        if meta.is_dir() && !meta.is_symlink() {
            let entries = list_dir(path.clone()).await?;
            let mut js = tokio::task::JoinSet::new();
            for entry in entries {
                let e_path = entry.path();
                let e_meta = get_metadata_de(entry).await?;
                js.spawn(do_remove_work(e_path, e_meta));
            }

            let mut child_delete_error = false;

            while let Some(join_res) = js.join_next().await {
                let task_res = join_res
                    .map_err(|e| wrap_join_err(e, &format!("while deleting data {:?}", &path)))?;

                if let Err(e) = task_res {
                    log::warn!("deleting entry failed: {e}");
                    if !child_delete_error {
                        child_delete_error = true;
                    }
                }
            }
            if !child_delete_error {
                rmdir(path).await?;
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::DirectoryNotEmpty,
                    format!(
                        "errors while deleting sub-directories of {:?}, cannot proceed",
                        &path
                    ),
                ));
            }
        } else {
            rmfile(path).await?;
        }
        Ok(())
    })
}

pub async fn remove(path: &str) -> std::io::Result<()> {
    let path_pb = std::path::PathBuf::from(path);
    let path_meta = get_metadata(path_pb.clone()).await?;
    do_remove_work(path_pb, path_meta).await?;
    Ok(())
}
