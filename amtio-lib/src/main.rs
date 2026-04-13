static SIZE_PREFS: &[&'static str] = &["B", "KB", "MB", "GB", "TB"];

fn human_size(size: u64) -> String {
    let mut size_pref = 0;
    let mut size_div = size;
    let mut size_rem = 0;
    while size_div >= 1024 {
        (size_div, size_rem) = (size_div % 1024, size_div % 1024);
        size_pref += 1;
    }
    format!(
        "{size_div}.{}{}",
        size_rem * 100 / 1024,
        SIZE_PREFS[size_pref]
    )
}

pub fn main() {
    match amtio_lib::size("./*", 8, 512) {
        Ok(sizes) => {
            for size_info in sizes.sizes {
                println!("{} - {}", human_size(size_info.size), size_info.name)
            }
        }
        Err(e) => eprintln!("error: {e}"),
    };
}
