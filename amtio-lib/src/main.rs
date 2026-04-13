pub fn main() {
    match amtio_lib::size("./*", 8, 512) {
        Ok(sizes) => {
            for size_info in sizes.sizes {
                println!("{}: {}", size_info.name, size_info.size)
            }
        }
        Err(e) => eprintln!("error: {e}"),
    };
}
