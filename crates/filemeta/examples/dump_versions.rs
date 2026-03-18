use rustfs_filemeta::FileMeta;
use std::{env, fs, path::PathBuf};

fn main() {
    let path = env::args()
        .nth(1)
        .map(PathBuf::from)
        .expect("usage: dump_versions <xl.meta path>");

    let data = fs::read(&path).expect("read xl.meta");
    let meta = FileMeta::load(&data).expect("load xl.meta");
    let versions = meta
        .into_file_info_versions("debug-bucket", "debug-object", true)
        .expect("decode versions");

    println!("path: {}", path.display());
    println!("versions: {}", versions.versions.len());
    for (idx, version) in versions.versions.iter().enumerate() {
        println!(
            "#{idx}: version_id={:?} deleted={} mark_deleted={} is_latest={} size={} mod_time={:?}",
            version.version_id, version.deleted, version.mark_deleted, version.is_latest, version.size, version.mod_time
        );
    }
}
