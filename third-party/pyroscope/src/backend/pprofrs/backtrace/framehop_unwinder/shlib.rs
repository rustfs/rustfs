use std::path::PathBuf;

use findshlibs::{SharedLibrary, TargetSharedLibrary};
use framehop::{Module, ModuleSectionInfo};
use memmap2::Mmap;
use object::{Object, ObjectSection};
use once_cell::sync::Lazy;

static OBJECTS: Lazy<Vec<Module<Vec<u8>>>> = Lazy::new(find_objects);

pub fn get_objects() -> &'static [Module<Vec<u8>>] {
    &OBJECTS
}

pub struct ObjectInfo<'f>(object::File<'f>);

impl<'file, D> ModuleSectionInfo<D> for ObjectInfo<'file>
where
    D: From<&'file [u8]>,
{
    fn base_svma(&self) -> u64 {
        if let Some(section) = self.0.section_by_name("__TEXT") {
            // in mach-o addresses are relative to __TEXT
            section.address()
        } else {
            self.0.relative_address_base()
        }
    }

    fn section_svma_range(&mut self, name: &[u8]) -> Option<std::ops::Range<u64>> {
        if let Some(section) = self.0.section_by_name_bytes(name) {
            let start = section.address();
            let end = start + section.size();
            Some(start..end)
        } else {
            None
        }
    }

    fn section_data(&mut self, name: &[u8]) -> Option<D> {
        if let Some(section) = self.0.section_by_name_bytes(name) {
            let data = section.data().ok()?;
            Some(D::from(data))
        } else {
            None
        }
    }
}

fn open_mmap(path: &PathBuf) -> Option<Mmap> {
    let file = std::fs::File::open(path).ok()?;
    let mmap = unsafe { Mmap::map(&file) }.ok()?;
    Some(mmap)
}

fn find_objects() -> Vec<Module<Vec<u8>>> {
    let mut objects = Vec::new();
    // objects
    TargetSharedLibrary::each(|shlib| {
        let path = PathBuf::from(shlib.name());
        let base_avma = shlib.actual_load_addr().0 as u64;
        let avma_range = base_avma..base_avma + shlib.len() as u64;
        if let Some(mmap) = open_mmap(&path) {
            if let Ok(obj) = object::File::parse(&*mmap) {
                let section_info = ObjectInfo(obj);

                objects.push(Module::new(
                    path.to_string_lossy().to_string(),
                    avma_range,
                    base_avma,
                    section_info,
                ));
            }
        }
    });

    objects
}
