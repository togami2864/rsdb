use std::{fs, io, path::Path};

pub fn remove_test_file_and_dir<T: AsRef<Path>, U: AsRef<Path>>(
    dirname: T,
    filename: U,
) -> io::Result<()> {
    fs::remove_file(dirname.as_ref().join(filename.as_ref()))?;
    fs::remove_dir(dirname.as_ref())?;
    Ok(())
}
