use std::ffi::{CStr, CString};
use std::path::Path;
use std::ptr;
use std::slice;

use anyhow::{Context, Result, anyhow};

mod ffi {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(non_upper_case_globals)]

    include!(concat!(env!("OUT_DIR"), "/db_bindings.rs"));
}

pub struct Database {
    raw: *mut ffi::DB,
}

pub struct Cursor {
    raw: *mut ffi::DBC,
}

impl Database {
    pub fn open(path: &Path, database_name: Option<&str>) -> Result<Self> {
        let mut raw: *mut ffi::DB = ptr::null_mut();
        let file = CString::new(path.to_string_lossy().as_bytes())
            .with_context(|| format!("wallet path contains a null byte: {}", path.display()))?;
        let name = database_name
            .map(CString::new)
            .transpose()
            .context("database name contains a null byte")?;

        unsafe {
            let create_result = ffi::db_create(&mut raw, ptr::null_mut(), 0);
            if create_result != 0 {
                return Err(anyhow!(db_error(create_result)));
            }

            let open_result = ((*raw).open.expect("DB->open should be set"))(
                raw,
                ptr::null_mut(),
                file.as_ptr(),
                name.as_ref().map_or(ptr::null(), |value| value.as_ptr()),
                ffi::DBTYPE_DB_BTREE,
                ffi::DB_RDONLY,
                0,
            );

            if open_result != 0 {
                ((*raw).close.expect("DB->close should be set"))(raw, 0);
                return Err(anyhow!(db_error(open_result)));
            }
        }

        Ok(Self { raw })
    }

    pub fn cursor(&self) -> Result<Cursor> {
        let mut raw: *mut ffi::DBC = ptr::null_mut();

        unsafe {
            let result = ((*self.raw).cursor.expect("DB->cursor should be set"))(
                self.raw,
                ptr::null_mut(),
                &mut raw,
                0,
            );

            if result != 0 {
                return Err(anyhow!(db_error(result)));
            }
        }

        Ok(Cursor { raw })
    }
}

impl Cursor {
    pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut key: ffi::DBT = unsafe { std::mem::zeroed() };
        let mut value: ffi::DBT = unsafe { std::mem::zeroed() };
        key.flags = ffi::DB_DBT_MALLOC;
        value.flags = ffi::DB_DBT_MALLOC;

        let result = unsafe {
            ((*self.raw).c_get.expect("DBC->c_get should be set"))(
                self.raw,
                &mut key,
                &mut value,
                ffi::DB_NEXT,
            )
        };

        match result {
            0 => {
                let key_bytes = copy_dbt(&key);
                let value_bytes = copy_dbt(&value);
                free_dbt(&mut key);
                free_dbt(&mut value);
                Ok(Some((key_bytes, value_bytes)))
            }
            x if x == ffi::DB_NOTFOUND => {
                free_dbt(&mut key);
                free_dbt(&mut value);
                Ok(None)
            }
            error_code => {
                free_dbt(&mut key);
                free_dbt(&mut value);
                Err(anyhow!(db_error(error_code)))
            }
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                ((*self.raw).close.expect("DB->close should be set"))(self.raw, 0);
            }
        }
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                ((*self.raw).c_close.expect("DBC->c_close should be set"))(self.raw);
            }
        }
    }
}

fn copy_dbt(value: &ffi::DBT) -> Vec<u8> {
    if value.data.is_null() || value.size == 0 {
        return Vec::new();
    }

    unsafe { slice::from_raw_parts(value.data.cast::<u8>(), value.size as usize).to_vec() }
}

fn free_dbt(value: &mut ffi::DBT) {
    if !value.data.is_null() {
        unsafe {
            libc::free(value.data);
        }
        value.data = ptr::null_mut();
        value.size = 0;
    }
}

fn db_error(error_code: i32) -> String {
    unsafe {
        CStr::from_ptr(ffi::db_strerror(error_code))
            .to_string_lossy()
            .into_owned()
    }
}
