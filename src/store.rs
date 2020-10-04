use std::{
    io::{self, Read, Write, Seek, SeekFrom},
    fs::{OpenOptions, File},
    path::Path,
};
use bytes::{Bytes, Buf, BytesMut, BufMut};


pub const PAGE_SIZE: usize = 4096;
const MAGIC_HEADER: &[u8] = b"%store%";


pub struct MemPage {
    pub page_nr: usize,
    pub data: Bytes,
}


pub struct Pages {
    fh: Option<File>,
}


impl Pages {

    fn new() -> Self {
        Pages { fh: None }
    }

    fn is_open(&self) -> bool {
        match self.fh {
            Some(_) => true,
            None => false,
        }
    }

    fn open(&mut self, file_path: &str) -> io::Result<bool> {
        if !self.is_open() {
            let is_new_file = !Path::new(file_path).exists();
            let fh = OpenOptions::new().read(true).write(true).create(is_new_file).open(file_path)?;
            self.fh = Some(fh);
            return Ok(is_new_file);
        }
        Ok(false)
    }

    fn close(&mut self) {
        self.fh = None;  // Does this close the file???
    }

    fn write(&mut self, page: &MemPage) -> io::Result<()> {
        let fh = self.fh.as_mut().unwrap();
        let offset = (PAGE_SIZE * page.page_nr) as u64;
        fh.seek(SeekFrom::Start(offset))?;
        fh.write_all(page.data.as_ref())
    }

    fn read(&mut self, page_nr: usize) -> io::Result<MemPage> {
        let fh = self.fh.as_mut().unwrap();
        let offset = (PAGE_SIZE * page_nr) as u64;
        fh.seek(SeekFrom::Start(offset))?;
        let mut buf = BytesMut::with_capacity(PAGE_SIZE);
        fh.read(&mut buf)?;
        Ok(MemPage { page_nr, data: Bytes::from(buf) })
    }

}


struct Header {
    page_count: usize,
    root_page_nr: usize,
    leaf_count: usize,
}


impl Header {

    fn new() -> Header {
        Header { page_count: 0, root_page_nr: 0, leaf_count: 0 }
    }

    fn serialize(&self) -> MemPage {
        let mut buf = BytesMut::with_capacity(PAGE_SIZE);
        buf.put(MAGIC_HEADER);
        buf.put_u64(self.page_count as u64);
        buf.put_u64(self.root_page_nr as u64);
        buf.put_u64(self.leaf_count as u64);
        for _ in 0..buf.remaining_mut() {
            buf.put_u8(0);
        }
        MemPage { page_nr: 0, data: buf.freeze() }
    }

    fn deserialize(&mut self, page: &mut MemPage) -> io::Result<()> {
        let mut magic_header = [0 as u8; 7];
        page.data.copy_to_slice(magic_header.as_mut());
        if magic_header != MAGIC_HEADER {
            panic!("Not a store file");
        }
        self.page_count = page.data.get_u64() as usize;
        self.root_page_nr = page.data.get_u64() as usize;
        self.leaf_count = page.data.get_u64() as usize;
        Ok(())
    }

}


pub struct Store {
    file_path: String,
    pages: Pages,
    header: Header,
}


impl Store {

    pub fn new(file_name: &str) -> Self {
        Store {
            file_path: file_name.to_string(),
            pages: Pages::new(),
            header: Header::new(),
        }
    }

    pub fn open(&mut self) -> io::Result<()> {
        let is_new_file = self.pages.open(&self.file_path)?;
        match is_new_file {
            false => self.read_header(),
            true =>self.write_header(),
        }
    }

    pub fn close(&mut self) {
        self.pages.close()
    }

    pub fn is_empty(&self) -> bool {
        self.header.leaf_count == 0
    }

    pub fn root_page_nr(&self) -> usize {
        self.header.root_page_nr
    }

    pub fn set_root_page_nr(&mut self, root_page_nr: usize) {
        self.header.root_page_nr = root_page_nr
    }

    fn write_header(&mut self) -> io::Result<()> {
        self.write_page(&self.header.serialize())
    }

    fn read_header(&mut self) -> io::Result<()> {
        let mut page = self.read_page(0)?;
        self.header.deserialize(&mut page)
    }

    pub fn write_page(&mut self, page: &MemPage) -> io::Result<()> {
        assert!(page.page_nr <= self.header.page_count);
        self.pages.write(page)
    }

    pub fn read_page(&mut self, page_nr: usize) -> io::Result<MemPage> {
        assert!(page_nr <= self.header.page_count);
        self.pages.read(page_nr)
    }

    pub fn next_page(&mut self) -> io::Result<usize> {
        let next_page_nr = self.header.page_count;
        self.header.page_count += 1;
        self.write_header()?;
        Ok(next_page_nr)
    }

}
