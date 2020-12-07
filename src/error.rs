use std::{
    self,
    io,
    fmt,
    error,
};
use bincode;


pub type Result<T> = std::result::Result<T, Error>;


#[derive(Debug)]
pub enum Error {
    Io(::std::io::Error),
    Serde(bincode::Error),
    KeyNotFound,
    InvalidFileHandle,
    InvalidFileFormat,
}


impl Error {
    pub fn is_key_not_found(&self) -> bool {
        matches!(*self, Error::KeyNotFound)
    }
}


impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => err.fmt(f),
            Error::Serde(ref err) => err.fmt(f),
            Error::KeyNotFound => write!(f, "Key not found"),
            Error::InvalidFileHandle => write!(f, "Programming error: Invalid file handle"),
            Error::InvalidFileFormat => write!(f, "Invalid file format"),
        }
    }
}


impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::Io(ref err) => Some(err),
            Error::Serde(ref err) => Some(err),
            _ => None,
        }
    }
}


impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}


impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Error {
        Error::Serde(err)
    }
}
