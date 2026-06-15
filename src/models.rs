use async_native_tls::TlsStream;
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use futures::{StreamExt, TryStreamExt};

pub type DynErr = Box<dyn std::error::Error>;