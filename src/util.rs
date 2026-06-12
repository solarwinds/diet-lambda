use std::{future, io::ErrorKind, net::SocketAddr, task::Poll};

use anyhow::Error;
use axum::serve::Listener;
use bytes::Bytes;
use http_body_util::{BodyExt, combinators::UnsyncBoxBody};
use hyper_tls::HttpsConnector;
use hyper_util::{
    client::legacy::{self, connect::HttpConnector},
    rt::{TokioExecutor, TokioTimer},
};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

pub fn flatten<T, E1, E2>(result: Result<Result<T, E1>, E2>) -> Result<T, Error>
where
    Error: From<E1> + From<E2>,
{
    let ok = result??;
    Ok(ok)
}

pub type Body = UnsyncBoxBody<Bytes, Error>;
pub type Client = legacy::Client<HttpsConnector<HttpConnector>, Body>;

pub fn body<B>(body: B) -> Body
where
    B: hyper::body::Body<Data = Bytes> + Send + 'static,
    Error: From<B::Error>,
{
    Body::new(body.map_err(Error::from))
}

pub fn client() -> Client {
    legacy::Builder::new(TokioExecutor::new())
        .timer(TokioTimer::new())
        .build(HttpsConnector::new())
}

// We listen on both ports for both protocols (HTTP and gRPC),
// which uses less memory on top of being friendlier to typos
pub struct MultiListener<const N: usize> {
    listeners: Box<[TcpListener; N]>,
}

impl<const N: usize> MultiListener<N> {
    pub async fn bind<A: ToSocketAddrs>(addresses: [A; N]) -> Result<Self, Error> {
        let mut listeners = Vec::with_capacity(N);
        for address in addresses {
            let listener = TcpListener::bind(address).await?;
            if let Ok(addr) = listener.local_addr() {
                tracing::info!(addr = %addr, "extension listening");
            }
            listeners.push(listener);
        }

        Ok(Self {
            listeners: listeners.try_into().unwrap(),
        })
    }
}

impl<const N: usize> Listener for MultiListener<N> {
    type Io = TcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        future::poll_fn(|cx| {
            for listener in self.listeners.iter_mut() {
                match listener.poll_accept(cx) {
                    Poll::Ready(Ok((stream, addr))) => return Poll::Ready((stream, addr)),
                    Poll::Ready(Err(..)) => continue,
                    Poll::Pending => continue,
                }
            }

            Poll::Pending
        })
        .await
    }

    fn local_addr(&self) -> Result<Self::Addr, std::io::Error> {
        Err(std::io::Error::from(ErrorKind::Unsupported))
    }
}
