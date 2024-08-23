use std::{
    io::Result,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::{AsyncWrite, ReadBuf};

use super::*;

pub type Message = Vec<u8>;

#[derive(Debug, Default, Clone)]
pub struct LaggyMockStream {
    pub ready: bool,
    pub delay: u64,
    pub inner_index: usize,
    pub outer_index: usize,
    pub inner: Arc<Mutex<Vec<Message>>>,
    pub outer: Arc<Mutex<Vec<Message>>>,
}

impl Unpin for LaggyMockStream {}

#[derive(Debug, Default, Clone)]
pub struct MockStream {
    pub inner_index: usize,
    pub outer_index: usize,
    pub inner: Arc<Mutex<Vec<Message>>>,
    pub outer: Arc<Mutex<Vec<Message>>>,
}

impl Unpin for MockStream {}

impl AsyncWrite for MockStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        self.outer_index += 1;
        let idx = self.outer_index;
        let this = self.get_mut();
        let mut buffer = this.outer.lock().unwrap();

        if idx > buffer.len() {
            buffer.push(vec![]);
        }
        buffer[idx - 1] = buf.to_vec();
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for MockStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        self.inner_index += 1;
        let idx = self.inner_index;
        let inner = self.inner.lock().unwrap();
        let amt = std::cmp::min(inner[idx - 1].len(), buf.remaining());
        buf.put_slice(&inner.get(idx - 1).unwrap_or(&vec![])[..amt]);
        // inner.drain(..amt);
        Poll::Ready(Ok(()))
    }
}

impl Addy for MockStream {
    fn addr(&self) -> io::Result<SocketAddr> {
        Ok("127.0.0.1:0001".parse().unwrap())
    }
}

impl AsyncWrite for LaggyMockStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        let delay = self.delay;
        let waker = cx.waker().clone();

        if !self.ready {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                waker.wake();
            });
            self.ready = true;
            return Poll::Pending;
        }
        self.ready = false;

        self.outer_index += 1;
        let idx = self.outer_index;
        let this = self.get_mut();
        let mut buffer = this.outer.lock().unwrap();

        if idx > buffer.len() {
            buffer.push(vec![]);
        }
        buffer[idx - 1] = buf.to_vec();
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for LaggyMockStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let delay = self.delay;
        let waker = cx.waker().clone();

        if !self.ready {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                waker.wake();
            });
            self.ready = true;
            return Poll::Pending;
        }
        self.ready = false;

        self.inner_index += 1;
        let idx = self.inner_index;
        let inner = self.inner.lock().unwrap();
        let amt = std::cmp::min(inner[idx - 1].len(), buf.remaining());
        buf.put_slice(&inner.get(idx - 1).unwrap_or(&vec![])[..amt]);
        // inner.drain(..amt);
        Poll::Ready(Ok(()))
    }
}

impl Addy for LaggyMockStream {
    fn addr(&self) -> io::Result<SocketAddr> {
        Ok("127.0.0.1:0001".parse().unwrap())
    }
}
