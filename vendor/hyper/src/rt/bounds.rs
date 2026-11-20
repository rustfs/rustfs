//! Trait aliases.
//!
//! Traits in this module ease setting bounds and are usually automatically
//! implemented by implementing another trait.

#[cfg(all(feature = "client", feature = "http2"))]
pub use self::h2_client::Http2ClientConnExec;
#[cfg(all(feature = "server", feature = "http2"))]
pub use self::h2_server::Http2ServerConnExec;

#[cfg(all(any(feature = "client", feature = "server"), feature = "http2"))]
pub(crate) use self::h2_common::Http2UpgradedExec;

#[cfg(all(any(feature = "client", feature = "server"), feature = "http2"))]
mod h2_common {
    use crate::proto::h2::upgrade::UpgradedSendStreamTask;
    use crate::rt::Executor;

    pub trait Http2UpgradedExec<B> {
        #[doc(hidden)]
        fn execute_upgrade(&self, fut: UpgradedSendStreamTask<B>);
    }

    #[doc(hidden)]
    impl<E, B> Http2UpgradedExec<B> for E
    where
        E: Executor<UpgradedSendStreamTask<B>>,
    {
        fn execute_upgrade(&self, fut: UpgradedSendStreamTask<B>) {
            self.execute(fut);
        }
    }
}

#[cfg(all(feature = "client", feature = "http2"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "client", feature = "http2"))))]
mod h2_client {
    use std::{error::Error, future::Future};

    use crate::rt::{Read, Write};
    use crate::{proto::h2::client::H2ClientFuture, rt::Executor};

    /// An executor to spawn http2 futures for the client.
    ///
    /// This trait is implemented for any type that implements [`Executor`]
    /// trait for any future.
    ///
    /// This trait is sealed and cannot be implemented for types outside this crate.
    ///
    /// [`Executor`]: crate::rt::Executor
    pub trait Http2ClientConnExec<B, T>:
        super::Http2UpgradedExec<B::Data> + sealed_client::Sealed<(B, T)> + Clone
    where
        B: http_body::Body,
        B::Error: Into<Box<dyn Error + Send + Sync>>,
        T: Read + Write + Unpin,
    {
        #[doc(hidden)]
        fn execute_h2_future(&mut self, future: H2ClientFuture<B, T, Self>);
    }

    impl<E, B, T> Http2ClientConnExec<B, T> for E
    where
        E: Clone,
        E: Executor<H2ClientFuture<B, T, E>>,
        E: super::Http2UpgradedExec<B::Data>,
        B: http_body::Body + 'static,
        B::Error: Into<Box<dyn Error + Send + Sync>>,
        H2ClientFuture<B, T, E>: Future<Output = ()>,
        T: Read + Write + Unpin,
    {
        fn execute_h2_future(&mut self, future: H2ClientFuture<B, T, E>) {
            self.execute(future);
        }
    }

    impl<E, B, T> sealed_client::Sealed<(B, T)> for E
    where
        E: Clone,
        E: Executor<H2ClientFuture<B, T, E>>,
        E: super::Http2UpgradedExec<B::Data>,
        B: http_body::Body + 'static,
        B::Error: Into<Box<dyn Error + Send + Sync>>,
        H2ClientFuture<B, T, E>: Future<Output = ()>,
        T: Read + Write + Unpin,
    {
    }

    mod sealed_client {
        pub trait Sealed<X> {}
    }
}

#[cfg(all(feature = "server", feature = "http2"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "server", feature = "http2"))))]
mod h2_server {
    use crate::{proto::h2::server::H2Stream, rt::Executor};
    use http_body::Body;
    use std::future::Future;

    /// An executor to spawn http2 connections.
    ///
    /// This trait is implemented for any type that implements [`Executor`]
    /// trait for any future.
    ///
    /// This trait is sealed and cannot be implemented for types outside this crate.
    ///
    /// [`Executor`]: crate::rt::Executor
    pub trait Http2ServerConnExec<F, B: Body>:
        super::Http2UpgradedExec<B::Data> + sealed::Sealed<(F, B)> + Clone
    {
        #[doc(hidden)]
        fn execute_h2stream(&mut self, fut: H2Stream<F, B, Self>);
    }

    #[doc(hidden)]
    impl<E, F, B> Http2ServerConnExec<F, B> for E
    where
        E: Clone,
        E: Executor<H2Stream<F, B, E>>,
        E: super::Http2UpgradedExec<B::Data>,
        H2Stream<F, B, E>: Future<Output = ()>,
        B: Body,
    {
        fn execute_h2stream(&mut self, fut: H2Stream<F, B, E>) {
            self.execute(fut);
        }
    }

    impl<E, F, B> sealed::Sealed<(F, B)> for E
    where
        E: Clone,
        E: Executor<H2Stream<F, B, E>>,
        E: super::Http2UpgradedExec<B::Data>,
        H2Stream<F, B, E>: Future<Output = ()>,
        B: Body,
    {
    }

    mod sealed {
        pub trait Sealed<T> {}
    }
}
