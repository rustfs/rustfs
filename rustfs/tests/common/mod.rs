use std::future::Future;

const EMBEDDED_TEST_STACK_SIZE: usize = 8 * 1024 * 1024;

pub fn run_embedded_test<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    let handle = std::thread::Builder::new()
        .name("embedded-test".to_string())
        .stack_size(EMBEDDED_TEST_STACK_SIZE)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build embedded test runtime")
                .block_on(Box::pin(test()));
        })
        .expect("spawn embedded test thread");

    if let Err(payload) = handle.join() {
        std::panic::resume_unwind(payload);
    }
}
