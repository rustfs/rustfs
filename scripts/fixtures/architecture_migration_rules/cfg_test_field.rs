struct GuardFixture {
    #[cfg(test)]
    test_only: (),
    #[cfg(test)]
    test_generic: Option<
        usize,
    >,
}

#[cfg(test)]
fn test_only_constructor(
    data_shards: usize,
    parity_shards: usize,
) {
    let _ = Erasure::new_with_options(data_shards, parity_shards, 64, false);
}

fn production_after_test_fields() {
    let _ = Erasure::new(2, 1, 64);
}

fn second_production_path() {
    let _ = Erasure::new_with_options(2, 1, 64, false);
}
