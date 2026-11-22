use quick_cache::{Options, OptionsBuilder, OptionsError};

fn options(capacity: u64) -> Result<Options, OptionsError> {
    OptionsBuilder::new()
        .estimated_items_capacity(10)
        .weight_capacity(capacity)
        .build()
}

#[test]
fn named_builder_error_supports_downstream_result() {
    assert!(options(10).is_ok());
    let error: OptionsError = OptionsBuilder::new().build().unwrap_err();
    assert_eq!(error.to_string(), "weight_capacity is not set");
    let _: &dyn std::error::Error = &error;
}
