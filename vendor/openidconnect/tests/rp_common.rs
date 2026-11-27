#![allow(clippy::cognitive_complexity, clippy::expect_fun_call)]

use log::{error, warn};
use openidconnect::core::{
    CoreApplicationType, CoreClientRegistrationRequest, CoreClientRegistrationResponse,
    CoreProviderMetadata,
};
use openidconnect::{
    ClientContactEmail, ClientName, HttpClientError, HttpRequest, HttpResponse, IssuerUrl,
    RedirectUrl,
};

use std::cell::RefCell;
use std::sync::Once;
use std::time::Duration;

pub const CERTIFICATION_BASE_URL: &str = "https://rp.certification.openid.net:8080";
pub const RP_CONTACT_EMAIL: &str = "ramos@cs.stanford.edu";
pub const RP_NAME: &str = "openidconnect-rs";
pub const RP_REDIRECT_URI: &str = "http://localhost:8080";

static INIT_LOG: Once = Once::new();

thread_local! {
    static TEST_ID: RefCell<&'static str> = const { RefCell::new("UNINITIALIZED_TEST_ID") };
}

pub fn get_test_id() -> &'static str {
    TEST_ID.with(|id| *id.borrow())
}

pub fn set_test_id(test_id: &'static str) {
    TEST_ID.with(|id| *id.borrow_mut() = test_id);
}

#[macro_export]
macro_rules! log_error {
    ($($args:tt)+) => {
        error!("[{}] {}", rp_common::get_test_id(), format!($($args)+))
    }
}
#[macro_export]
macro_rules! log_info {
    ($($args:tt)+) => {
        info!("[{}] {}", rp_common::get_test_id(), format!($($args)+));
    }
}
#[macro_export]
macro_rules! log_debug {
    ($($args:tt)+) => {
        debug!("[{}] {}", rp_common::get_test_id(), format!($($args)+));
    }
}

#[macro_export]
macro_rules! log_container_field {
    ($container:ident. $field:ident) => {
        log_info!(
            concat!("  ", stringify!($field), " = {:?}"),
            $container.$field()
        );
    };
}

fn _init_log() {
    color_backtrace::install();
    env_logger::init();
}

pub fn init_log(test_id: &'static str) {
    INIT_LOG.call_once(_init_log);
    set_test_id(test_id);
}

// FIXME: just clone `request` directly once we update `http` to 1.0, which implements `Clone`.
#[cfg(feature = "reqwest-blocking")]
pub(crate) fn clone_request(request: &HttpRequest) -> HttpRequest {
    let mut request_copy = http::Request::builder()
        .method(request.method().to_owned())
        .uri(request.uri().to_owned())
        .version(request.version());

    for (name, value) in request.headers() {
        request_copy = request_copy.header(name, value);
    }
    request_copy.body(request.body().to_owned()).unwrap()
}

pub fn http_client(request: HttpRequest) -> Result<HttpResponse, HttpClientError<reqwest::Error>> {
    retry::retry(
        (0..5).map(|i| {
            if i != 0 {
                warn!("Retrying HTTP request ({}/5)", i + 1)
            }
            Duration::from_millis(500)
        }),
        || -> Result<HttpResponse, HttpClientError<reqwest::Error>> {
            #[cfg(feature = "reqwest-blocking")]
            {
                use openidconnect::SyncHttpClient;
                reqwest::blocking::Client::default().call(clone_request(&request))
            }
            #[cfg(not(feature = "reqwest-blocking"))]
            {
                let _ = &request;
                panic!("reqwest-blocking feature is required")
            }
        },
    )
    .map_err(|err| match err {
        retry::Error::Operation { error, .. } => error,
        retry::Error::Internal(msg) => panic!("unexpected error: {msg}"),
    })
}

pub trait PanicIfFail<T, F>
where
    F: std::error::Error,
{
    fn panic_if_fail(self, msg: &'static str) -> T;
}
impl<T, F> PanicIfFail<T, F> for Result<T, F>
where
    F: std::error::Error,
{
    fn panic_if_fail(self, msg: &'static str) -> T {
        match self {
            Ok(ret) => ret,
            Err(fail) => {
                let mut err_msg = format!("Panic: {msg}");

                let mut cur_fail: Option<&dyn std::error::Error> = Some(&fail);
                while let Some(cause) = cur_fail {
                    err_msg += &format!("\n    caused by: {cause}");
                    cur_fail = cause.source();
                }
                error!("[{}] {}", get_test_id(), err_msg);
                panic!("{}", msg);
            }
        }
    }
}

pub fn issuer_url(test_id: &str) -> IssuerUrl {
    IssuerUrl::new(format!("{CERTIFICATION_BASE_URL}/{RP_NAME}/{test_id}"))
        .expect("Failed to parse issuer URL")
}

pub fn get_provider_metadata(test_id: &str) -> CoreProviderMetadata {
    let _issuer_url = issuer_url(test_id);
    CoreProviderMetadata::discover(&_issuer_url, &http_client).expect(&format!(
        "Failed to fetch provider metadata from {_issuer_url:?}"
    ))
}

pub fn register_client<F>(
    provider_metadata: &CoreProviderMetadata,
    request_fn: F,
) -> CoreClientRegistrationResponse
where
    F: FnOnce(CoreClientRegistrationRequest) -> CoreClientRegistrationRequest,
{
    let registration_request_pre = CoreClientRegistrationRequest::new(
        vec![RedirectUrl::new(RP_REDIRECT_URI.to_string()).unwrap()],
        Default::default(),
    )
    .set_application_type(Some(CoreApplicationType::Native))
    .set_client_name(Some(
        vec![(None, ClientName::new(RP_NAME.to_string()))]
            .into_iter()
            .collect(),
    ))
    .set_contacts(Some(vec![ClientContactEmail::new(
        RP_CONTACT_EMAIL.to_string(),
    )]));

    let registration_request_post = request_fn(registration_request_pre);

    let registration_endpoint = provider_metadata
        .registration_endpoint()
        .expect("provider does not support dynamic registration");
    registration_request_post
        .register(registration_endpoint, &http_client)
        .expect(&format!(
            "Failed to register client at {registration_endpoint:?}"
        ))
}
