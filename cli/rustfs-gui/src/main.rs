use components::Navbar;
use dioxus::logger::tracing::debug;
use dioxus::prelude::*;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
use views::HomeViews;

mod components;
mod utils;
mod views;

fn main() {
    init_logger();
    dioxus::launch(App);
}

#[derive(Debug, Clone, Routable, PartialEq)]
#[rustfmt::skip]
enum Route {
    #[layout(Navbar)]
    #[route("/")]
    HomeViews {},
}

const FAVICON: Asset = asset!("/assets/favicon.ico");
const TAILWIND_CSS: Asset = asset!("/assets/tailwind.css");

#[component]
fn App() -> Element {
    // Build cool things ✌️
    use document::{Link, Title};
    debug!("App rendered");
    rsx! {
        // Global app resources
        Link { rel: "icon", href: FAVICON }
        Link { rel: "stylesheet", href: TAILWIND_CSS }
        // Script { src: "https://cdn.tailwindcss.com" }
        Title { "RustFS" }
        Router::<Route> {}
    }
}

fn init_logger() {
    // configuring rolling logs rolling by day
    let home_dir = dirs::home_dir().expect("无法获取用户目录");
    let rustfs_dir = home_dir.join("rustfs");
    let logs_dir = rustfs_dir.join("logs");
    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY) // rotate log files once every hour
        .filename_prefix("rustfs-cli") // log file names will be prefixed with `myapp.`
        .filename_suffix("log") // log file names will be suffixed with `.log`
        .build(logs_dir) // try to build an appender that stores log files in `/ var/ log`
        .expect("initializing rolling file appender failed");
    // non-blocking writer for improved performance
    let (non_blocking_file, _guard) = tracing_appender::non_blocking(file_appender);

    // console output layer
    let console_layer = fmt::layer().with_writer(std::io::stdout).with_ansi(true); // enable colors in the console

    // file output layer
    let file_layer = fmt::layer().with_writer(non_blocking_file).with_ansi(false); // disable colors in the file

    // Combine all tiers and initialize global subscribers
    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .with(tracing_subscriber::EnvFilter::new("info")) // filter the log level by environment variables
        .init();
}
