use crate::route::Route;
use dioxus::logger::tracing::info;
use dioxus::prelude::*;

const FAVICON: Asset = asset!("/assets/favicon.ico");
const TAILWIND_CSS: Asset = asset!("/assets/tailwind.css");

/// The main application component
/// This is the root component of the application
/// It contains the global resources and the router
/// for the application
#[component]
pub fn App() -> Element {
    // Build cool things ✌️
    use document::{Link, Title};
    info!("App rendered");
    rsx! {
        // Global app resources
        Link { rel: "icon", href: FAVICON }
        Link { rel: "stylesheet", href: TAILWIND_CSS }
        Title { "RustFS" }
        Router::<Route> {}
    }
}
