// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
