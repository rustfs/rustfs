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
use dioxus::logger::tracing::debug;
use dioxus::prelude::*;

const NAVBAR_CSS: Asset = asset!("/assets/styling/navbar.css");

#[component]
pub fn Navbar() -> Element {
    rsx! {
        document::Link { rel: "stylesheet", href: NAVBAR_CSS }

        div { id: "navbar", class: "hidden", style: "display: none;",
            Link { to: Route::HomeViews {}, "Home" }
            Link { to: Route::SettingViews {}, "Setting" }
        }

        Outlet::<Route> {}
    }
}

#[derive(Props, PartialEq, Debug, Clone)]
pub struct LoadingSpinnerProps {
    #[props(default = true)]
    loading: bool,
    #[props(default = "Processing...")]
    text: &'static str,
}

#[component]
pub fn LoadingSpinner(props: LoadingSpinnerProps) -> Element {
    debug!("loading: {}", props.loading);
    if !props.loading {
        debug!("LoadingSpinner false loading: {}", props.loading);
        return rsx! {};
    }
    rsx! {
        div { class: "flex items-center justify-center z-10",
            svg {
                class: "animate-spin h-5 w-5 text-blue-500",
                xmlns: "http://www.w3.org/2000/svg",
                fill: "none",
                view_box: "0 0 24 24",
                circle {
                    class: "opacity-25",
                    cx: "12",
                    cy: "12",
                    r: "10",
                    stroke: "currentColor",
                    stroke_width: "4",
                }
                path {
                    class: "opacity-75",
                    fill: "currentColor",
                    d: "M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z",
                }
            }
            span { class: "ml-2 text-gray-600", "{props.text}" }
        }
    }
}
