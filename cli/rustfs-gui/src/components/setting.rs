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

use crate::components::navbar::LoadingSpinner;
use dioxus::logger::tracing::{debug, error};
use dioxus::prelude::*;

const SETTINGS_JS: Asset = asset!("/assets/js/sts.js");
const TAILWIND_CSS: Asset = asset!("/assets/tailwind.css");
#[component]
pub fn Setting() -> Element {
    use crate::utils::{RustFSConfig, ServiceManager};
    use document::{Meta, Script, Stylesheet, Title};

    #[allow(clippy::redundant_closure)]
    let service = use_signal(|| ServiceManager::new());
    let conf = RustFSConfig::load().unwrap_or_else(|e| {
        error!("load config error: {}", e);
        RustFSConfig::default_config()
    });
    debug!("conf address: {:?}", conf.clone().address);

    let config = use_signal(|| conf.clone());
    let address_state = use_signal(|| conf.address.to_string());
    let mut host_state = use_signal(|| conf.host.to_string());
    let mut port_state = use_signal(|| conf.port.to_string());
    let mut access_key_state = use_signal(|| conf.access_key.to_string());
    let mut secret_key_state = use_signal(|| conf.secret_key.to_string());
    let mut volume_name_state = use_signal(|| conf.volume_name.to_string());
    let loading = use_signal(|| false);

    let save_and_restart = {
        let host_state = host_state;
        let port_state = port_state;
        let access_key_state = access_key_state;
        let secret_key_state = secret_key_state;
        let volume_name_state = volume_name_state;
        let mut loading = loading;
        debug!("save_and_restart access_key:{}", access_key_state.read());
        move |_| {
            // set the loading status
            loading.set(true);
            let mut config = config;
            config.write().address = format!("{}:{}", host_state.read(), port_state.read());
            config.write().host = host_state.read().to_string();
            config.write().port = port_state.read().to_string();
            config.write().access_key = access_key_state.read().to_string();
            config.write().secret_key = secret_key_state.read().to_string();
            config.write().volume_name = volume_name_state.read().to_string();
            // restart service
            let service = service;
            let config = config.read().clone();
            spawn(async move {
                if let Err(e) = service.read().restart(config).await {
                    ServiceManager::show_error(&format!("Failed to send restart command: {e}"));
                }
                // reset the status when you're done
                loading.set(false);
            });
        }
    };

    rsx! {
        Title { "Settings - RustFS App" }
        Meta { name: "description", content: "Settings - RustFS App." }
        // The Stylesheet component inserts a style link into the head of the document
        Stylesheet { href: TAILWIND_CSS }
        Script { src: SETTINGS_JS }
        div { class: "bg-white p-8",
            h1 { class: "text-2xl font-semibold mb-6", "Settings" }
            div { class: "border-b border-gray-200 mb-6",
                nav { class: "flex space-x-8",
                    button {
                        class: "tab-btn px-1 py-4 text-sm font-medium border-b-2 border-black",
                        "data-tab": "service",
                        "onclick": "switchTab('service')",
                        "Service "
                    }
                    button {
                        class: "tab-btn px-1 py-4 text-sm font-medium text-gray-500 hover:text-gray-700",
                        "data-tab": "user",
                        "onclick": "switchTab('user')",
                        "User "
                    }
                    button {
                        class: "tab-btn px-1 py-4 text-sm font-medium text-gray-500 hover:text-gray-700 hidden",
                        "data-tab": "logs",
                        "onclick": "switchTab('logs')",
                        "Logs "
                    }
                }
            }
            div { id: "tabContent",
                div { class: "tab-content", id: "service",
                    div { class: "mb-8",
                        h2 { class: "text-base font-medium mb-2", "Service address" }
                        p { class: "text-gray-600 mb-4",
                            " The service address is the IP address and port number of the service. the default address is "
                            code { class: "bg-gray-100 px-1 py-0.5 rounded", {address_state} }
                            ". "
                        }
                        div { class: "flex space-x-2",
                            input {
                                class: "border rounded px-3 py-2 w-48 focus:outline-none focus:ring-2 focus:ring-blue-500",
                                r#type: "text",
                                value: host_state,
                                oninput: move |evt| host_state.set(evt.value().clone()),
                            }
                            span { class: "flex items-center", ":" }
                            input {
                                class: "border rounded px-3 py-2 w-20 focus:outline-none focus:ring-2 focus:ring-blue-500",
                                r#type: "text",
                                value: port_state,
                                oninput: move |evt| port_state.set(evt.value().clone()),
                            }
                        }
                    }
                    div { class: "mb-8",
                        h2 { class: "text-base font-medium mb-2", "Storage path" }
                        p { class: "text-gray-600 mb-4",
                            "Update the storage path of the service. the default path is {volume_name_state}."
                        }
                        input {
                            class: "border rounded px-3 py-2 w-full focus:outline-none focus:ring-2 focus:ring-blue-500",
                            r#type: "text",
                            value: volume_name_state,
                            oninput: move |evt| volume_name_state.set(evt.value().clone()),
                        }
                    }
                }
                div { class: "tab-content hidden", id: "user",
                    div { class: "mb-8",
                        h2 { class: "text-base font-medium mb-2", "User" }
                        p { class: "text-gray-600 mb-4",
                            "The user is the owner of the service. the default user is "
                            code { class: "bg-gray-100 px-1 py-0.5 rounded", {access_key_state} }
                        }
                        input {
                            class: "border rounded px-3 py-2 w-full focus:outline-none focus:ring-2 focus:ring-blue-500",
                            r#type: "text",
                            value: access_key_state,
                            oninput: move |evt| access_key_state.set(evt.value().clone()),
                        }
                    }
                    div { class: "mb-8",
                        h2 { class: "text-base font-medium mb-2", "Password" }
                        p { class: "text-gray-600 mb-4",
                            "The password is the password of the user. the default password is "
                            code { class: "bg-gray-100 px-1 py-0.5 rounded", {secret_key_state} }
                        }
                        div { class: "relative",
                            input {
                                class: "border rounded px-3 py-2 w-full pr-10 focus:outline-none focus:ring-2 focus:ring-blue-500",
                                r#type: "password",
                                value: secret_key_state,
                                oninput: move |evt| secret_key_state.set(evt.value().clone()),
                            }
                            button {
                                class: "absolute right-2 top-1/2 transform -translate-y-1/2 text-gray-500 hover:text-gray-700",
                                "onclick": "togglePassword(this)",
                                svg {
                                    class: "h-5 w-5",
                                    fill: "currentColor",
                                    view_box: "0 0 20 20",
                                    xmlns: "http://www.w3.org/2000/svg",
                                    path { d: "M10 12a2 2 0 100-4 2 2 0 000 4z" }
                                    path {
                                        clip_rule: "evenodd",
                                        d: "M.458 10C1.732 5.943 5.522 3 10 3s8.268 2.943 9.542 7c-1.274 4.057-5.064 7-9.542 7S1.732 14.057.458 10zM14 10a4 4 0 11-8 0 4 4 0 018 0z",
                                        fill_rule: "evenodd",
                                    }
                                }
                            }
                        }
                    }
                }
                div { class: "tab-content hidden", id: "logs",
                    div { class: "mb-8",
                        h2 { class: "text-base font-medium mb-2", "Logs storage path" }
                        p { class: "text-gray-600 mb-4",
                            "The logs storage path is the path where the logs are stored. the default path is /var/log/rustfs. "
                        }
                        input {
                            class: "border rounded px-3 py-2 w-full focus:outline-none focus:ring-2 focus:ring-blue-500",
                            r#type: "text",
                            value: "/var/logs/rustfs",
                        }
                    }
                }
            }
            div { class: "flex space-x-4",
                button {
                    class: "bg-[#111827] text-white px-4 py-2 rounded hover:bg-[#1f2937]",
                    onclick: save_and_restart,
                    " Save and restart "
                }
                GoBackButton { "Back" }
            }
            LoadingSpinner {
                loading: loading.read().to_owned(),
                text: "Service processing...",
            }
        }
    }
}
