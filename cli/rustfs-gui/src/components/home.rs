use crate::components::navbar::LoadingSpinner;
use crate::route::Route;
use crate::utils::{RustFSConfig, ServiceManager};
use chrono::Datelike;
use dioxus::logger::tracing::debug;
use dioxus::prelude::*;
use std::time::Duration;

const HEADER_LOGO: Asset = asset!("/assets/rustfs-logo.svg");
const TAILWIND_CSS: Asset = asset!("/assets/tailwind.css");

/// Define the state of the service
#[derive(PartialEq, Debug, Clone)]
enum ServiceState {
    Start,
    Stop,
}

/// Define the Home component
/// The Home component is the main component of the application
/// It is responsible for starting and stopping the service
/// It also displays the service status and provides a button to toggle the service
/// The Home component also displays the footer of the application
/// The footer contains links to the official site, documentation, GitHub, and license
/// The footer also displays the version of the application
/// The Home component also contains a button to change the theme of the application
/// The Home component also contains a button to go to the settings page
#[component]
pub fn Home() -> Element {
    #[allow(clippy::redundant_closure)]
    let service = use_signal(|| ServiceManager::new());
    let conf = RustFSConfig::load().unwrap_or_else(|e| {
        ServiceManager::show_error(&format!("load config failed: {e}"));
        RustFSConfig::default()
    });

    debug!("loaded configurations: {:?}", conf);
    let config = use_signal(|| conf.clone());

    use dioxus_router::prelude::Link;
    use document::{Meta, Stylesheet, Title};
    let mut service_state = use_signal(|| ServiceState::Start);
    // Create a periodic check on the effect of the service status
    use_effect(move || {
        spawn(async move {
            loop {
                if let Some(pid) = ServiceManager::check_service_status().await {
                    debug!("service_running true pid: {:?}", pid);
                    service_state.set(ServiceState::Stop);
                } else {
                    debug!("service_running true pid: 0");
                    service_state.set(ServiceState::Start);
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    });
    debug!("project start service_state: {:?}", service_state.read());
    // Use 'use_signal' to manage service status
    let mut loading = use_signal(|| false);
    let mut start_service = move |_| {
        let service = service;
        let config = config.read().clone();
        let mut service_state = service_state;
        // set the loading status
        loading.set(true);
        debug!("stop loading_state: {:?}", loading.read());
        spawn(async move {
            match service.read().start(config).await {
                Ok(result) => {
                    if result.success {
                        let duration = result.end_time - result.start_time;
                        debug!("The service starts successfully and takes a long time:{}ms", duration.num_milliseconds());
                        service_state.set(ServiceState::Stop);
                    } else {
                        ServiceManager::show_error(&result.message);
                        service_state.set(ServiceState::Start);
                    }
                }
                Err(e) => {
                    ServiceManager::show_error(&format!("start service failed: {e}"));
                }
            }
            // Only set loading to false when it's actually done
            loading.set(false);
            debug!("start loading_state: {:?}", loading.read());
        });
    };

    let mut stop_service = move |_| {
        let service = service;
        let mut service_state = service_state;
        // set the loading status
        loading.set(true);
        spawn(async move {
            match service.read().stop().await {
                Ok(result) => {
                    if result.success {
                        let duration = result.end_time - result.start_time;
                        debug!("The service stops successfully and takes a long time:{}ms", duration.num_milliseconds());
                        service_state.set(ServiceState::Start);
                    } else {
                        ServiceManager::show_error(&result.message);
                    }
                }
                Err(e) => {
                    ServiceManager::show_error(&format!("stop service failed: {e}"));
                }
            }
            debug!("service_state: {:?}", service_state.read());
            // Only set loading to false when it's actually done
            loading.set(false);
            debug!("stop loading_state: {:?}", loading.read());
        });
    };

    // Toggle the state when the button is clicked
    let toggle_service = {
        let mut service_state = service_state;
        debug!("toggle_service service_state: {:?}", service_state.read());
        move |_| {
            if service_state.read().eq(&ServiceState::Stop) {
                // If the service status is started, you need to run a command to stop the service
                stop_service(());
                service_state.set(ServiceState::Start);
            } else {
                start_service(());
                service_state.set(ServiceState::Stop);
            }
        }
    };

    // Define dynamic styles based on state
    let button_class = if service_state.read().eq(&ServiceState::Start) {
        "bg-[#111827] hover:bg-[#1f2937] text-white px-4 py-2 rounded-md flex items-center space-x-2"
    } else {
        "bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-md flex items-center space-x-2"
    };

    rsx! {
        // The Stylesheet component inserts a style link into the head of the document
        Stylesheet {href: TAILWIND_CSS,}
        Title { "RustFS APP" }
        Meta {
            name: "description",
            // TODO: translate to english
            content: "RustFS RustFS 用热门安全的 Rust 语言开发，兼容 S3 协议。适用于 AI/ML 及海量数据存储、大数据、互联网、工业和保密存储等全部场景。近乎免费使用。遵循 Apache 2 协议，支持国产保密设备和系统。",
        }
        div { class: "min-h-screen flex flex-col items-center bg-white",
            div { class: "absolute top-4 right-6 flex space-x-2",
                // change theme
                button { class: "p-2 hover:bg-gray-100 rounded-lg", ChangeThemeButton {} }
                // setting button
                Link {
                    class: "p-2 hover:bg-gray-100 rounded-lg",
                    to: Route::SettingViews {},
                    SettingButton {}
                }
            }
            main { class: "flex-1 flex flex-col items-center justify-center space-y-6 p-4",
                div { class: "w-24 h-24 bg-gray-900 rounded-full flex items-center justify-center",
                    img { alt: "Logo", class: "w-16 h-16", src: HEADER_LOGO }
                }
                div { class: "text-gray-600",
                    "Service is running on "
                    span { class: "text-blue-600", " 127.0.0.1:9000 " }
                }
                LoadingSpinner {
                    loading: loading.read().to_owned(),
                    text: "processing...",
                }
                button { class: button_class, onclick: toggle_service,
                    svg {
                        class: "h-4 w-4",
                        fill: "none",
                        stroke: "currentColor",
                        view_box: "0 0 24 24",
                        xmlns: "http://www.w3.org/2000/svg",
                        if service_state.read().eq(&ServiceState::Start) {
                            path {
                                d: "M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z",
                                stroke_linecap: "round",
                                stroke_linejoin: "round",
                                stroke_width: "2",
                            }
                            path {
                                d: "M21 12a9 9 0 11-18 0 9 9 0 0118 0z",
                                stroke_linecap: "round",
                                stroke_linejoin: "round",
                                stroke_width: "2",
                            }
                        } else {
                            path {
                                stroke_linecap: "round",
                                stroke_linejoin: "round",
                                stroke_width: "2",
                                d: "M21 12a9 9 0 11-18 0 9 9 0 0118 0z",
                            }
                            path {
                                stroke_linecap: "round",
                                stroke_linejoin: "round",
                                stroke_width: "2",
                                d: "M9 10h6v4H9z",
                            }
                        }
                    }
                    span { id: "serviceStatus",
                        if service_state.read().eq(&ServiceState::Start) {
                            "Start service"
                        } else {
                            "Stop service"
                        }
                    }
                }
            }
            Footer { version: "v1.0.0".to_string() }
        }
    }
}

#[component]
pub fn Footer(version: String) -> Element {
    let now = chrono::Local::now();
    let year = now.naive_local().year();
    rsx! {
        footer { class: "w-full py-6 flex flex-col items-center space-y-4 mb-6",
            nav { class: "flex space-x-4 text-gray-600",
                a { class: "hover:text-gray-900", href: "https://rustfs.com", "Official Site" }
                a {
                    class: "hover:text-gray-900",
                    href: "https://rustfs.com/docs",
                    "Documentation"
                }
                a {
                    class: "hover:text-gray-900",
                    href: "https://github.com/rustfs/rustfs",
                    "GitHub"
                }
                a {
                    class: "hover:text-gray-900",
                    href: "https://rustfs.com/docs/license/",
                    "License"
                }
                a { class: "hover:text-gray-900", href: "#", "Sponsors" }
            }
            div { class: "text-gray-500 text-sm", " © rustfs.com {year}, All rights reserved." }
            div { class: "text-gray-400 text-sm mb-8", " version {version} " }
        }
    }
}

#[component]
pub fn GoBackButtons() -> Element {
    rsx! {
        button {
            class: "p-2 hover:bg-gray-100 rounded-lg",
            "onclick": "window.history.back()",
            "Back to the Past"
        }
    }
}

#[component]
pub fn GoForwardButtons() -> Element {
    rsx! {
        button {
            class: "p-2 hover:bg-gray-100 rounded-lg",
            "onclick": "window.history.forward()",
            "Back to the Future"
        }
    }
}

#[component]
pub fn ChangeThemeButton() -> Element {
    rsx! {
        svg {
            class: "h-6 w-6 text-gray-600",
            fill: "none",
            stroke: "currentColor",
            view_box: "0 0 24 24",
            xmlns: "http://www.w3.org/2000/svg",
            path {
                d: "M9 3v2m6-2v2M9 19v2m6-2v2M5 9H3m2 6H3m18-6h-2m2 6h-2M7 19h10a2 2 0 002-2V7a2 2 0 00-2-2H7a2 2 0 00-2 2v10a2 2 0 002 2zM9 9h6v6H9V9z",
                stroke_linecap: "round",
                stroke_linejoin: "round",
                stroke_width: "2",
            }
        }
    }
}

#[component]
pub fn SettingButton() -> Element {
    rsx! {
        svg {
            class: "h-6 w-6 text-gray-600",
            fill: "none",
            stroke: "currentColor",
            view_box: "0 0 24 24",
            xmlns: "http://www.w3.org/2000/svg",
            path {
                d: "M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z",
                stroke_linecap: "round",
                stroke_linejoin: "round",
                stroke_width: "2",
            }
            path {
                d: "M15 12a3 3 0 11-6 0 3 3 0 016 0z",
                stroke_linecap: "round",
                stroke_linejoin: "round",
                stroke_width: "2",
            }
        }
    }
}
