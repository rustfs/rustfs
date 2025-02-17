use crate::components::Home;
use dioxus::prelude::*;

#[component]
pub fn HomeViews() -> Element {
    rsx! {
        Home {}
    }
}
