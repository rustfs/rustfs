use crate::components::Setting;
use dioxus::prelude::*;

#[component]
pub fn SettingViews() -> Element {
    rsx! {
        Setting {}
    }
}
