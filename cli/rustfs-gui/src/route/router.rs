use crate::components::Navbar;
use crate::views::{HomeViews, SettingViews};
use dioxus::prelude::*;

/// The router for the application
#[derive(Debug, Clone, Routable, PartialEq)]
#[rustfmt::skip]
pub enum Route {
    #[layout(Navbar)]
    #[route("/")]
    HomeViews {},
    #[route("/settings")]
    SettingViews {},
}
