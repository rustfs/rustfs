mod components;
mod views;

use dioxus::prelude::*;

fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    // The use signal hook runs once when the component is created and then returns the current value every run after the first
    let name = use_signal(|| "RustFS");

    rsx! { "hello {name}!" }
}
