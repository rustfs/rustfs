mod components;
mod route;
mod utils;
mod views;

fn main() {
    let _worker_guard = utils::init_logger();
    dioxus::launch(views::App);
}
