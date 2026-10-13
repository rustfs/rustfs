use crate::backend::Tag;
use crate::error::{PyroscopeError, Result};
use crate::pyroscope::{PyroscopeAgentBuilder, PyroscopeAgentRunning};
use crate::{PyroscopeAgent, ThreadId};
use std::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex,
};

#[derive(Debug, PartialEq, Clone)]
pub enum Signal {
    Kill,
    AddThreadTag(ThreadId, Tag),
    RemoveThreadTag(ThreadId, Tag),
}

const TAG: &str = "pyroscope::ffikit";

static SENDER: Mutex<Option<Sender<Signal>>> = Mutex::new(None);

pub fn run(agent: PyroscopeAgentBuilder) -> Result<()> {
    let mut sender_holder = SENDER.lock()?;
    if (*sender_holder).is_some() {
        return Err(PyroscopeError::new("FFI channel already initialized"));
    }

    let agent = agent.build()?;

    let agent = agent.start()?;

    let (sender, receiver): (Sender<Signal>, Receiver<Signal>) = mpsc::channel();

    *sender_holder = Some(sender);

    std::thread::spawn(move || {
        while let Ok(signal) = receiver.recv() {
            match signal {
                Signal::Kill => {
                    if let Err(err) = stop(agent) {
                        log::error!(target: TAG, "failed to stop agent {err}");
                    }
                    break;
                }
                Signal::AddThreadTag(thread_id, tag) => {
                    if let Err(err) = agent.add_thread_tag(thread_id, tag) {
                        log::error!(target: TAG, "failed to add tag {err}");
                    }
                }
                Signal::RemoveThreadTag(thread_id, tag) => {
                    if let Err(err) = agent.remove_thread_tag(thread_id, tag) {
                        log::error!(target: TAG, "failed to remove tag {err}");
                    }
                }
            }
        }
    });

    Ok(())
}

pub fn send(signal: Signal) -> Result<()> {
    if let Some(sender) = &*SENDER.lock()? {
        sender.send(signal)?;
    } else {
        return Err(PyroscopeError::new("FFI channel not initialized"));
    }
    Ok(())
}

fn stop(agent: PyroscopeAgent<PyroscopeAgentRunning>) -> Result<()> {
    agent.stop()?;
    *SENDER.lock()? = None;
    Ok(())
}
