use std::{
    io::Write,
    sync::mpsc::{sync_channel, Receiver, SyncSender},
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::encode::gen::push::{PushRequest, RawProfileSeries, RawSample};
use crate::encode::gen::types::LabelPair;
use crate::utils::rustc_version;
use crate::{
    backend::{Report, ReportBatch, ReportData},
    encode::pprof,
    pyroscope::PyroscopeConfig,
    utils::get_time_range,
    Result,
};
use libflate::gzip::Encoder;
use prost::Message;
use reqwest::Url;
use uuid::Uuid;

const LOG_TAG: &str = "Pyroscope::Session";
const LABEL_SCOPE_NAME: &str = "otel.scope.name";
const LABEL_SCOPE_VERSION: &str = "otel.scope.version";
const LABEL_PROCESS_RUNTIME_NAME: &str = "process.runtime.name";
const LABEL_PROCESS_RUNTIME_VERSION: &str = "process.runtime.version";
const LABEL_SERVICE_NAME: &str = "service_name";
const LABEL_PROFILE_NAME: &str = "__name__";
const SCOPE_NAME: &str = "com.grafana.pyroscope/rust";

/// Session Signal
///
/// This enum is used to send data to the session thread. It can also kill the session thread.
pub enum SessionSignal {
    /// Send session data to the session thread.
    Session(Box<Session>),
    /// Kill the session thread.
    Kill,
}

/// Manage sessions and send data to the server.
#[derive(Debug)]
pub struct SessionManager {
    /// The SessionManager thread.
    pub handle: Option<JoinHandle<Result<()>>>,
    /// Channel to send data to the SessionManager thread.
    pub tx: SyncSender<SessionSignal>,
}

impl SessionManager {
    /// Create a new SessionManager
    pub fn new() -> Result<Self> {
        log::info!(target: LOG_TAG, "Creating SessionManager");

        // Create a channel for sending and receiving sessions
        let (tx, rx): (SyncSender<SessionSignal>, Receiver<SessionSignal>) = sync_channel(10);

        // Create a thread for the SessionManager
        let handle = Some(thread::spawn(move || {
            log::trace!(target: LOG_TAG, "Started");
            let client = reqwest::blocking::Client::new();
            while let Ok(signal) = rx.recv() {
                match signal {
                    SessionSignal::Session(session) => {
                        // Send the session
                        // Matching is done here (instead of ?) to avoid breaking
                        // the SessionManager thread if the server is not available.
                        match (*session).push(&client) {
                            Ok(_) => log::trace!("SessionManager - Session sent"),
                            Err(e) => log::error!("SessionManager - Failed to send session: {e}"),
                        }
                    }
                    SessionSignal::Kill => {
                        // Kill the session manager
                        log::trace!(target: LOG_TAG, "Kill signal received");
                        return Ok(());
                    }
                }
            }
            Ok(())
        }));

        Ok(SessionManager { handle, tx })
    }

    /// Push a new session into the SessionManager
    pub fn push(&self, session: SessionSignal) -> Result<()> {
        // Push the session into the SessionManager
        self.tx.send(session)?;

        log::trace!(target: LOG_TAG, "SessionSignal pushed");

        Ok(())
    }
}

pub struct Session {
    pub config: PyroscopeConfig,
    pub batch: ReportBatch,
    // unix time todo remove comment, use types
    pub from: u64,
    // unix time todo remove comment, use types
    pub until: u64,
}

impl Session {
    /// Create a new Session
    /// # Example
    /// ```ignore
    /// let config = PyroscopeConfig::new("https://localhost:8080", "my-app", 100, "pyspy", "0.8.16");
    /// let report = vec![1, 2, 3];
    /// let until = 154065120;
    /// let session = Session::new(until, config, report)?;
    /// ```
    pub fn new(until: u64, config: PyroscopeConfig, batch: ReportBatch) -> Result<Self> {
        log::info!(target: LOG_TAG, "Creating Session");

        // get_time_range should be used with "from". We balance this by reducing
        // one upload interval (one bucket) from the returned range.
        let time_range = get_time_range(until, config.upload_interval)?;
        let bucket = time_range.until - time_range.from;

        Ok(Self {
            config,
            batch,
            from: time_range.from - bucket,
            until: time_range.until - bucket,
        })
    }

    fn push(self, client: &reqwest::blocking::Client) -> Result<()> {
        log::info!(target: LOG_TAG, "Sending Session: {} - {}", self.from, self.until);

        let ReportBatch { profile_type, data } = self.batch;

        let raw_profile = match data {
            ReportData::RawPprof(pprof_bytes) => {
                if self.config.func.is_some() {
                    log::warn!(target: LOG_TAG, "report transform function is not supported with raw pprof backends (e.g. jemalloc)");
                }
                pprof_bytes
            }
            ReportData::Reports(reports) => {
                let transformed: Vec<Report>;
                let encode_input = match self.config.func {
                    None => &reports,
                    Some(f) => {
                        transformed = reports.iter().map(|r| f(r.to_owned())).collect();
                        &transformed
                    }
                };
                pprof::encode(
                    encode_input,
                    self.config.sample_rate,
                    self.from * 1_000_000_000,
                    (self.until - self.from) * 1_000_000_000,
                )
                .encode_to_vec()
            }
        };

        let labels = labels_for_profile(&self.config, profile_type);
        let req = PushRequest {
            series: vec![RawProfileSeries {
                labels,
                samples: vec![RawSample {
                    raw_profile,
                    id: Uuid::new_v4().to_string(),
                }],
            }],
        };

        let req = Self::gzip(&req.encode_to_vec())?;

        let mut url = Url::parse(&self.config.url)?;
        url.path_segments_mut()
            .unwrap()
            .push("push.v1.PusherService")
            .push("Push");

        let mut req_builder = client
            .post(url.as_str())
            .header(
                "User-Agent",
                format!(
                    "pyroscope-rs/{}/{} reqwest",
                    self.config.spy_name, self.config.spy_version
                ),
            )
            .header("Content-Type", "application/proto")
            .header("Content-Encoding", "gzip");

        if let Some(basic_auth) = &self.config.basic_auth {
            req_builder = req_builder.basic_auth(
                basic_auth.username.clone(),
                Some(basic_auth.password.clone()),
            );
        }
        if let Some(tenant_id) = &self.config.tenant_id {
            req_builder = req_builder.header("X-Scope-OrgID", tenant_id);
        }
        for (k, v) in &self.config.http_headers {
            req_builder = req_builder.header(k, v);
        }

        let mut response = req_builder
            .body(req)
            .timeout(Duration::from_secs(10))
            .send()?;

        let status = response.status();

        if status.is_success() {
            let mut sink = std::io::sink();
            _ = response.copy_to(&mut sink);
        } else {
            let resp = response.text();
            let resp = match &resp {
                Ok(t) => t,
                Err(_) => "",
            };
            log::error!(target: LOG_TAG, "Sending Session failed {} {}", status.as_u16(), resp);
        }
        Ok(())
    }

    fn gzip(report: &[u8]) -> Result<Vec<u8>> {
        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(report)?;
        let compressed_data = encoder.finish().into_result()?;
        Ok(compressed_data)
    }
}

fn labels_for_profile(config: &PyroscopeConfig, profile_type: String) -> Vec<LabelPair> {
    let mut labels: Vec<LabelPair> = Vec::with_capacity(6 + config.tags.len());
    labels.push(LabelPair {
        name: LABEL_PROFILE_NAME.to_string(),
        value: profile_type,
    });
    for (k, v) in &config.tags {
        if k == LABEL_PROFILE_NAME {
            continue;
        }
        labels.push(LabelPair {
            name: k.clone(),
            value: v.clone(),
        })
    }
    push_label_if_absent(&mut labels, LABEL_SERVICE_NAME, &config.application_name);
    push_label_if_absent(&mut labels, LABEL_SCOPE_NAME, SCOPE_NAME);
    push_label_if_absent(&mut labels, LABEL_SCOPE_VERSION, &config.spy_version);
    push_label_if_absent(&mut labels, LABEL_PROCESS_RUNTIME_NAME, "rust");
    if let Some(rustc_version) = rustc_version() {
        push_label_if_absent(&mut labels, LABEL_PROCESS_RUNTIME_VERSION, rustc_version);
    }

    labels
}

fn push_label_if_absent(labels: &mut Vec<LabelPair>, name: &str, value: &str) {
    if labels.iter().any(|label| label.name == name) {
        return;
    }
    labels.push(LabelPair {
        name: name.to_string(),
        value: value.to_string(),
    });
}
