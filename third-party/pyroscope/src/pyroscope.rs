use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{
        mpsc::{self, Sender},
        Arc, Condvar, Mutex,
    },
    thread::JoinHandle,
    time::Duration,
};

use crate::{
    backend::{BackendReady, BackendUninitialized, Report, Tag},
    error::Result,
    session::{Session, SessionManager, SessionSignal},
    timer::{Timer, TimerSignal},
    utils::get_time_range,
    PyroscopeError,
};

use crate::backend::{BackendImpl, ThreadTag};

const LOG_TAG: &str = "Pyroscope::Agent";
const PPROFRS_SPY_NAME: &str = "pyroscope-rs";
const PPROFRS_SPY_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Pyroscope Agent Configuration. This is the configuration that is passed to the agent.
///
/// # Example
/// ```
/// use pyroscope::pyroscope::PyroscopeConfig;
/// let config = PyroscopeConfig::new("http://localhost:8080", "my-app", 100, "pyspy", "0.8.16");
/// ```
#[derive(Clone, Debug)]
pub struct PyroscopeConfig {
    /// Pyroscope Server Address
    pub url: String,
    /// Application Name
    pub application_name: String,
    /// Tags
    pub tags: HashMap<String, String>,
    /// Sample Rate
    pub sample_rate: u32,
    /// Spy Name
    pub spy_name: String,
    /// Spy Version
    pub spy_version: String,
    pub basic_auth: Option<BasicAuth>,
    /// Function to apply
    pub func: Option<fn(Report) -> Report>,
    pub tenant_id: Option<String>,
    pub http_headers: HashMap<String, String>,
    pub upload_interval: Duration,
}

#[derive(Clone, Debug)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

impl Default for PyroscopeConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:4040".to_string(),
            application_name: "undefined".to_string(),
            tags: HashMap::new(),
            sample_rate: 100u32,
            spy_name: PPROFRS_SPY_NAME.to_string(),
            spy_version: PPROFRS_SPY_VERSION.to_string(),
            basic_auth: None,
            func: None,
            tenant_id: None,
            http_headers: HashMap::new(),
            upload_interval: Duration::from_secs(10),
        }
    }
}

impl PyroscopeConfig {
    /// Create a new PyroscopeConfig object.
    ///
    /// # Example
    /// ```
    /// use pyroscope::pyroscope::PyroscopeConfig;
    /// let config = PyroscopeConfig::new("http://localhost:8080", "my-app", 100, "pyspy", "0.8.16");
    /// ```
    pub fn new(
        url: impl AsRef<str>,
        application_name: impl AsRef<str>,
        sample_rate: u32,
        spy_name: impl AsRef<str>,
        spy_version: impl AsRef<str>,
    ) -> Self {
        Self {
            url: url.as_ref().to_owned(),
            application_name: application_name.as_ref().to_owned(),
            tags: HashMap::new(),
            sample_rate,
            spy_name: spy_name.as_ref().to_owned(),
            spy_version: spy_version.as_ref().to_owned(),
            basic_auth: None,
            func: None,
            tenant_id: None,
            http_headers: HashMap::new(),
            upload_interval: Duration::from_secs(10),
        }
    }

    // Set the Pyroscope Server URL
    pub fn url(self, url: impl AsRef<str>) -> Self {
        Self {
            url: url.as_ref().to_owned(),
            ..self
        }
    }

    pub fn basic_auth(self, username: String, password: String) -> Self {
        Self {
            basic_auth: Some(BasicAuth { username, password }),
            ..self
        }
    }

    /// Set the Function.
    pub fn func(self, func: fn(Report) -> Report) -> Self {
        Self {
            func: Some(func),
            ..self
        }
    }

    /// Set the tags.
    ///
    /// # Example
    /// ```
    /// use pyroscope::pyroscope::PyroscopeConfig;
    /// let config = PyroscopeConfig::new("http://localhost:8080", "my-app", 100, "pyroscope-rs", "0.1.0")
    ///    .tags(vec![("env", "dev")]);
    /// ```
    pub fn tags(self, tags: Vec<(&str, &str)>) -> Self {
        // Convert &[(&str, &str)] to HashMap(String, String)
        let tags_hashmap: HashMap<String, String> = tags
            .to_owned()
            .iter()
            .cloned()
            .map(|(a, b)| (a.to_owned(), b.to_owned()))
            .collect();

        Self {
            tags: tags_hashmap,
            ..self
        }
    }

    pub fn tenant_id(self, tenant_id: String) -> Self {
        Self {
            tenant_id: Some(tenant_id),
            ..self
        }
    }

    pub fn http_headers(self, http_headers: HashMap<String, String>) -> Self {
        Self {
            http_headers,
            ..self
        }
    }

    pub fn upload_interval(self, upload_interval: Duration) -> Self {
        Self {
            upload_interval,
            ..self
        }
    }
}

/// PyroscopeAgent Builder
///
/// # Example
/// ```no_run
/// use pyroscope::pyroscope::PyroscopeAgentBuilder;
/// use pyroscope::backend::{pprof_backend, PprofConfig, BackendConfig};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let agent = PyroscopeAgentBuilder::new(
///     "http://localhost:8080", "my-app", 100, "pyroscope-rs", "0.1.0",
///     pprof_backend(PprofConfig::default(), BackendConfig::default()),
/// )
/// .build()?;
/// # Ok(())
/// # }
/// ```
pub struct PyroscopeAgentBuilder {
    /// Profiler backend
    backend: BackendImpl<BackendUninitialized>,
    /// Configuration Object
    config: PyroscopeConfig,
}

impl PyroscopeAgentBuilder {
    /// Create a new PyroscopeAgentBuilder object.
    ///
    /// # Example
    /// ```no_run
    /// use pyroscope::pyroscope::PyroscopeAgentBuilder;
    /// use pyroscope::backend::{pprof_backend, PprofConfig, BackendConfig};
    ///
    /// let builder = PyroscopeAgentBuilder::new(
    ///     "http://localhost:8080", "my-app", 100, "pyroscope-rs", "0.1.0",
    ///     pprof_backend(PprofConfig::default(), BackendConfig::default()),
    /// );
    /// ```
    pub fn new(
        url: impl AsRef<str>,
        application_name: impl AsRef<str>,
        sample_rate: u32,
        spy_name: impl AsRef<str>,
        spy_version: impl AsRef<str>,
        backend: BackendImpl<BackendUninitialized>,
    ) -> Self {
        Self {
            backend,
            config: PyroscopeConfig::new(url, application_name, sample_rate, spy_name, spy_version),
        }
    }

    /// Override the Pyroscope Server URL.
    ///
    /// # Example
    /// ```no_run
    /// use pyroscope::pyroscope::PyroscopeAgentBuilder;
    /// use pyroscope::backend::{pprof_backend, PprofConfig, BackendConfig};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let agent = PyroscopeAgentBuilder::new(
    ///     "http://localhost:4040", "my-app", 100, "pyroscope-rs", "0.1.0",
    ///     pprof_backend(PprofConfig::default(), BackendConfig::default()),
    /// )
    /// .url("http://localhost:8080")
    /// .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn url(self, url: impl AsRef<str>) -> Self {
        Self {
            config: self.config.url(url),
            ..self
        }
    }

    pub fn basic_auth(self, username: impl AsRef<str>, password: impl AsRef<str>) -> Self {
        Self {
            config: self
                .config
                .basic_auth(username.as_ref().to_owned(), password.as_ref().to_owned()),
            ..self
        }
    }

    /// Set the Function.
    /// This is optional. If not set, the agent will not apply any function.
    /// # Example
    /// ```no_run
    /// use pyroscope::pyroscope::PyroscopeAgentBuilder;
    /// use pyroscope::backend::{pprof_backend, PprofConfig, BackendConfig};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let agent = PyroscopeAgentBuilder::new(
    ///     "http://localhost:8080", "my-app", 100, "pyroscope-rs", "0.1.0",
    ///     pprof_backend(PprofConfig::default(), BackendConfig::default()),
    /// )
    /// .func(|report| report)
    /// .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn func(self, func: fn(Report) -> Report) -> Self {
        Self {
            config: self.config.func(func),
            ..self
        }
    }

    /// Set tags. Default is empty.
    ///
    /// # Example
    /// ```no_run
    /// use pyroscope::pyroscope::PyroscopeAgentBuilder;
    /// use pyroscope::backend::{pprof_backend, PprofConfig, BackendConfig};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let agent = PyroscopeAgentBuilder::new(
    ///     "http://localhost:8080", "my-app", 100, "pyroscope-rs", "0.1.0",
    ///     pprof_backend(PprofConfig::default(), BackendConfig::default()),
    /// )
    /// .tags(vec![("env", "dev")])
    /// .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn tags(self, tags: Vec<(&str, &str)>) -> Self {
        Self {
            config: self.config.tags(tags),
            ..self
        }
    }

    pub fn tenant_id(self, tenant_id: String) -> Self {
        Self {
            config: self.config.tenant_id(tenant_id),
            ..self
        }
    }

    pub fn http_headers(self, http_headers: HashMap<String, String>) -> Self {
        Self {
            config: self.config.http_headers(http_headers),
            ..self
        }
    }

    pub fn upload_interval(self, upload_interval: Duration) -> Self {
        Self {
            config: self.config.upload_interval(upload_interval),
            ..self
        }
    }

    /// Initialize the backend, timer and return a PyroscopeAgent with Ready
    /// state. While you can call this method, you should call it through the
    /// `PyroscopeAgent.build()` method.
    pub fn build(self) -> Result<PyroscopeAgent<PyroscopeAgentReady>> {
        let config = self.config;

        // Set Global Tags
        // for (key, value) in config.tags.iter() {
        // todo!("implement")
        // }

        // Initialize the Backend
        let backend_ready = self.backend.initialize()?;
        log::trace!(target: LOG_TAG, "Backend initialized");

        // Start the Timer
        let timer = Timer::initialize(config.upload_interval)?;
        log::trace!(target: LOG_TAG, "Timer initialized");

        // Start the SessionManager
        let session_manager = SessionManager::new()?;
        log::trace!(target: LOG_TAG, "SessionManager initialized");

        // Return PyroscopeAgent
        Ok(PyroscopeAgent {
            backend: backend_ready,
            config,
            timer,
            session_manager,
            tx: None,
            handle: None,
            running: Arc::new((
                #[allow(clippy::mutex_atomic)]
                Mutex::new(false),
                Condvar::new(),
            )),
            _state: PhantomData,
        })
    }
}

/// This trait is used to encode the state of the agent.
pub trait PyroscopeAgentState {}

/// Marker struct for an Uninitialized state.
#[derive(Debug)]
pub struct PyroscopeAgentBare;

/// Marker struct for a Ready state.
#[derive(Debug)]
pub struct PyroscopeAgentReady;

/// Marker struct for a Running state.
#[derive(Debug)]
pub struct PyroscopeAgentRunning;

impl PyroscopeAgentState for PyroscopeAgentBare {}

impl PyroscopeAgentState for PyroscopeAgentReady {}

impl PyroscopeAgentState for PyroscopeAgentRunning {}

/// PyroscopeAgent is the main object of the library. It is used to start and stop the profiler, schedule the timer, and send the profiler data to the server.
pub struct PyroscopeAgent<S: PyroscopeAgentState> {
    /// Instance of the Timer
    timer: Timer,
    /// Instance of the SessionManager
    session_manager: SessionManager,
    /// Channel sender for the timer thread
    tx: Option<Sender<TimerSignal>>,
    /// Handle to the thread that runs the Pyroscope Agent
    handle: Option<JoinHandle<Result<()>>>,
    /// A structure to signal thread termination
    running: Arc<(Mutex<bool>, Condvar)>,
    /// Profiler backend
    pub backend: BackendImpl<BackendReady>,
    /// Configuration Object
    pub config: PyroscopeConfig,
    /// PyroscopeAgent State
    _state: PhantomData<S>,
}

impl<S: PyroscopeAgentState> PyroscopeAgent<S> {
    /// Transition the PyroscopeAgent to a new state.
    fn transition<D: PyroscopeAgentState>(self) -> PyroscopeAgent<D> {
        PyroscopeAgent {
            timer: self.timer,
            session_manager: self.session_manager,
            tx: self.tx,
            handle: self.handle,
            running: self.running,
            backend: self.backend,
            config: self.config,
            _state: PhantomData,
        }
    }
}

impl<S: PyroscopeAgentState> PyroscopeAgent<S> {
    /// Properly shutdown the agent.
    pub fn shutdown(mut self) {
        log::debug!(target: LOG_TAG, "PyroscopeAgent::drop()");

        // Shutdown Backend
        match self.backend.shutdown() {
            Ok(_) => log::debug!(target: LOG_TAG, "Backend shutdown"),
            Err(e) => log::error!(target: LOG_TAG, "Backend shutdown error: {e}"),
        }

        // Drop Timer listeners
        match self.timer.drop_listeners() {
            Ok(_) => log::trace!(target: LOG_TAG, "Dropped timer listeners"),
            Err(_) => log::error!(target: LOG_TAG, "Error Dropping timer listeners"),
        }

        // Wait for the Timer thread to finish
        if let Some(handle) = self.timer.handle.take() {
            match handle.join() {
                Ok(_) => log::trace!(target: LOG_TAG, "Dropped timer thread"),
                Err(_) => log::error!(target: LOG_TAG, "Error Dropping timer thread"),
            }
        }

        // Stop the SessionManager
        match self.session_manager.push(SessionSignal::Kill) {
            Ok(_) => log::trace!(target: LOG_TAG, "Sent kill signal to SessionManager"),
            Err(_) => log::error!(
                target: LOG_TAG,
                "Error sending kill signal to SessionManager"
            ),
        }

        if let Some(handle) = self.session_manager.handle.take() {
            match handle.join() {
                Ok(_) => log::trace!(target: LOG_TAG, "Dropped SessionManager thread"),
                Err(_) => log::error!(target: LOG_TAG, "Error Dropping SessionManager thread"),
            }
        }

        // Wait for main thread to finish
        if let Some(handle) = self.handle.take() {
            match handle.join() {
                Ok(_) => log::trace!(target: LOG_TAG, "Dropped main thread"),
                Err(_) => log::error!(target: LOG_TAG, "Error Dropping main thread"),
            }
        }

        log::debug!(target: LOG_TAG, "Agent Shutdown");
    }
}

impl PyroscopeAgent<PyroscopeAgentReady> {
    /// Start profiling and sending data. The agent will keep running until stopped. The agent will send data to the server once per `upload_interval` (10s by default).
    ///
    /// # Example
    /// ```no_run
    /// # use pyroscope::pyroscope::PyroscopeAgentBuilder;
    /// # use pyroscope::backend::{pprof_backend, PprofConfig, BackendConfig};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let agent = PyroscopeAgentBuilder::new("http://localhost:4040", "my-app", 100, "pyroscope-rs", "0.1.0", pprof_backend(PprofConfig::default(), BackendConfig::default())).build()?;
    /// let agent_running = agent.start()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn start(mut self) -> Result<PyroscopeAgent<PyroscopeAgentRunning>> {
        log::debug!(target: LOG_TAG, "Starting");

        // Create a clone of Backend
        let backend = Arc::clone(&self.backend.backend);
        // Call start()

        // set running to true
        let pair = Arc::clone(&self.running);
        let (lock, _cvar) = &*pair;
        let mut running = lock.lock()?;
        *running = true;
        drop(running);

        // Create a channel to listen for timer signals
        let (tx, rx) = mpsc::channel();
        self.timer.attach_listener(tx.clone())?;
        self.tx = Some(tx);

        let config = self.config.clone();

        // Clone SessionManager Sender
        let stx = self.session_manager.tx.clone();

        self.handle = Some(std::thread::spawn(move || {
            log::trace!(target: LOG_TAG, "Main Thread started");

            while let Ok(signal) = rx.recv() {
                match signal {
                    TimerSignal::NextSnapshot(until) => {
                        log::trace!(target: LOG_TAG, "Sending session {until}");

                        // Generate report from backend
                        let report = backend
                            .lock()?
                            .as_mut()
                            .ok_or_else(|| {
                                PyroscopeError::AdHoc(
                                    "PyroscopeAgent - Failed to unwrap backend".to_string(),
                                )
                            })?
                            .report()?;

                        // Send new Session to SessionManager
                        stx.send(SessionSignal::Session(Box::new(Session::new(
                            until,
                            config.clone(),
                            report,
                        )?)))?
                    }
                    TimerSignal::Terminate => {
                        log::trace!(target: LOG_TAG, "Session Killed");

                        // Notify the Stop function
                        let (lock, cvar) = &*pair;
                        let mut running = lock.lock()?;
                        *running = false;
                        cvar.notify_one();

                        // Kill the internal thread
                        return Ok(());
                    }
                }
            }
            Ok(())
        }));

        Ok(self.transition())
    }
}

impl PyroscopeAgent<PyroscopeAgentRunning> {
    /// Stop the agent. The agent will stop profiling and send a last report to the server.
    ///
    /// # Example
    /// ```no_run
    /// # use pyroscope::pyroscope::PyroscopeAgentBuilder;
    /// # use pyroscope::backend::{pprof_backend, PprofConfig, BackendConfig};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let agent = PyroscopeAgentBuilder::new("http://localhost:4040", "my-app", 100, "pyroscope-rs", "0.1.0", pprof_backend(PprofConfig::default(), BackendConfig::default())).build()?;
    /// # let agent_running = agent.start()?;
    /// let agent_ready = agent_running.stop()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn stop(mut self) -> Result<PyroscopeAgent<PyroscopeAgentReady>> {
        log::debug!(target: LOG_TAG, "Stopping");
        // get tx and send termination signal
        if let Some(sender) = self.tx.take() {
            // Send last session
            sender.send(TimerSignal::NextSnapshot(
                get_time_range(0, self.config.upload_interval)?.until,
            ))?;
            // Terminate PyroscopeAgent internal thread
            sender.send(TimerSignal::Terminate)?;
        } else {
            log::error!("PyroscopeAgent - Missing sender")
        }

        // Wait for the Thread to finish
        let pair = Arc::clone(&self.running);
        let (lock, cvar) = &*pair;
        let _guard = cvar.wait_while(lock.lock()?, |running| *running)?;

        Ok(self.transition())
    }

    /// Return a tuple of functions to add and remove tags to the agent across
    /// thread boundaries. This function can be called multiple times.
    ///
    /// # Example
    /// ```no_run
    /// # use pyroscope::pyroscope::PyroscopeAgentBuilder;
    /// # use pyroscope::backend::{pprof_backend, PprofConfig, BackendConfig};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let agent = PyroscopeAgentBuilder::new("http://localhost:4040", "my-app", 100, "pyroscope-rs", "0.1.0", pprof_backend(PprofConfig::default(), BackendConfig::default())).build()?;
    /// # let agent_running = agent.start()?;
    /// let (add_tag, remove_tag) = agent_running.tag_wrapper();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The functions can be later called from any thread.
    ///
    /// # Example
    /// ```ignore
    /// add_tag("key".to_string(), "value".to_string());
    /// // some computation
    /// remove_tag("key".to_string(), "value".to_string());
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn tag_wrapper(
        &self,
    ) -> (
        impl Fn(String, String) -> Result<()>,
        impl Fn(String, String) -> Result<()>,
    ) {
        let backend_add = self.backend.backend.clone();
        let backend_remove = self.backend.backend.clone();

        (
            move |key, value| {
                // https://github.com/tikv/pprof-rs/blob/01cff82dbe6fe110a707bf2b38d8ebb1d14a18f8/src/profiler.rs#L405
                let thread_id = crate::utils::ThreadId::pthread_self();
                let rule = ThreadTag::new(thread_id, Tag::new(key, value));
                let backend = backend_add.lock()?;
                backend
                    .as_ref()
                    .ok_or_else(|| {
                        PyroscopeError::AdHoc(
                            "PyroscopeAgent - Failed to unwrap backend".to_string(),
                        )
                    })?
                    .add_tag(rule)?;

                Ok(())
            },
            move |key, value| {
                // https://github.com/tikv/pprof-rs/blob/01cff82dbe6fe110a707bf2b38d8ebb1d14a18f8/src/profiler.rs#L405
                let thread_id = crate::utils::ThreadId::pthread_self();
                let rule = ThreadTag::new(thread_id, Tag::new(key, value));
                let backend = backend_remove.lock()?;
                backend
                    .as_ref()
                    .ok_or_else(|| {
                        PyroscopeError::AdHoc(
                            "PyroscopeAgent - Failed to unwrap backend".to_string(),
                        )
                    })?
                    .remove_tag(rule)?;

                Ok(())
            },
        )
    }

    /// Add a thread Tag rule to the backend Ruleset. For tagging, it's
    /// recommended to use the `tag_wrapper` function.
    pub fn add_thread_tag(&self, thread_id: crate::utils::ThreadId, tag: Tag) -> Result<()> {
        let rule = ThreadTag::new(thread_id, tag);
        self.backend.add_tag(rule)?;

        Ok(())
    }

    /// Remove a thread Tag rule from the backend Ruleset. For tagging, it's
    /// recommended to use the `tag_wrapper` function.
    pub fn remove_thread_tag(&self, thread_id: crate::utils::ThreadId, tag: Tag) -> Result<()> {
        let rule = ThreadTag::new(thread_id, tag);
        self.backend.remove_tag(rule)?;

        Ok(())
    }
}

pub fn parse_http_headers_json(http_headers_json: String) -> Result<HashMap<String, String>> {
    let mut http_headers = HashMap::new();
    let parsed: serde_json::Value = serde_json::from_str(&http_headers_json)?;
    let parsed = parsed
        .as_object()
        .ok_or_else(|| PyroscopeError::AdHoc(format!("expected object, got {parsed}")))?;
    for (k, v) in parsed {
        if let Some(value) = v.as_str() {
            http_headers.insert(k.to_string(), value.to_string());
        } else {
            return Err(PyroscopeError::AdHoc(format!(
                "invalid http header value, not a string: {v}"
            )));
        }
    }
    Ok(http_headers)
}

pub fn parse_vec_string_json(s: String) -> Result<Vec<String>> {
    let parsed: serde_json::Value = serde_json::from_str(&s)?;
    let parsed = parsed
        .as_array()
        .ok_or_else(|| PyroscopeError::AdHoc(format!("expected array, got {parsed}")))?;
    let mut res = Vec::with_capacity(parsed.len());
    for v in parsed {
        if let Some(s) = v.as_str() {
            res.push(s.to_string());
        } else {
            return Err(PyroscopeError::AdHoc(format!(
                "invalid element value, not a string: {v}"
            )));
        }
    }
    Ok(res)
}
