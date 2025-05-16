use crate::metrics::{MetricName, MetricNamespace, MetricSubsystem, MetricType};
use std::collections::HashSet;

/// MetricDescriptor - Metric descriptors
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MetricDescriptor {
    pub name: MetricName,
    pub metric_type: MetricType,
    pub help: String,
    pub variable_labels: Vec<String>,
    pub namespace: MetricNamespace,
    pub subsystem: MetricSubsystem,

    // Internal management values
    label_set: Option<HashSet<String>>,
}

impl MetricDescriptor {
    /// Create a new metric descriptor
    pub fn new(
        name: MetricName,
        metric_type: MetricType,
        help: String,
        variable_labels: Vec<String>,
        namespace: MetricNamespace,
        subsystem: impl Into<MetricSubsystem>, // Modify the parameter type
    ) -> Self {
        Self {
            name,
            metric_type,
            help,
            variable_labels,
            namespace,
            subsystem: subsystem.into(),
            label_set: None,
        }
    }

    /// Get the full metric name, including the prefix and formatting path
    #[allow(dead_code)]
    pub fn get_full_metric_name(&self) -> String {
        let prefix = self.metric_type.as_prom();
        let namespace = self.namespace.as_str();
        let formatted_subsystem = self.subsystem.as_str();

        format!("{}{}_{}_{}", prefix, namespace, formatted_subsystem, self.name.as_str())
    }

    /// check whether the label is in the label set
    #[allow(dead_code)]
    pub fn has_label(&mut self, label: &str) -> bool {
        self.get_label_set().contains(label)
    }

    /// Gets a collection of tags and creates them if they don't exist
    pub fn get_label_set(&mut self) -> &HashSet<String> {
        if self.label_set.is_none() {
            let mut set = HashSet::with_capacity(self.variable_labels.len());
            for label in &self.variable_labels {
                set.insert(label.clone());
            }
            self.label_set = Some(set);
        }
        self.label_set.as_ref().unwrap()
    }
}
