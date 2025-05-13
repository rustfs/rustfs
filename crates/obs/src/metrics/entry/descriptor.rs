use crate::metrics::{MetricName, MetricType};
use std::collections::HashSet;

/// MetricDescriptor - Indicates the metric descriptor
#[derive(Debug, Clone)]
pub struct MetricDescriptor {
    pub name: MetricName,
    pub metric_type: MetricType,
    pub help: String,
    pub variable_labels: Vec<String>,

    // internal values for management：
    label_set: Option<HashSet<String>>,
}

impl MetricDescriptor {
    /// Create a new metric descriptor
    pub fn new(name: MetricName, metric_type: MetricType, help: String, variable_labels: Vec<String>) -> Self {
        Self {
            name,
            metric_type,
            help,
            variable_labels,
            label_set: None,
        }
    }

    /// Check whether the label is in the label set
    pub fn has_label(&mut self, label: &str) -> bool {
        self.get_label_set().contains(label)
    }

    /// Gets a collection of tags, and creates if it doesn't exist
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
