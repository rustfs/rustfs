use crate::metrics::{MetricName, MetricNamespace, MetricSubsystem, MetricType};
use std::collections::HashSet;

/// MetricDescriptor - 指标描述符
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MetricDescriptor {
    pub name: MetricName,
    pub metric_type: MetricType,
    pub help: String,
    pub variable_labels: Vec<String>,
    pub namespace: MetricNamespace,
    pub subsystem: MetricSubsystem, // 从 String 修改为 MetricSubsystem

    // 内部管理值
    label_set: Option<HashSet<String>>,
}

impl MetricDescriptor {
    /// 创建新的指标描述符
    pub fn new(
        name: MetricName,
        metric_type: MetricType,
        help: String,
        variable_labels: Vec<String>,
        namespace: MetricNamespace,
        subsystem: impl Into<MetricSubsystem>, // 修改参数类型
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

    /// 获取完整的指标名称，包含前缀和格式化路径
    #[allow(dead_code)]
    pub fn get_full_metric_name(&self) -> String {
        let prefix = self.metric_type.to_prom();
        let namespace = self.namespace.as_str();
        let formatted_subsystem = self.subsystem.as_str();

        format!("{}{}_{}_{}", prefix, namespace, formatted_subsystem, self.name.as_str())
    }

    /// 检查标签是否在标签集中
    #[allow(dead_code)]
    pub fn has_label(&mut self, label: &str) -> bool {
        self.get_label_set().contains(label)
    }

    /// 获取标签集合，如果不存在则创建
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
