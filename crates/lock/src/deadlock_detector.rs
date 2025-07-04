use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, SystemTime};
use tracing::{debug, warn};

use crate::types::{DeadlockDetectionResult, LockPriority, LockType, WaitGraphNode, WaitQueueItem};

/// Deadlock detector
#[derive(Debug)]
pub struct DeadlockDetector {
    /// Wait graph: owner -> waiting resources
    wait_graph: HashMap<String, WaitGraphNode>,
    /// Resource holder mapping: resource -> owner
    resource_holders: HashMap<String, String>,
    /// Resource wait queue: resource -> wait queue
    wait_queues: HashMap<String, VecDeque<WaitQueueItem>>,
    /// Detection statistics
    detection_count: usize,
    /// Last detection time
    last_detection: SystemTime,
}

impl DeadlockDetector {
    /// Create new deadlock detector
    pub fn new() -> Self {
        Self {
            wait_graph: HashMap::new(),
            resource_holders: HashMap::new(),
            wait_queues: HashMap::new(),
            detection_count: 0,
            last_detection: SystemTime::now(),
        }
    }

    /// Add wait relationship
    pub fn add_wait_relationship(&mut self, owner: &str, waiting_for: &str, held_resources: Vec<String>, priority: LockPriority) {
        let node = WaitGraphNode {
            owner: owner.to_string(),
            waiting_for: vec![waiting_for.to_string()],
            held_resources,
            priority,
            wait_start_time: SystemTime::now(),
        };

        self.wait_graph.insert(owner.to_string(), node);
        debug!("Added wait relationship: {} -> {}", owner, waiting_for);
    }

    /// Remove wait relationship
    pub fn remove_wait_relationship(&mut self, owner: &str) {
        self.wait_graph.remove(owner);
        debug!("Removed wait relationship for owner: {}", owner);
    }

    /// Update resource holder
    pub fn update_resource_holder(&mut self, resource: &str, owner: &str) {
        if owner.is_empty() {
            self.resource_holders.remove(resource);
        } else {
            self.resource_holders.insert(resource.to_string(), owner.to_string());
        }
        debug!("Updated resource holder: {} -> {}", resource, owner);
    }

    /// Add wait queue item
    pub fn add_wait_queue_item(&mut self, resource: &str, owner: &str, lock_type: LockType, priority: LockPriority) {
        let item = WaitQueueItem::new(owner, lock_type, priority);
        self.wait_queues
            .entry(resource.to_string())
            .or_default()
            .push_back(item);
        debug!("Added wait queue item: {} -> {}", resource, owner);
    }

    /// Remove wait queue item
    pub fn remove_wait_queue_item(&mut self, resource: &str, owner: &str) {
        if let Some(queue) = self.wait_queues.get_mut(resource) {
            queue.retain(|item| item.owner != owner);
            if queue.is_empty() {
                self.wait_queues.remove(resource);
            }
        }
        debug!("Removed wait queue item: {} -> {}", resource, owner);
    }

    /// Detect deadlock
    pub fn detect_deadlock(&mut self) -> DeadlockDetectionResult {
        self.detection_count += 1;
        self.last_detection = SystemTime::now();

        let mut result = DeadlockDetectionResult {
            has_deadlock: false,
            deadlock_cycle: Vec::new(),
            suggested_resolution: None,
            affected_resources: Vec::new(),
            affected_owners: Vec::new(),
        };

        // Use depth-first search to detect cycle
        let mut visited = HashSet::new();
        let mut recursion_stack = HashSet::new();

        for owner in self.wait_graph.keys() {
            if !visited.contains(owner)
                && self.dfs_detect_cycle(owner, &mut visited, &mut recursion_stack, &mut result) {
                result.has_deadlock = true;
                break;
            }
        }

        if result.has_deadlock {
            warn!("Deadlock detected! Cycle: {:?}", result.deadlock_cycle);
            result.suggested_resolution = self.suggest_resolution(&result);
        }

        result
    }

    /// Depth-first search to detect cycle
    fn dfs_detect_cycle(
        &self,
        owner: &str,
        visited: &mut HashSet<String>,
        recursion_stack: &mut HashSet<String>,
        result: &mut DeadlockDetectionResult,
    ) -> bool {
        visited.insert(owner.to_string());
        recursion_stack.insert(owner.to_string());

        if let Some(node) = self.wait_graph.get(owner) {
            for waiting_for in &node.waiting_for {
                if let Some(holder) = self.resource_holders.get(waiting_for) {
                    if !visited.contains(holder) {
                        if self.dfs_detect_cycle(holder, visited, recursion_stack, result) {
                            result.deadlock_cycle.push(owner.to_string());
                            return true;
                        }
                    } else if recursion_stack.contains(holder) {
                        // Cycle detected
                        result.deadlock_cycle.push(owner.to_string());
                        result.deadlock_cycle.push(holder.to_string());
                        result.affected_owners.push(owner.to_string());
                        result.affected_owners.push(holder.to_string());
                        return true;
                    }
                }
            }
        }

        recursion_stack.remove(owner);
        false
    }

    /// Suggest resolution
    fn suggest_resolution(&self, result: &DeadlockDetectionResult) -> Option<String> {
        if result.deadlock_cycle.is_empty() {
            return None;
        }

        // Find owner with lowest priority
        let mut lowest_priority_owner = None;
        let mut lowest_priority = LockPriority::Critical;

        for owner in &result.affected_owners {
            if let Some(node) = self.wait_graph.get(owner) {
                if node.priority < lowest_priority {
                    lowest_priority = node.priority;
                    lowest_priority_owner = Some(owner.clone());
                }
            }
        }

        if let Some(owner) = lowest_priority_owner {
            Some(format!("Suggest releasing lock held by {owner} to break deadlock cycle"))
        } else {
            Some("Suggest randomly selecting an owner to release lock".to_string())
        }
    }

    /// Get wait queue information
    pub fn get_wait_queue_info(&self, resource: &str) -> Vec<WaitQueueItem> {
        self.wait_queues
            .get(resource)
            .map(|queue| queue.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Check for long waits
    pub fn check_long_waits(&self, timeout: Duration) -> Vec<String> {
        let mut long_waiters = Vec::new();

        for (owner, node) in &self.wait_graph {
            if node.wait_start_time.elapsed().unwrap_or(Duration::ZERO) > timeout {
                long_waiters.push(owner.clone());
            }
        }

        long_waiters
    }

    /// Suggest priority upgrade
    pub fn suggest_priority_upgrade(&self, resource: &str) -> Option<String> {
        if let Some(queue) = self.wait_queues.get(resource) {
            if queue.len() > 1 {
                // Find request with longest wait time and lowest priority
                let mut longest_wait = Duration::ZERO;
                let mut candidate = None;

                for item in queue {
                    let wait_duration = item.wait_duration();
                    if wait_duration > longest_wait && item.priority < LockPriority::High {
                        longest_wait = wait_duration;
                        candidate = Some(item.owner.clone());
                    }
                }

                if let Some(owner) = candidate {
                    return Some(format!("Suggest upgrading priority of {owner} to reduce wait time"));
                }
            }
        }

        None
    }

    /// Clean up expired waits
    pub fn cleanup_expired_waits(&mut self, max_wait_time: Duration) {
        let mut to_remove = Vec::new();

        for (owner, node) in &self.wait_graph {
            if node.wait_start_time.elapsed().unwrap_or(Duration::ZERO) > max_wait_time {
                to_remove.push(owner.clone());
            }
        }

        for owner in to_remove {
            self.remove_wait_relationship(&owner);
            warn!("Removed expired wait relationship for owner: {}", owner);
        }
    }

    /// Get detection statistics
    pub fn get_stats(&self) -> (usize, SystemTime) {
        (self.detection_count, self.last_detection)
    }

    /// Reset detector
    pub fn reset(&mut self) {
        self.wait_graph.clear();
        self.resource_holders.clear();
        self.wait_queues.clear();
        self.detection_count = 0;
        self.last_detection = SystemTime::now();
        debug!("Deadlock detector reset");
    }
}

impl Default for DeadlockDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deadlock_detector_creation() {
        let detector = DeadlockDetector::new();
        assert_eq!(detector.detection_count, 0);
    }

    #[test]
    fn test_add_wait_relationship() {
        let mut detector = DeadlockDetector::new();
        detector.add_wait_relationship("owner1", "resource1", vec!["resource2".to_string()], LockPriority::Normal);

        assert!(detector.wait_graph.contains_key("owner1"));
        let node = detector.wait_graph.get("owner1").unwrap();
        assert_eq!(node.owner, "owner1");
        assert_eq!(node.waiting_for, vec!["resource1"]);
    }

    #[test]
    fn test_deadlock_detection() {
        let mut detector = DeadlockDetector::new();

        // Create deadlock scenario: owner1 -> resource1 -> owner2 -> resource2 -> owner1
        detector.add_wait_relationship("owner1", "resource1", vec!["resource2".to_string()], LockPriority::Normal);
        detector.add_wait_relationship("owner2", "resource2", vec!["resource1".to_string()], LockPriority::Normal);

        detector.update_resource_holder("resource1", "owner2");
        detector.update_resource_holder("resource2", "owner1");

        let result = detector.detect_deadlock();
        assert!(result.has_deadlock);
        assert!(!result.deadlock_cycle.is_empty());
        assert!(result.suggested_resolution.is_some());
    }

    #[test]
    fn test_no_deadlock() {
        let mut detector = DeadlockDetector::new();

        // Create deadlock-free scenario
        detector.add_wait_relationship("owner1", "resource1", vec![], LockPriority::Normal);
        detector.update_resource_holder("resource1", "owner2");

        let result = detector.detect_deadlock();
        assert!(!result.has_deadlock);
    }

    #[test]
    fn test_wait_queue_management() {
        let mut detector = DeadlockDetector::new();

        detector.add_wait_queue_item("resource1", "owner1", LockType::Exclusive, LockPriority::Normal);
        detector.add_wait_queue_item("resource1", "owner2", LockType::Shared, LockPriority::High);

        let queue_info = detector.get_wait_queue_info("resource1");
        assert_eq!(queue_info.len(), 2);

        detector.remove_wait_queue_item("resource1", "owner1");
        let queue_info = detector.get_wait_queue_info("resource1");
        assert_eq!(queue_info.len(), 1);
    }

    #[test]
    fn test_priority_upgrade_suggestion() {
        let mut detector = DeadlockDetector::new();

        // Add multiple wait items
        detector.add_wait_queue_item("resource1", "owner1", LockType::Exclusive, LockPriority::Low);
        detector.add_wait_queue_item("resource1", "owner2", LockType::Exclusive, LockPriority::Normal);

        let suggestion = detector.suggest_priority_upgrade("resource1");
        assert!(suggestion.is_some());
        assert!(suggestion.unwrap().contains("owner1"));
    }

    #[test]
    fn test_cleanup_expired_waits() {
        let mut detector = DeadlockDetector::new();

        // Add a wait relationship
        detector.add_wait_relationship("owner1", "resource1", vec![], LockPriority::Normal);

        // Simulate long wait
        std::thread::sleep(Duration::from_millis(10));

        detector.cleanup_expired_waits(Duration::from_millis(5));
        assert!(!detector.wait_graph.contains_key("owner1"));
    }
}
