// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! I/O priority queue for scheduling I/O operations.
//!
//! This module provides a priority queue implementation for I/O operations
//! with support for starvation prevention and fair scheduling.

use crate::config::IoPriorityQueueConfig;
use crate::scheduler::IoPriority;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// A queued I/O request.
#[derive(Debug, Clone)]
pub struct IoRequest {
    /// Request ID.
    pub id: u64,
    /// Request priority.
    pub priority: IoPriority,
    /// Request size in bytes.
    pub size: usize,
    /// Queue time.
    pub queued_at: Instant,
    /// Whether this is a sequential read.
    pub is_sequential: bool,
}

impl IoRequest {
    /// Create a new I/O request.
    pub fn new(id: u64, priority: IoPriority, size: usize, is_sequential: bool) -> Self {
        Self {
            id,
            priority,
            size,
            queued_at: Instant::now(),
            is_sequential,
        }
    }

    /// Get the wait time in the queue.
    pub fn wait_time(&self) -> Duration {
        self.queued_at.elapsed()
    }
}

/// Queue status for a priority level.
#[derive(Debug, Clone, Default)]
pub struct IoQueueStatus {
    /// Number of requests in the queue.
    pub count: usize,
    /// Total size of all requests.
    pub total_size: usize,
    /// Oldest request wait time.
    pub oldest_wait: Option<Duration>,
    /// Number of requests processed.
    pub processed: u64,
}

impl IoQueueStatus {
    /// Create new queue status.
    pub fn new() -> Self {
        Self::default()
    }
}

/// I/O priority queue.
pub struct IoPriorityQueue {
    /// Queue configuration.
    config: IoPriorityQueueConfig,
    /// High priority queue.
    high: VecDeque<IoRequest>,
    /// Normal priority queue.
    normal: VecDeque<IoRequest>,
    /// Low priority queue.
    low: VecDeque<IoRequest>,
    /// Next request ID.
    next_id: u64,
    /// Last dequeue time for each priority (for starvation prevention).
    last_dequeue: [Option<Instant>; 3],
    /// Statistics for each queue.
    stats: [IoQueueStatus; 3],
}

impl IoPriorityQueue {
    /// Create a new priority queue with the given configuration.
    pub fn new(config: IoPriorityQueueConfig) -> Self {
        Self {
            config,
            high: VecDeque::with_capacity(100),
            normal: VecDeque::with_capacity(500),
            low: VecDeque::with_capacity(200),
            next_id: 0,
            last_dequeue: [None, None, None],
            stats: [IoQueueStatus::new(), IoQueueStatus::new(), IoQueueStatus::new()],
        }
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(IoPriorityQueueConfig::default())
    }

    /// Get the configuration.
    pub fn config(&self) -> &IoPriorityQueueConfig {
        &self.config
    }

    /// Enqueue a request.
    pub fn enqueue(&mut self, priority: IoPriority, size: usize, is_sequential: bool) -> u64 {
        let id = self.next_id;
        self.next_id += 1;

        let request = IoRequest::new(id, priority, size, is_sequential);

        match priority {
            IoPriority::High => {
                if self.high.len() < self.config.high_capacity {
                    self.high.push_back(request);
                }
            }
            IoPriority::Normal => {
                if self.normal.len() < self.config.normal_capacity {
                    self.normal.push_back(request);
                }
            }
            IoPriority::Low => {
                if self.low.len() < self.config.low_capacity {
                    self.low.push_back(request);
                }
            }
        }

        id
    }

    /// Dequeue the next request.
    ///
    /// Uses weighted fair queuing with starvation prevention.
    pub fn dequeue(&mut self) -> Option<IoRequest> {
        let now = Instant::now();

        // Check for starvation: if a lower priority queue hasn't been served in a while,
        // give it priority
        let normal_starved = self.is_starved(IoPriority::Normal, now);
        let low_starved = self.is_starved(IoPriority::Low, now);

        // Priority order with starvation consideration
        // Check conditions first, then dequeue
        let dequeue_high = !self.high.is_empty() && !low_starved && !normal_starved;
        let dequeue_normal = !self.normal.is_empty() && !low_starved;
        let dequeue_low = !self.low.is_empty();
        let dequeue_high_fallback = !self.high.is_empty();
        let dequeue_normal_fallback = !self.normal.is_empty();

        if dequeue_high {
            let request = self.high.pop_front();
            if request.is_some() {
                self.last_dequeue[0] = Some(Instant::now());
                self.stats[0].processed += 1;
            }
            request
        } else if dequeue_normal {
            let request = self.normal.pop_front();
            if request.is_some() {
                self.last_dequeue[1] = Some(Instant::now());
                self.stats[1].processed += 1;
            }
            request
        } else if dequeue_low {
            let request = self.low.pop_front();
            if request.is_some() {
                self.last_dequeue[2] = Some(Instant::now());
                self.stats[2].processed += 1;
            }
            request
        } else if dequeue_high_fallback {
            let request = self.high.pop_front();
            if request.is_some() {
                self.last_dequeue[0] = Some(Instant::now());
                self.stats[0].processed += 1;
            }
            request
        } else if dequeue_normal_fallback {
            let request = self.normal.pop_front();
            if request.is_some() {
                self.last_dequeue[1] = Some(Instant::now());
                self.stats[1].processed += 1;
            }
            request
        } else {
            None
        }
    }

    /// Check if a priority level is starved.
    fn is_starved(&self, priority: IoPriority, now: Instant) -> bool {
        let idx = match priority {
            IoPriority::High => 0,
            IoPriority::Normal => 1,
            IoPriority::Low => 2,
        };

        if let Some(last) = self.last_dequeue[idx] {
            now.duration_since(last) > self.config.starvation_threshold
        } else {
            false
        }
    }

    /// Get the total number of queued requests.
    pub fn len(&self) -> usize {
        self.high.len() + self.normal.len() + self.low.len()
    }

    /// Check if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.high.is_empty() && self.normal.is_empty() && self.low.is_empty()
    }

    /// Get queue status for a priority level.
    pub fn status(&self, priority: IoPriority) -> IoQueueStatus {
        let (queue, idx) = match priority {
            IoPriority::High => (&self.high, 0),
            IoPriority::Normal => (&self.normal, 1),
            IoPriority::Low => (&self.low, 2),
        };

        let mut status = self.stats[idx].clone();
        status.count = queue.len();
        status.total_size = queue.iter().map(|r| r.size).sum();
        status.oldest_wait = queue.front().map(|r| r.wait_time());
        status
    }

    /// Get the total queue status.
    pub fn total_status(&self) -> IoQueueStatus {
        let mut total = IoQueueStatus::new();
        total.count = self.len();
        total.total_size = self
            .high
            .iter()
            .chain(self.normal.iter())
            .chain(self.low.iter())
            .map(|r| r.size)
            .sum();
        total.processed = self.stats.iter().map(|s| s.processed).sum();
        total.oldest_wait = self
            .high
            .front()
            .map(|r| r.wait_time())
            .or_else(|| self.normal.front().map(|r| r.wait_time()))
            .or_else(|| self.low.front().map(|r| r.wait_time()));
        total
    }

    /// Clear all queues.
    pub fn clear(&mut self) {
        self.high.clear();
        self.normal.clear();
        self.low.clear();
    }

    /// Peek at the next request without removing it.
    pub fn peek(&self) -> Option<&IoRequest> {
        if !self.high.is_empty() {
            self.high.front()
        } else if !self.normal.is_empty() {
            self.normal.front()
        } else {
            self.low.front()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enqueue_dequeue() {
        let mut queue = IoPriorityQueue::with_defaults();

        let id1 = queue.enqueue(IoPriority::High, 1024, true);
        let id2 = queue.enqueue(IoPriority::Normal, 2048, false);
        let id3 = queue.enqueue(IoPriority::Low, 4096, true);

        assert_eq!(queue.len(), 3);

        // High priority should be dequeued first
        let req1 = queue.dequeue().unwrap();
        assert_eq!(req1.id, id1);
        assert_eq!(req1.priority, IoPriority::High);

        let req2 = queue.dequeue().unwrap();
        assert_eq!(req2.id, id2);
        assert_eq!(req2.priority, IoPriority::Normal);

        let req3 = queue.dequeue().unwrap();
        assert_eq!(req3.id, id3);
        assert_eq!(req3.priority, IoPriority::Low);

        assert!(queue.is_empty());
    }

    #[test]
    fn test_queue_status() {
        let mut queue = IoPriorityQueue::with_defaults();

        queue.enqueue(IoPriority::High, 1024, true);
        queue.enqueue(IoPriority::High, 2048, true);
        queue.enqueue(IoPriority::Normal, 4096, false);

        let high_status = queue.status(IoPriority::High);
        assert_eq!(high_status.count, 2);
        assert_eq!(high_status.total_size, 3072);

        let normal_status = queue.status(IoPriority::Normal);
        assert_eq!(normal_status.count, 1);
        assert_eq!(normal_status.total_size, 4096);

        let total = queue.total_status();
        assert_eq!(total.count, 3);
        assert_eq!(total.total_size, 7168);
    }

    #[test]
    fn test_queue_capacity() {
        let config = IoPriorityQueueConfig {
            high_capacity: 2,
            normal_capacity: 2,
            low_capacity: 2,
            ..Default::default()
        };
        let mut queue = IoPriorityQueue::new(config);

        queue.enqueue(IoPriority::High, 1024, true);
        queue.enqueue(IoPriority::High, 1024, true);
        queue.enqueue(IoPriority::High, 1024, true); // Should be dropped

        assert_eq!(queue.status(IoPriority::High).count, 2);
    }

    #[test]
    fn test_clear() {
        let mut queue = IoPriorityQueue::with_defaults();

        queue.enqueue(IoPriority::High, 1024, true);
        queue.enqueue(IoPriority::Normal, 2048, false);
        queue.enqueue(IoPriority::Low, 4096, true);

        assert_eq!(queue.len(), 3);
        queue.clear();
        assert!(queue.is_empty());
    }

    #[test]
    fn test_peek() {
        let mut queue = IoPriorityQueue::with_defaults();

        queue.enqueue(IoPriority::Normal, 2048, false);
        queue.enqueue(IoPriority::High, 1024, true);

        let peeked = queue.peek().unwrap();
        assert_eq!(peeked.priority, IoPriority::High);

        // Peek shouldn't remove the item
        assert_eq!(queue.len(), 2);
    }
}
