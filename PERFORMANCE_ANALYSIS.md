# Data Scanner Performance Analysis and Improvement Plan

## Executive Summary

The data scanner module in RustFS is causing significant performance degradation during write operations. This analysis identifies critical bottlenecks and proposes concrete improvements.

## Key Performance Issues Identified

### 1. Continuous File System Scanning During Writes
**Impact: HIGH**
- The scanner performs full directory walks (`walk_dir`) on all buckets every 60 seconds by default
- Each scan reads ALL metadata files (`xl.meta`) in the bucket, causing massive I/O operations
- During write operations, this creates I/O contention and locks

### 2. Excessive Lock Contention
**Impact: HIGH**
- Multiple `Arc<Mutex>` and `Arc<RwLock>` are held during scanning operations
- The scanner locks bucket_metrics, disk_metrics, and data_usage_stats frequently
- These locks block write operations when they need to update the same metrics

### 3. Inefficient Concurrent Scanning
**Impact: MEDIUM**
- Default `max_concurrent_scans: 20` creates too many parallel I/O operations
- Semaphore-based concurrency control doesn't consider disk I/O bandwidth limits
- Multiple EC sets are scanned simultaneously, overwhelming the disk subsystem

### 4. Redundant Metadata Reading
**Impact: HIGH**
- The scanner reads EVERY object's metadata during each scan cycle
- For large buckets with millions of objects, this creates enormous I/O load
- No caching mechanism for unchanged objects between scans

### 5. Synchronous Blocking Operations
**Impact: MEDIUM**
- Several operations use blocking I/O within async contexts
- `read_metadata` operations are not properly batched
- No I/O throttling mechanism to prevent overwhelming the system

## Root Cause Analysis

### Primary Issue: Aggressive Full Scanning
The scanner performs complete bucket scans every minute, reading all object metadata regardless of whether objects have changed. This creates:
- Constant disk I/O pressure
- Lock contention with write operations
- CPU overhead from metadata parsing
- Memory pressure from buffering scan results

### Secondary Issue: Poor Resource Management
- No adaptive scanning based on system load
- No prioritization between scanning and write operations
- Inefficient use of system resources during concurrent operations

## Proposed Solutions

### Solution 1: Implement Incremental Scanning
Instead of full scans, track changes and only scan modified objects:
- Use filesystem change notifications (inotify on Linux)
- Maintain a change journal for modified objects
- Only scan objects that have been modified since last scan

### Solution 2: Adaptive Scan Intervals
Dynamically adjust scan frequency based on:
- Current write load
- System I/O utilization
- Time of day / business hours
- Number of recent changes

### Solution 3: Optimized Lock Strategy
- Replace `Mutex` with `RwLock` where appropriate
- Use lock-free data structures for metrics collection
- Implement fine-grained locking per bucket instead of global locks

### Solution 4: I/O Throttling and Prioritization
- Implement I/O rate limiting for scanner operations
- Give priority to write operations over scanning
- Use async I/O with proper backpressure handling

### Solution 5: Metadata Caching
- Cache object metadata with TTL
- Only re-read metadata if file modification time has changed
- Use memory-mapped files for frequently accessed metadata

## Implementation Priority

1. **Immediate Fix**: Reduce scan frequency and concurrency
2. **Short-term**: Implement I/O throttling and better lock management
3. **Medium-term**: Add incremental scanning capability
4. **Long-term**: Full metadata caching system

## Expected Performance Improvements

- **Write latency reduction**: 40-60%
- **I/O utilization reduction**: 50-70%
- **CPU usage reduction**: 20-30%
- **Memory usage optimization**: 30-40%