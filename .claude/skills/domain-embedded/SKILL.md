---
name: domain-embedded
description: "Use when developing embedded/no_std Rust. Keywords: embedded, no_std, microcontroller, MCU, ARM, RISC-V, bare metal, firmware, HAL, PAC, RTIC, embassy, interrupt, DMA, peripheral, GPIO, SPI, I2C, UART, embedded-hal, cortex-m, esp32, stm32, nrf, 嵌入式, 单片机, 固件, 裸机"
globs: ["**/Cargo.toml", "**/.cargo/config.toml"]
---

# Embedded Domain

> **Layer 3: Domain Constraints**

## Domain Constraints → Design Implications

| Domain Rule | Design Constraint | Rust Implication |
|-------------|-------------------|------------------|
| No heap | Stack allocation | heapless, no Box/Vec |
| No std | Core only | #![no_std] |
| Real-time | Predictable timing | No dynamic alloc |
| Resource limited | Minimal memory | Static buffers |
| Hardware safety | Safe peripheral access | HAL + ownership |
| Interrupt safe | No blocking in ISR | Atomic, critical sections |

---

## Critical Constraints

### No Dynamic Allocation

```
RULE: Cannot use heap (no allocator)
WHY: Deterministic memory, no OOM
RUST: heapless::Vec<T, N>, arrays
```

### Interrupt Safety

```
RULE: Shared state must be interrupt-safe
WHY: ISR can preempt at any time
RUST: Mutex<RefCell<T>> + critical section
```

### Hardware Ownership

```
RULE: Peripherals must have clear ownership
WHY: Prevent conflicting access
RUST: HAL takes ownership, singletons
```

---

## Trace Down ↓

From constraints to design (Layer 2):

```
"Need no_std compatible data structures"
    ↓ m02-resource: heapless collections
    ↓ Static sizing: heapless::Vec<T, N>

"Need interrupt-safe state"
    ↓ m03-mutability: Mutex<RefCell<Option<T>>>
    ↓ m07-concurrency: Critical sections

"Need peripheral ownership"
    ↓ m01-ownership: Singleton pattern
    ↓ m12-lifecycle: RAII for hardware
```

---

## Layer Stack

| Layer | Examples | Purpose |
|-------|----------|---------|
| PAC | stm32f4, esp32c3 | Register access |
| HAL | stm32f4xx-hal | Hardware abstraction |
| Framework | RTIC, Embassy | Concurrency |
| Traits | embedded-hal | Portable drivers |

## Framework Comparison

| Framework | Style | Best For |
|-----------|-------|----------|
| RTIC | Priority-based | Interrupt-driven apps |
| Embassy | Async | Complex state machines |
| Bare metal | Manual | Simple apps |

## Key Crates

| Purpose | Crate |
|---------|-------|
| Runtime (ARM) | cortex-m-rt |
| Panic handler | panic-halt, panic-probe |
| Collections | heapless |
| HAL traits | embedded-hal |
| Logging | defmt |
| Flash/debug | probe-run |

## Design Patterns

| Pattern | Purpose | Implementation |
|---------|---------|----------------|
| no_std setup | Bare metal | `#![no_std]` + `#![no_main]` |
| Entry point | Startup | `#[entry]` or embassy |
| Static state | ISR access | `Mutex<RefCell<Option<T>>>` |
| Fixed buffers | No heap | `heapless::Vec<T, N>` |

## Code Pattern: Static Peripheral

```rust
#![no_std]
#![no_main]

use cortex_m::interrupt::{self, Mutex};
use core::cell::RefCell;

static LED: Mutex<RefCell<Option<Led>>> = Mutex::new(RefCell::new(None));

#[entry]
fn main() -> ! {
    let dp = pac::Peripherals::take().unwrap();
    let led = Led::new(dp.GPIOA);

    interrupt::free(|cs| {
        LED.borrow(cs).replace(Some(led));
    });

    loop {
        interrupt::free(|cs| {
            if let Some(led) = LED.borrow(cs).borrow_mut().as_mut() {
                led.toggle();
            }
        });
    }
}
```

---

## Common Mistakes

| Mistake | Domain Violation | Fix |
|---------|-----------------|-----|
| Using Vec | Heap allocation | heapless::Vec |
| No critical section | Race with ISR | Mutex + interrupt::free |
| Blocking in ISR | Missed interrupts | Defer to main loop |
| Unsafe peripheral | Hardware conflict | HAL ownership |

---

## Trace to Layer 1

| Constraint | Layer 2 Pattern | Layer 1 Implementation |
|------------|-----------------|------------------------|
| No heap | Static collections | heapless::Vec<T, N> |
| ISR safety | Critical sections | Mutex<RefCell<T>> |
| Hardware ownership | Singleton | take().unwrap() |
| no_std | Core-only | #![no_std], #![no_main] |

---

## Related Skills

| When | See |
|------|-----|
| Static memory | m02-resource |
| Interior mutability | m03-mutability |
| Interrupt patterns | m07-concurrency |
| Unsafe for hardware | unsafe-checker |
