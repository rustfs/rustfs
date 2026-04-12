use std::fmt;

pub fn now_utc() -> impl fmt::Debug {
    #[cfg(not(target_arch = "wasm32"))]
    {
        time::OffsetDateTime::now_utc()
    }
    #[cfg(target_arch = "wasm32")]
    {
        ()
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub struct Instant(std::time::Instant);

#[cfg(target_arch = "wasm32")]
pub struct Instant(());

impl Instant {
    pub fn now() -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Self(std::time::Instant::now())
        }
        #[cfg(target_arch = "wasm32")]
        {
            Self(())
        }
    }

    pub fn elapsed(&self) -> impl fmt::Debug {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.0.elapsed()
        }
        #[cfg(target_arch = "wasm32")]
        {
            ()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_utc() {
        let t = now_utc();
        // Ensure it returns something Debug-formattable
        let _ = format!("{t:?}");
    }

    #[test]
    fn test_instant_now_and_elapsed() {
        let t = Instant::now();
        let e = t.elapsed();
        let _ = format!("{e:?}");
    }
}
