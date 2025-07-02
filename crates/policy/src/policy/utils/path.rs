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

struct LazyBuf<'a> {
    s: &'a str,
    buf: Option<Vec<u8>>,
    w: usize,
}

impl<'a> LazyBuf<'a> {
    pub fn new(s: &'a str) -> Self {
        Self { s, buf: None, w: 0 }
    }

    fn index(&self, i: usize) -> u8 {
        self.buf.as_ref().map(|x| x[i]).unwrap_or_else(|| self.s.as_bytes()[i])
    }

    fn append(&mut self, c: u8) {
        if self.buf.is_none() {
            if self.w < self.s.len() && self.s.as_bytes()[self.w] == c {
                self.w += 1;
                return;
            }
            self.buf = Some({
                let mut buf = vec![0u8; self.s.len()];
                buf[..self.w].copy_from_slice(&self.s.as_bytes()[..self.w]);
                buf
            });
        }

        self.buf.as_mut().unwrap()[self.w] = c;
        self.w += 1;
    }

    fn string(&self) -> String {
        match self.buf {
            Some(ref s) => String::from_utf8_lossy(&s[..self.w]).to_string(),
            None => String::from_utf8_lossy(&self.s.as_bytes()[..self.w]).to_string(),
        }
    }
}

/// copy from golang(path.Clean)
pub fn clean(path: &str) -> String {
    if path.is_empty() {
        return ".".into();
    }

    let p = path.as_bytes();
    let (rooted, n, mut out, mut r, mut dotdot) = (p[0] == b'/', path.len(), LazyBuf::new(path), 0, 0);

    if rooted {
        out.append(b'/');
        r = 1;
        dotdot = 1;
    }

    while r < n {
        if p[r] == b'/' || (p[r] == b'.' && (r + 1 == n || p[r + 1] == b'/')) {
            r += 1;
        } else if p[r] == b'.' && p[r + 1] == b'.' && (r + 2 == n || p[r + 2] == b'/') {
            r += 2;
            if out.w > dotdot {
                out.w -= 1;

                while out.w > dotdot && out.index(out.w) != b'/' {
                    out.w -= 1;
                }
            } else if !rooted {
                if out.w > 0 {
                    out.append(b'/');
                }

                out.append(b'.');
                out.append(b'.');
                dotdot = out.w;
            }
        } else {
            if rooted && out.w != 1 || !rooted && out.w != 0 {
                out.append(b'/');
            }

            while r < n && p[r] != b'/' {
                out.append(p[r]);
                r += 1;
            }
        }
    }

    if out.w == 0 { ".".into() } else { out.string() }
}

#[cfg(test)]
mod tests {
    use super::clean;

    #[test_case::test_case("", "."; "1")]
    #[test_case::test_case("abc", "abc"; "2")]
    #[test_case::test_case("abc/def", "abc/def"; "3")]
    #[test_case::test_case("a/b/c", "a/b/c"; "4")]
    #[test_case::test_case(".", "."; "5")]
    #[test_case::test_case("..", ".."; "6")]
    #[test_case::test_case("../..", "../.."; "7")]
    #[test_case::test_case("../../abc", "../../abc"; "8")]
    #[test_case::test_case("/abc", "/abc"; "9")]
    #[test_case::test_case("/", "/"; "10")]
    #[test_case::test_case("abc/", "abc"; "11")]
    #[test_case::test_case("abc/def/", "abc/def"; "12")]
    #[test_case::test_case("a/b/c/", "a/b/c"; "13")]
    #[test_case::test_case("./", "."; "14")]
    #[test_case::test_case("../", ".."; "15")]
    #[test_case::test_case("../../", "../.."; "16")]
    #[test_case::test_case("/abc/", "/abc"; "17")]
    #[test_case::test_case("abc//def//ghi", "abc/def/ghi"; "18")]
    #[test_case::test_case("//abc", "/abc"; "19")]
    #[test_case::test_case("///abc", "/abc"; "20")]
    #[test_case::test_case("//abc//", "/abc"; "21")]
    #[test_case::test_case("abc//", "abc"; "22")]
    #[test_case::test_case("abc/./def", "abc/def"; "23")]
    #[test_case::test_case("/./abc/def", "/abc/def"; "24")]
    #[test_case::test_case("abc/.", "abc"; "25")]
    #[test_case::test_case("abc/def/ghi/../jkl", "abc/def/jkl"; "26")]
    #[test_case::test_case("abc/def/../ghi/../jkl", "abc/jkl"; "27")]
    #[test_case::test_case("abc/def/..", "abc"; "28")]
    #[test_case::test_case("abc/def/../..", "."; "29")]
    #[test_case::test_case("/abc/def/../..", "/"; "30")]
    #[test_case::test_case("abc/def/../../..", ".."; "31")]
    #[test_case::test_case("/abc/def/../../..", "/"; "32")]
    #[test_case::test_case("abc/def/../../../ghi/jkl/../../../mno", "../../mno"; "33")]
    #[test_case::test_case("abc/./../def", "def"; "34")]
    #[test_case::test_case("abc//./../def", "def"; "35")]
    #[test_case::test_case("abc/../../././../def", "../../def"; "36")]
    fn test_clean(path: &str, result: &str) {
        assert_eq!(clean(path), result.to_owned());
        assert_eq!(clean(result), result.to_owned());
    }
}
