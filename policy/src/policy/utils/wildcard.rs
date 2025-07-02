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

#[allow(dead_code)]
pub fn is_simple_match<P, N>(pattern: P, name: N) -> bool
where
    P: AsRef<str>,
    N: AsRef<str>,
{
    inner_match(pattern, name, true)
}

pub fn is_match<P, N>(pattern: P, name: N) -> bool
where
    P: AsRef<str>,
    N: AsRef<str>,
{
    inner_match(pattern, name, false)
}

#[allow(dead_code)]
pub fn is_match_as_pattern_prefix<P, N>(pattern: P, text: N) -> bool
where
    P: AsRef<str>,
    N: AsRef<str>,
{
    let (mut p, mut t) = (pattern.as_ref().as_bytes().iter(), text.as_ref().as_bytes().iter());

    while let (Some(&x), Some(&y)) = (p.next(), t.next()) {
        if x == b'*' {
            return true;
        }

        if x == b'?' {
            continue;
        }

        if x != y {
            return false;
        }
    }

    text.as_ref().len() <= pattern.as_ref().len()
}

#[inline]
fn inner_match(pattern: impl AsRef<str>, name: impl AsRef<str>, simple: bool) -> bool {
    let (pattern, name) = (pattern.as_ref(), name.as_ref());

    if pattern.is_empty() {
        return pattern == name;
    }

    if pattern == "*" {
        return true;
    }

    deep_match(pattern.as_bytes(), name.as_bytes(), simple)
}

fn deep_match(mut pattern: &[u8], mut name: &[u8], simple: bool) -> bool {
    while !pattern.is_empty() {
        match pattern[0] {
            b'?' => {
                if name.is_empty() {
                    return simple;
                }
            }

            b'*' => {
                return pattern.len() == 1
                    || deep_match(&pattern[1..], name, simple)
                    || (!name.is_empty() && deep_match(pattern, &name[1..], simple));
            }

            _ => {
                if name.is_empty() || name[0] != pattern[0] {
                    return false;
                }
            }
        }

        name = &name[1..];
        pattern = &pattern[1..];
    }

    name.is_empty() && pattern.is_empty()
}

#[cfg(test)]
mod tests {
    use super::{is_match, is_match_as_pattern_prefix, is_simple_match};

    #[test_case::test_case("*", "s3:GetObject" => true ; "1")]
    #[test_case::test_case("", "s3:GetObject" => false ; "2")]
    #[test_case::test_case("", "" => true; "3")]
    #[test_case::test_case("s3:*", "s3:ListMultipartUploadParts" => true; "4")]
    #[test_case::test_case("s3:ListBucketMultipartUploads", "s3:ListBucket" => false; "5")]
    #[test_case::test_case("s3:ListBucket", "s3:ListBucket" => true; "6")]
    #[test_case::test_case("s3:ListBucketMultipartUploads", "s3:ListBucketMultipartUploads" => true; "7")]
    #[test_case::test_case("my-bucket/oo*", "my-bucket/oo" => true; "8")]
    #[test_case::test_case("my-bucket/In*", "my-bucket/India/Karnataka/" => true; "9")]
    #[test_case::test_case("my-bucket/In*", "my-bucket/Karnataka/India/" => false; "10")]
    #[test_case::test_case("my-bucket/In*/Ka*/Ban", "my-bucket/India/Karnataka/Ban" => true; "11")]
    #[test_case::test_case("my-bucket/In*/Ka*/Ban", "my-bucket/India/Karnataka/Ban/Ban/Ban/Ban/Ban" => true; "12")]
    #[test_case::test_case("my-bucket/In*/Ka*/Ban", "my-bucket/India/Karnataka/Area1/Area2/Area3/Ban" => true; "13")]
    #[test_case::test_case( "my-bucket/In*/Ka*/Ba", "my-bucket/India/State1/State2/Karnataka/Area1/Area2/Area3/Ban" => ignore["will fail"] true; "14")]
    #[test_case::test_case("my-bucket/In*/Ka*/Ban", "my-bucket/India/Karnataka/Bangalore" => false; "15")]
    #[test_case::test_case("my-bucket/In*/Ka*/Ban*", "my-bucket/India/Karnataka/Bangalore" => true; "16")]
    #[test_case::test_case("my-bucket/*", "my-bucket/India" => true; "17")]
    #[test_case::test_case("my-bucket/oo*", "my-bucket/odo" => false; "18")]
    #[test_case::test_case("my-bucket?/abc*", "mybucket/abc" => false; "19")]
    #[test_case::test_case("my-bucket?/abc*", "my-bucket1/abc" => true; "20")]
    #[test_case::test_case("my-?-bucket/abc*", "my--bucket/abc" => false; "21")]
    #[test_case::test_case("my-?-bucket/abc*", "my-1-bucket/abc" => true; "22")]
    #[test_case::test_case("my-?-bucket/abc*", "my-k-bucket/abc" => true; "23")]
    #[test_case::test_case("my??bucket/abc*", "mybucket/abc" => false; "24")]
    #[test_case::test_case("my??bucket/abc*", "my4abucket/abc" => true; "25")]
    #[test_case::test_case("my-bucket?abc*", "my-bucket/abc" => true; "26")]
    #[test_case::test_case("my-bucket/abc?efg", "my-bucket/abcdefg" => true; "27")]
    #[test_case::test_case("my-bucket/abc?efg", "my-bucket/abc/efg" => true; "28")]
    #[test_case::test_case("my-bucket/abc????", "my-bucket/abcde" => false; "29")]
    #[test_case::test_case("my-bucket/abc????", "my-bucket/abcdefg" => true; "30")]
    #[test_case::test_case("my-bucket/abc?", "my-bucket/abc" => false; "31")]
    #[test_case::test_case("my-bucket/abc?", "my-bucket/abcd" => true; "32")]
    #[test_case::test_case("my-bucket/abc?", "my-bucket/abcde" => false; "33")]
    #[test_case::test_case("my-bucket/mnop*?", "my-bucket/mnop" => false; "34")]
    #[test_case::test_case("my-bucket/mnop*?", "my-bucket/mnopqrst/mnopqr" => true; "35")]
    #[test_case::test_case("my-bucket/mnop*?", "my-bucket/mnopqrst/mnopqrs" => true; "36")]
    #[test_case::test_case("my-bucket/mnop*?", "my-bucket/mnop" => false; "37")]
    #[test_case::test_case("my-bucket/mnop*?", "my-bucket/mnopq" => true; "38")]
    #[test_case::test_case("my-bucket/mnop*?", "my-bucket/mnopqr" => true; "39")]
    #[test_case::test_case("my-bucket/mnop*?and", "my-bucket/mnopqand" => true; "40")]
    #[test_case::test_case("my-bucket/mnop*?and", "my-bucket/mnopand" => false; "41")]
    #[test_case::test_case("my-bucket/mnop*?and", "my-bucket/mnopqand" => true; "42")]
    #[test_case::test_case("my-bucket/mnop*?", "my-bucket/mn" => false; "43")]
    #[test_case::test_case("my-bucket/mnop*?", "my-bucket/mnopqrst/mnopqrs" => true; "44")]
    #[test_case::test_case("my-bucket/mnop*??", "my-bucket/mnopqrst" => true; "45")]
    #[test_case::test_case("my-bucket/mnop*qrst", "my-bucket/mnopabcdegqrst" => true; "46")]
    #[test_case::test_case("my-bucket/mnop*?and", "my-bucket/mnopqand" => true; "47")]
    #[test_case::test_case("my-bucket/mnop*?and", "my-bucket/mnopand" => false; "48")]
    #[test_case::test_case("my-bucket/mnop*?and?", "my-bucket/mnopqanda" => true; "49")]
    #[test_case::test_case("my-bucket/mnop*?and", "my-bucket/mnopqanda" => false; "50")]
    #[test_case::test_case("my-?-bucket/abc*", "my-bucket/mnopqanda" => false; "51")]
    #[test_case::test_case("a?", "a" => false; "52")]
    #[test_case::test_case("*", "mybucket/myobject" => true; "53")]
    fn test_is_match(pattern: &str, text: &str) -> bool {
        is_match(pattern, text)
    }

    #[test_case::test_case("*", "s3:GetObject" => true ; "1")]
    #[test_case::test_case("", "s3:GetObject" => false ; "2")]
    #[test_case::test_case("", "" => true ; "3")]
    #[test_case::test_case("s3:*", "s3:ListMultipartUploadParts" => true ; "4")]
    #[test_case::test_case("s3:ListBucketMultipartUploads", "s3:ListBucket" => false ; "5")]
    #[test_case::test_case("s3:ListBucket", "s3:ListBucket" => true ; "6")]
    #[test_case::test_case("s3:ListBucketMultipartUploads", "s3:ListBucketMultipartUploads" => true ; "7")]
    #[test_case::test_case("my-bucket/oo*", "my-bucket/oo" => true ; "8")]
    #[test_case::test_case("my-bucket/In*", "my-bucket/India/Karnataka/" => true ; "9")]
    #[test_case::test_case("my-bucket/In*", "my-bucket/Karnataka/India/" => false ; "10")]
    #[test_case::test_case("my-bucket/In*/Ka*/Ban", "my-bucket/India/Karnataka/Ban" => true ; "11")]
    #[test_case::test_case("my-bucket/In*/Ka*/Ban", "my-bucket/India/Karnataka/Ban/Ban/Ban/Ban/Ban" => true ; "12")]
    #[test_case::test_case("my-bucket/In*/Ka*/Ban", "my-bucket/India/Karnataka/Area1/Area2/Area3/Ban" => true ; "13")]
    #[test_case::test_case("my-bucket/In*/Ka*/Ban", "my-bucket/India/State1/State2/Karnataka/Area1/Area2/Area3/Ban" => true ; "14")]
    #[test_case::test_case("my-bucket/In*/Ka*/Ban", "my-bucket/India/Karnataka/Bangalore" => false ; "15")]
    #[test_case::test_case("my-bucket/In*/Ka*/Ban*", "my-bucket/India/Karnataka/Bangalore" => true ; "16")]
    #[test_case::test_case("my-bucket/*", "my-bucket/India" => true ; "17")]
    #[test_case::test_case("my-bucket/oo*", "my-bucket/odo" => false ; "18")]
    #[test_case::test_case("my-bucket/oo?*", "my-bucket/oo???" => true ; "19")]
    #[test_case::test_case("my-bucket/oo??*", "my-bucket/odo" => false ; "20")]
    #[test_case::test_case("?h?*", "?h?hello" => true ; "21")]
    #[test_case::test_case("a?", "a" => true ; "22")]
    fn test_is_simple_match(pattern: &str, text: &str) -> bool {
        is_simple_match(pattern, text)
    }

    #[test_case::test_case("", "" => true ; "1")]
    #[test_case::test_case("a", "" => true ; "2")]
    #[test_case::test_case("a", "b" => false ; "3")]
    #[test_case::test_case("abc", "ab" => true ; "4")]
    #[test_case::test_case("ab*", "ab" => true ; "5")]
    #[test_case::test_case("abc*", "ab" => true ; "6")]
    #[test_case::test_case("abc?", "ab" => true ; "7")]
    #[test_case::test_case("abc*", "abd" => false ; "8")]
    #[test_case::test_case("abc*c", "abcd" => true ; "9")]
    #[test_case::test_case("ab*??d", "abxxc" => true ; "10")]
    #[test_case::test_case("ab*??", "abxc" => true ; "11")]
    #[test_case::test_case("ab??", "abxc" => true ; "12")]
    #[test_case::test_case("ab??", "abx" => true ; "13")]
    #[test_case::test_case("ab??d", "abcxd" => true ; "14")]
    #[test_case::test_case("ab??d", "abcxdd" => false ; "15")]
    #[test_case::test_case("", "b" => false ; "16")]
    fn test_is_match_as_pattern_prefix(pattern: &str, text: &str) -> bool {
        is_match_as_pattern_prefix(pattern, text)
    }
}
