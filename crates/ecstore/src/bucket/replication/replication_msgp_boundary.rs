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

use std::io::{Read, Write};

use time::OffsetDateTime;

use super::replication_error_boundary::Result;
use crate::bucket::msgp_decode;

pub(crate) struct ReplicationMsgpCodec;

impl ReplicationMsgpCodec {
    pub(crate) fn read_ext8_time<R: Read>(rd: &mut R) -> Result<OffsetDateTime> {
        msgp_decode::read_msgp_ext8_time(rd)
    }

    pub(crate) fn skip_value<R: Read>(rd: &mut R) -> Result<()> {
        msgp_decode::skip_msgp_value(rd)
    }

    pub(crate) fn write_time<W: Write>(wr: &mut W, time: OffsetDateTime) -> Result<()> {
        msgp_decode::write_msgp_time(wr, time)
    }
}
