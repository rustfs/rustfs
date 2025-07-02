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

pub const ERR_IGNORE_FILE_CONTRIB: &str = "ignore this file's contribution toward data-usage";
pub const ERR_SKIP_FILE: &str = "skip this file";
pub const ERR_HEAL_STOP_SIGNALLED: &str = "heal stop signaled";
pub const ERR_HEAL_IDLE_TIMEOUT: &str = "healing results were not consumed for too long";
pub const ERR_RETRY_HEALING: &str = "some items failed to heal, we will retry healing this drive again";
