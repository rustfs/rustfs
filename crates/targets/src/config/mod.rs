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

mod common;
mod loader;
mod target_args;

pub use loader::{
    collect_env_target_instance_ids, collect_env_target_instance_ids_from_env, collect_target_configs,
    collect_target_configs_from_env,
};
pub use target_args::{
    build_kafka_args, build_mqtt_args, build_mysql_args, build_nats_args, build_pulsar_args, build_webhook_args,
    validate_kafka_config, validate_mqtt_config, validate_mysql_config, validate_nats_config, validate_pulsar_config,
    validate_webhook_config,
};
