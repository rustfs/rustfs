//  Copyright 2024 RustFS Team
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

// use figment::{
//     Figment,
//     providers::{Env, Format, Json},
// };
// use logger::{AuditEntry, AuditLogger, Config, LogEntry, Trace};
// use tokio::time::{Duration, sleep};
//
// #[tokio::main]
// async fn main() {
//     // 1. 从文件和环境变量加载配置
//     // 环境变量会覆盖文件中的设置
//     // 例如：`AUDIT_WEBHOOK_DEFAULT_ENABLED=true AUDIT_WEBHOOK_DEFAULT_ENDPOINT=http://localhost:3000/logs`
//     let config: Config = Figment::new()
//         .merge(Json::file("config.json"))
//         .merge(Env::prefixed("").split("__"))
//         .extract()
//         .expect("Failed to load configuration");
//
//     println!("Loaded config: {:?}", config);
//
//     // 2. 初始化记录器
//     let logger = AuditLogger::new(&config);
//
//     // 3. 发送一些日志
//     println!("\n--- Sending logs ---");
//     for i in 0..5 {
//         let log_entry = LogEntry {
//             deployment_id: "global-deployment-id".to_string(),
//             level: "INFO".to_string(),
//             message: format!("This is log message #{}", i),
//             trace: Some(Trace {
//                 message: "An operation was performed".to_string(),
//                 source: vec!["main.rs:45:main()".to_string()],
//                 variables: Default::default(),
//             }),
//             time: chrono::Utc::now(),
//             request_id: uuid::Uuid::new_v4().to_string(),
//         };
//         logger.log(log_entry).await;
//
//         let audit_entry = AuditEntry::new("GetObject", "my-bucket", &format!("object-{}", i));
//         logger.log(audit_entry).await;
//
//         sleep(Duration::from_millis(100)).await;
//     }
//     println!("--- Finished sending logs ---\n");
//
//     // 4. 优雅地关闭
//     // 这将确保所有缓冲的/队列中的日志在退出前被发送
//     logger.shutdown().await;
// }
