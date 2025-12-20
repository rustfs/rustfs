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

#[cfg(test)]
#[allow(clippy::unsafe_op_in_unsafe_fn)]
mod tests {
    use crate::config::Opt;
    use clap::Parser;

    #[test]
    fn test_default_console_configuration() {
        // Test that default console configuration is correct
        let args = vec!["rustfs", "/test/volume"];
        let opt = Opt::parse_from(args);

        assert!(opt.console_enable);
        assert_eq!(opt.console_address, ":9001");
        assert_eq!(opt.address, ":9000");
    }

    #[test]
    fn test_custom_console_configuration() {
        // Test custom console configuration
        let args = vec![
            "rustfs",
            "/test/volume",
            "--console-address",
            ":8080",
            "--address",
            ":8000",
            "--console-enable",
            "false",
        ];
        let opt = Opt::parse_from(args);

        assert!(opt.console_enable);
        assert_eq!(opt.console_address, ":8080");
        assert_eq!(opt.address, ":8000");
    }

    #[test]
    fn test_console_and_endpoint_ports_different() {
        // Ensure console and endpoint use different default ports
        let args = vec!["rustfs", "/test/volume"];
        let opt = Opt::parse_from(args);

        // Parse port numbers from addresses
        let endpoint_port: u16 = opt.address.trim_start_matches(':').parse().expect("Invalid endpoint port");
        let console_port: u16 = opt
            .console_address
            .trim_start_matches(':')
            .parse()
            .expect("Invalid console port");

        assert_ne!(endpoint_port, console_port, "Console and endpoint should use different ports");
        assert_eq!(endpoint_port, 9000);
        assert_eq!(console_port, 9001);
    }

    #[test]
    fn test_volumes_and_disk_layout_parsing() {
        use rustfs_ecstore::disks_layout::DisksLayout;

        // Test case 1: Single volume path
        let args = vec!["rustfs", "/data/vol1"];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 1);
        assert_eq!(opt.volumes[0], "/data/vol1");
        
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse single volume");
        assert!(!layout.is_empty_layout());
        assert!(layout.is_single_drive_layout());
        assert_eq!(layout.get_single_drive_layout(), "/data/vol1");

        // Test case 2: Multiple volume paths (space-separated via env)
        let args = vec!["rustfs", "/data/vol1", "/data/vol2", "/data/vol3", "/data/vol4"];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 4);
        
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse multiple volumes");
        assert!(!layout.is_empty_layout());
        assert!(!layout.is_single_drive_layout());
        assert_eq!(layout.get_set_count(0), 1);
        assert_eq!(layout.get_drives_per_set(0), 4);

        // Test case 3: Ellipses pattern - simple range
        let args = vec!["rustfs", "/data/vol{1...4}"];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 1);
        assert_eq!(opt.volumes[0], "/data/vol{1...4}");
        
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse ellipses pattern");
        assert!(!layout.is_empty_layout());
        assert_eq!(layout.get_set_count(0), 1);
        assert_eq!(layout.get_drives_per_set(0), 4);

        // Test case 4: Ellipses pattern - larger range that creates multiple sets
        let args = vec!["rustfs", "/data/vol{1...16}"];
        let opt = Opt::parse_from(args);
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse ellipses with multiple sets");
        assert!(!layout.is_empty_layout());
        assert_eq!(layout.get_drives_per_set(0), 16);

        // Test case 5: Distributed setup pattern
        let args = vec!["rustfs", "http://server{1...4}/data/vol{1...4}"];
        let opt = Opt::parse_from(args);
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse distributed pattern");
        assert!(!layout.is_empty_layout());
        assert_eq!(layout.get_drives_per_set(0), 16);

        // Test case 6: Multiple pools (legacy: false)
        let args = vec!["rustfs", "http://server1/data{1...4}", "http://server2/data{1...4}"];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 2);
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse multiple pools");
        assert!(!layout.legacy);
        assert_eq!(layout.pools.len(), 2);

        // Test case 7: Minimum valid drives for erasure coding (2 drives minimum)
        let args = vec!["rustfs", "/data/vol1", "/data/vol2"];
        let opt = Opt::parse_from(args);
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Should succeed with 2 drives");
        assert_eq!(layout.get_drives_per_set(0), 2);

        // Test case 8: Invalid - single drive not enough for erasure coding
        let args = vec!["rustfs", "/data/vol1"];
        let opt = Opt::parse_from(args);
        // Single drive is special case and should succeed for single drive layout
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Single drive should work");
        assert!(layout.is_single_drive_layout());

        // Test case 9: Command line with both address and volumes
        let args = vec![
            "rustfs",
            "/data/vol{1...8}",
            "--address",
            ":9000",
            "--console-address",
            ":9001",
        ];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 1);
        assert_eq!(opt.address, ":9000");
        assert_eq!(opt.console_address, ":9001");
        
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse with address args");
        assert!(!layout.is_empty_layout());
        assert_eq!(layout.get_drives_per_set(0), 8);

        // Test case 10: Multiple ellipses in single argument - nested pattern
        let args = vec!["rustfs", "/data{0...3}/vol{0...4}"];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 1);
        assert_eq!(opt.volumes[0], "/data{0...3}/vol{0...4}");
        
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse nested ellipses pattern");
        assert!(!layout.is_empty_layout());
        // 4 data dirs * 5 vols = 20 drives
        let total_drives = layout.get_set_count(0) * layout.get_drives_per_set(0);
        assert_eq!(total_drives, 20, "Expected 20 drives from /data{{0...3}}/vol{{0...4}}");

        // Test case 10: Multiple pools with nested ellipses patterns
        let args = vec!["rustfs", "/data{0...3}/vol{0...4}", "/data{4...7}/vol{0...4}"];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 2);
        
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse multiple pools with nested patterns");
        assert!(!layout.legacy);
        assert_eq!(layout.pools.len(), 2);
        
        // Each pool should have 20 drives (4 * 5)
        let pool0_drives = layout.get_set_count(0) * layout.get_drives_per_set(0);
        let pool1_drives = layout.get_set_count(1) * layout.get_drives_per_set(1);
        assert_eq!(pool0_drives, 20, "Pool 0 should have 20 drives");
        assert_eq!(pool1_drives, 20, "Pool 1 should have 20 drives");

        // Test case 11: Complex distributed pattern with multiple ellipses
        let args = vec!["rustfs", "http://server{1...2}.local/disk{1...8}"];
        let opt = Opt::parse_from(args);
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse distributed nested pattern");
        assert!(!layout.is_empty_layout());
        // 2 servers * 8 disks = 16 drives
        let total_drives = layout.get_set_count(0) * layout.get_drives_per_set(0);
        assert_eq!(total_drives, 16, "Expected 16 drives from server{{1...2}}/disk{{1...8}}");

        // Test case 12: Zero-padded patterns
        let args = vec!["rustfs", "/data/vol{01...16}"];
        let opt = Opt::parse_from(args);
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse zero-padded pattern");
        assert!(!layout.is_empty_layout());
        assert_eq!(layout.get_drives_per_set(0), 16);
    }

    #[test]
    #[allow(unsafe_code)]
    fn test_rustfs_volumes_env_variable() {
        use rustfs_ecstore::disks_layout::DisksLayout;
        use std::env;

        // Test case 1: Single volume via environment variable
        unsafe { env::set_var("RUSTFS_VOLUMES", "/data/vol1"); }
        let args = vec!["rustfs"];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 1);
        assert_eq!(opt.volumes[0], "/data/vol1");
        
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse single volume from env");
        assert!(layout.is_single_drive_layout());
        unsafe { env::remove_var("RUSTFS_VOLUMES"); }

        // Test case 2: Multiple volumes via environment variable (space-separated)
        unsafe { env::set_var("RUSTFS_VOLUMES", "/data/vol1 /data/vol2 /data/vol3 /data/vol4"); }
        let args = vec!["rustfs"];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 4);
        assert_eq!(opt.volumes[0], "/data/vol1");
        assert_eq!(opt.volumes[1], "/data/vol2");
        assert_eq!(opt.volumes[2], "/data/vol3");
        assert_eq!(opt.volumes[3], "/data/vol4");
        
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse multiple volumes from env");
        assert!(!layout.is_single_drive_layout());
        assert_eq!(layout.get_drives_per_set(0), 4);
        unsafe { env::remove_var("RUSTFS_VOLUMES"); }

        // Test case 3: Ellipses pattern via environment variable
        unsafe { env::set_var("RUSTFS_VOLUMES", "/data/vol{1...4}"); }
        let args = vec!["rustfs"];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 1);
        assert_eq!(opt.volumes[0], "/data/vol{1...4}");
        
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse ellipses pattern from env");
        assert_eq!(layout.get_drives_per_set(0), 4);
        unsafe { env::remove_var("RUSTFS_VOLUMES"); }

        // Test case 4: Larger range with ellipses
        unsafe { env::set_var("RUSTFS_VOLUMES", "/data/vol{1...16}"); }
        let args = vec!["rustfs"];
        let opt = Opt::parse_from(args);
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse large range from env");
        assert_eq!(layout.get_drives_per_set(0), 16);
        unsafe { env::remove_var("RUSTFS_VOLUMES"); }

        // Test case 5: Distributed setup pattern
        unsafe { env::set_var("RUSTFS_VOLUMES", "http://server{1...4}/data/vol{1...4}"); }
        let args = vec!["rustfs"];
        let opt = Opt::parse_from(args);
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse distributed pattern from env");
        assert_eq!(layout.get_drives_per_set(0), 16);
        unsafe { env::remove_var("RUSTFS_VOLUMES"); }

        // Test case 6: Multiple pools via environment variable (space-separated)
        unsafe { env::set_var("RUSTFS_VOLUMES", "http://server1/data{1...4} http://server2/data{1...4}"); }
        let args = vec!["rustfs"];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 2);
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse multiple pools from env");
        assert!(!layout.legacy);
        assert_eq!(layout.pools.len(), 2);
        unsafe { env::remove_var("RUSTFS_VOLUMES"); }

        // Test case 7: Nested ellipses pattern
        unsafe { env::set_var("RUSTFS_VOLUMES", "/data{0...3}/vol{0...4}"); }
        let args = vec!["rustfs"];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 1);
        assert_eq!(opt.volumes[0], "/data{0...3}/vol{0...4}");
        
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse nested ellipses from env");
        let total_drives = layout.get_set_count(0) * layout.get_drives_per_set(0);
        assert_eq!(total_drives, 20, "Expected 20 drives from /data{{0...3}}/vol{{0...4}}");
        unsafe { env::remove_var("RUSTFS_VOLUMES"); }

        // Test case 8: Multiple pools with nested ellipses
        unsafe { env::set_var("RUSTFS_VOLUMES", "/data{0...3}/vol{0...4} /data{4...7}/vol{0...4}"); }
        let args = vec!["rustfs"];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 2);
        
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse multiple pools with nested patterns from env");
        assert_eq!(layout.pools.len(), 2);
        let pool0_drives = layout.get_set_count(0) * layout.get_drives_per_set(0);
        let pool1_drives = layout.get_set_count(1) * layout.get_drives_per_set(1);
        assert_eq!(pool0_drives, 20, "Pool 0 should have 20 drives");
        assert_eq!(pool1_drives, 20, "Pool 1 should have 20 drives");
        unsafe { env::remove_var("RUSTFS_VOLUMES"); }

        // Test case 9: Complex distributed pattern with multiple ellipses
        unsafe { env::set_var("RUSTFS_VOLUMES", "http://server{1...2}.local/disk{1...8}"); }
        let args = vec!["rustfs"];
        let opt = Opt::parse_from(args);
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse distributed nested pattern from env");
        let total_drives = layout.get_set_count(0) * layout.get_drives_per_set(0);
        assert_eq!(total_drives, 16, "Expected 16 drives from server{{1...2}}/disk{{1...8}}");
        unsafe { env::remove_var("RUSTFS_VOLUMES"); }

        // Test case 10: Zero-padded patterns
        unsafe { env::set_var("RUSTFS_VOLUMES", "/data/vol{01...16}"); }
        let args = vec!["rustfs"];
        let opt = Opt::parse_from(args);
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse zero-padded pattern from env");
        assert_eq!(layout.get_drives_per_set(0), 16);
        unsafe { env::remove_var("RUSTFS_VOLUMES"); }

        // Test case 11: Environment variable with additional CLI options
        unsafe { env::set_var("RUSTFS_VOLUMES", "/data/vol{1...8}"); }
        let args = vec![
            "rustfs",
            "--address",
            ":9000",
            "--console-address",
            ":9001",
        ];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 1);
        assert_eq!(opt.address, ":9000");
        assert_eq!(opt.console_address, ":9001");
        
        let layout = DisksLayout::from_volumes(&opt.volumes).expect("Failed to parse env with CLI options");
        assert_eq!(layout.get_drives_per_set(0), 8);
        unsafe { env::remove_var("RUSTFS_VOLUMES"); }

        // Test case 12: Command line argument overrides environment variable
        unsafe { env::set_var("RUSTFS_VOLUMES", "/data/vol1"); }
        let args = vec!["rustfs", "/override/vol1"];
        let opt = Opt::parse_from(args);
        assert_eq!(opt.volumes.len(), 1);
        // CLI argument should override environment variable
        assert_eq!(opt.volumes[0], "/override/vol1");
        unsafe { env::remove_var("RUSTFS_VOLUMES"); }
    }

    #[test]
    fn test_server_domains_parsing() {
        // Test case 1: server domains without ports
        let args = vec![
            "rustfs",
            "/data/vol1",
            "--server-domains",
            "example.com,api.example.com,cdn.example.com",
        ];
        let opt = Opt::parse_from(args);

        assert_eq!(opt.server_domains.len(), 3);
        assert_eq!(opt.server_domains[0], "example.com");
        assert_eq!(opt.server_domains[1], "api.example.com");
        assert_eq!(opt.server_domains[2], "cdn.example.com");

        // Test case 2: server domains with ports
        let args = vec![
            "rustfs",
            "/data/vol1",
            "--server-domains",
            "example.com:9000,api.example.com:8080,cdn.example.com:443",
        ];
        let opt = Opt::parse_from(args);

        assert_eq!(opt.server_domains.len(), 3);
        assert_eq!(opt.server_domains[0], "example.com:9000");
        assert_eq!(opt.server_domains[1], "api.example.com:8080");
        assert_eq!(opt.server_domains[2], "cdn.example.com:443");

        // Test case 3: mixed server domains (with and without ports)
        let args = vec![
            "rustfs",
            "/data/vol1",
            "--server-domains",
            "example.com,api.example.com:9000,cdn.example.com,storage.example.com:8443",
        ];
        let opt = Opt::parse_from(args);

        assert_eq!(opt.server_domains.len(), 4);
        assert_eq!(opt.server_domains[0], "example.com");
        assert_eq!(opt.server_domains[1], "api.example.com:9000");
        assert_eq!(opt.server_domains[2], "cdn.example.com");
        assert_eq!(opt.server_domains[3], "storage.example.com:8443");

        // Test case 4: single domain with port
        let args = vec![
            "rustfs",
            "/data/vol1",
            "--server-domains",
            "example.com:9000",
        ];
        let opt = Opt::parse_from(args);

        assert_eq!(opt.server_domains.len(), 1);
        assert_eq!(opt.server_domains[0], "example.com:9000");

        // Test case 5: localhost with different ports
        let args = vec![
            "rustfs",
            "/data/vol1",
            "--server-domains",
            "localhost:9000,127.0.0.1:9000,localhost",
        ];
        let opt = Opt::parse_from(args);

        assert_eq!(opt.server_domains.len(), 3);
        assert_eq!(opt.server_domains[0], "localhost:9000");
        assert_eq!(opt.server_domains[1], "127.0.0.1:9000");
        assert_eq!(opt.server_domains[2], "localhost");
    }

}
