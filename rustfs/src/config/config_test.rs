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
        assert_eq!(opt.external_address, ":9000"); // Now defaults to DEFAULT_ADDRESS
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
    fn test_external_address_configuration() {
        // Test external address configuration for Docker
        let args = vec!["rustfs", "/test/volume", "--external-address", ":9020"];
        let opt = Opt::parse_from(args);

        assert_eq!(opt.external_address, ":9020".to_string());
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
}
