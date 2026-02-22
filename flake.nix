# Nix flake for building RustFS
#
# Prerequisites:
#   Install Nix: https://nixos.org/download/
#   Enable flakes: https://nixos.wiki/wiki/Flakes#Enable_flakes
#
# Usage:
#   nix build          # Build rustfs binary
#   nix run            # Build and run rustfs
#   ./result/bin/rustfs --help
{
  description = "RustFS - High-performance S3-compatible object storage";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
      ...
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      packages = forAllSystems (
        system:
        let
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs { inherit system overlays; };

          # Use the latest stable rust toolchain
          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
              "clippy"
              "rustfmt"
            ];
          };

          rustPlatform = pkgs.makeRustPlatform {
            cargo = rustToolchain;
            rustc = rustToolchain;
          };
        in
        {
          default = rustPlatform.buildRustPackage {
            pname = "rustfs";
            version = "0.0.5";

            src = ./.;

            cargoLock = {
              lockFile = ./Cargo.lock;
              allowBuiltinFetchGit = true;
            };

            nativeBuildInputs = with pkgs; [
              pkg-config
              protobuf
            ];

            buildInputs = with pkgs; [
              openssl
            ];

            cargoBuildFlags = [
              "--package"
              "rustfs"
            ];

            # Set environment variables for build
            PROTOC = "${pkgs.protobuf}/bin/protoc";

            doCheck = false;

            meta = {
              description = "High-performance S3-compatible object storage";
              homepage = "https://rustfs.com";
              license = pkgs.lib.licenses.asl20;
              mainProgram = "rustfs";
            };
          };
        }
      );

      devShells = forAllSystems (
        system:
        let
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs { inherit system overlays; };
          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
              "clippy"
              "rustfmt"
            ];
          };
        in
        {
          default = pkgs.mkShell {
            packages = [
              rustToolchain
              pkgs.pkg-config
              pkgs.protobuf
              pkgs.openssl
            ];

            PROTOC = "${pkgs.protobuf}/bin/protoc";
          };
        }
      );
    };
}
