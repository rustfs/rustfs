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
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      nixosModules.rustfs = import ./nix/rustfs-module.nix {
        defaultPackage = system: self.packages.${system}.rustfs;
      };
      nixosModules.default = self.nixosModules.rustfs;

      overlays.default = final: prev:
        let
          packages = self.packages.${prev.stdenv.hostPlatform.system};
        in
        {
          rustfs = packages.rustfs;
        }
        // prev.lib.optionalAttrs (builtins.hasAttr "rustfs-client" packages) {
          rustfs-client = packages.rustfs-client;
          rc = packages.rustfs-client;
        };

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

          clientVersion = "0.1.32";

          rustfs = rustPlatform.buildRustPackage {
            pname = "rustfs";
            version = "1.0.0-rc.4";

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

            PROTOC = "${pkgs.protobuf}/bin/protoc";

            doCheck = false;

            meta = {
              description = "High-performance S3-compatible object storage";
              homepage = "https://rustfs.com";
              license = pkgs.lib.licenses.asl20;
              mainProgram = "rustfs";
            };
          };

          clientAssets = {
            "x86_64-linux" = {
              name = "rustfs-cli-linux-amd64-v${clientVersion}.tar.gz";
              hash = "sha256-qwDZNwedy28ce0HTS7+q0OsL1PchhnLLy3wzZS0cRt8=";
            };
            "aarch64-linux" = {
              name = "rustfs-cli-linux-arm64-v${clientVersion}.tar.gz";
              hash = "sha256-1T1M9Q3lcy9IJo/n5eQezmbTaEgHVJbx1QFCaTX3BYY=";
            };
          };

          clientSupported = builtins.hasAttr system clientAssets;

          clientPackage =
            if clientSupported then
              let
                asset = clientAssets.${system};
              in
              pkgs.stdenvNoCC.mkDerivation {
                pname = "rustfs-cli";
                version = clientVersion;
                src = pkgs.fetchurl {
                  url = "https://github.com/rustfs/cli/releases/download/v${clientVersion}/${asset.name}";
                  inherit (asset) hash;
                };
                sourceRoot = ".";
                installPhase = ''
                  runHook preInstall
                  install -Dm755 rc "$out/bin/rc"
                  runHook postInstall
                '';
                meta = {
                  description = "RustFS S3-compatible command-line client";
                  homepage = "https://github.com/rustfs/cli";
                  license = pkgs.lib.licenses.asl20;
                  mainProgram = "rc";
                };
              }
            else
              null;
        in
        {
          inherit rustfs;
          default = rustfs;
        }
        // pkgs.lib.optionalAttrs clientSupported {
          rustfs-client = clientPackage;
          rc = clientPackage;
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
