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
    fenix.url = "github:nix-community/fenix";
  };

  outputs =
    inputs@{ nixpkgs, ... }:
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
          pkgs = nixpkgs.legacyPackages.${system};
          # access to fenix packages
          fnx = inputs.fenix.packages.${system};
          # create custom rust toolchain with latest fenix cargo/rustc
          rustPlatform = pkgs.makeRustPlatform {
            cargo = fnx.stable.cargo;
            rustc = fnx.stable.rustc;
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

            nativeBuildInputs = [
              pkgs.pkg-config
              pkgs.protobuf
            ];

            buildInputs = [ pkgs.openssl ];

            cargoBuildFlags = [
              "--package"
              "rustfs"
            ];

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
    };
}
