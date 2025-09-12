# /etc/nixos/my-rust-app/flake.nix
{
  description = "A standalone flake for my Rust application";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    crane.url = "github:ipetkov/crane";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, crane, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        craneLib = crane.packages.${system}.crane;
      in
      {
        # Package for the Rust executable
        packages.default = craneLib.buildPackage {
          src = self; # 'self' refers to the flake's root directory
          buildInputs = [ ]; # Add system dependencies here if needed
        };

        # App for 'nix run'
        apps.default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/signalkCompressor";
        };

      }) // { # Outputs not dependent on 'system'

        # NixOS module for the systemd service
        nixosModules.default = { config, pkgs, ... }: {
          options.services.my-rust-app = {
            enable = pkgs.lib.mkEnableOption "Enable my custom rust service";
          };

          config = pkgs.lib.mkIf config.services.my-rust-app.enable {
            systemd.services.my-rust-app = {
              description = "A custom service written in Rust";
              wantedBy = [ "multi-user.target" ];
              serviceConfig = {
                # This path correctly references the package from this flake
                ExecStart = "${self.packages.${pkgs.system}.default}/bin/signalkCompressor";
                User = "nobody";
                Group = "nogroup";
                Restart = "on-failure";
              };
            };
          };
        };
      };
}