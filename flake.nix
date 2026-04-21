{
  description = "Switchboard - browser-based control panel for small Kubernetes clusters";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems =
        f:
        nixpkgs.lib.genAttrs systems (
          system:
          f {
            inherit system;
            pkgs = import nixpkgs { inherit system; };
          }
        );
    in
    {
      packages = forAllSystems (
        { pkgs, ... }:
        let
          bin = pkgs.buildGoModule {
            pname = "switchboard";
            version = "0.1.0";
            src = ./.;
            vendorHash = null;
            subPackages = [ "." ];
          };

          dockerImage = pkgs.dockerTools.buildImage {
            name = "switchboard";
            tag = "latest";
            copyToRoot = [ bin ];
            config = {
              Cmd = [ "${bin}/bin/switchboard" ];
              ExposedPorts = {
                "8080/tcp" = { };
              };
            };
          };

          manifests = pkgs.runCommand "switchboard-manifests" { } ''
            mkdir -p $out
            cp ${./deploy/manifests.yaml} $out/manifests.yaml
          '';
        in
        {
          default = bin;
          switchboard = bin;
          dockerImage = dockerImage;
          manifests = manifests;
        }
      );

      devShells = forAllSystems (
        { pkgs, ... }:
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              go
              gopls
              gotools
            ];
          };
        }
      );

      formatter = forAllSystems ({ pkgs, ... }: pkgs.nixfmt);
    };
}
