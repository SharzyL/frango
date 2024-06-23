{
  inputs = {
    nixpkgs.url = "nixpkgs";
    flake-utils.url = "flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }@inputs:
    let
      overlay = final: prev: {
        frango = final.python312.pkgs.callPackage ./pkg.nix { };
        rraft-py = final.python312.pkgs.callPackage ./nix/rraft-py { };
      };
    in
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs { inherit system; overlays = [ overlay ]; };
          frango = pkgs.frango;
          python3 = pkgs.python312;
        in
        {
          legacyPackages = pkgs;

          packages.default = frango;
          packages.frango-docker-image = pkgs.dockerTools.buildLayeredImage {
            name = "frango";
            tag = "latest";
            contents = with pkgs; [ frango bash ];
            config.Cmd = [ "/bin/frango-node" ];
          };

          devShell = frango.overrideAttrs (oldAttrs: {
            nativeBuildInputs = oldAttrs.nativeBuildInputs ++ (with python3.pkgs; [
              (pkgs.pdm.override { inherit python3; })
              grpcio-tools
              mypy
              pytest
              types-protobuf
            ]);

            shellHook = ''
              export PYTHONPATH=$PYTHONPATH:.
            '';
          });
        }
      )
    // {
      inherit inputs;
      overlays.default = overlay;
    };
}
