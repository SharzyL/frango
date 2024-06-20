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
          packages.default = frango;
          legacyPackages = pkgs;
          devShell = frango.overrideAttrs (oldAttrs: {
            nativeBuildInputs = oldAttrs.nativeBuildInputs ++ (with python3.pkgs; [
              (pkgs.pdm.override { inherit python3; })
              grpcio-tools
              mypy
              pytest
              types-protobuf
            ]);
          });
        }
      )
    // {
      inherit inputs;
      overlays.default = overlay;
    };
}
