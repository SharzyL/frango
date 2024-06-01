{
  inputs = {
    nixpkgs.url = "nixpkgs";
    flake-utils.url = "flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }@inputs:
    let
      overlay = final: prev: {
        frango = final.python3.pkgs.callPackage ./pkg.nix { };
        rraft-py = final.python3.pkgs.callPackage ./nix/rraft-py { };
      };
    in
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs { inherit system; overlays = [ overlay ]; };
          frango = pkgs.frango;
        in
        {
          packages.default = frango;
          legacyPackages = pkgs;
          devShell = frango.overrideAttrs (oldAttrs: {
            nativeBuildInputs = oldAttrs.nativeBuildInputs ++ [
              pkgs.pdm
              pkgs.python3Packages.grpcio-tools
            ];
          });
        }
      )
    // {
      inherit inputs;
      overlays.default = overlay;
    };
}
