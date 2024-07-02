{
  description = "sheep";
  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.poetry2nix.url = "github:nix-community/poetry2nix";
  inputs.poetry2nix.inputs.nixpkgs.follows = "nixpkgs";

  outputs = { self, nixpkgs, flake-utils, poetry2nix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        p2n = poetry2nix.lib.mkPoetry2Nix { inherit pkgs; };
      in {
        packages.default = p2n.mkPoetryApplication { projectDir = self; };
        devShells.default = pkgs.mkShell {
          inputsFrom = [ self.packages.${system}.default ];
          packages = [ pkgs.duckdb pkgs.poetry pkgs.zstd pkgs.python311Packages.pyqt5 ];
          QT_PLUGIN_PATH = with pkgs.qt5; "${qtbase}/${qtbase.qtPluginPrefix}:${pkgs.libsForQt5.qtwayland}/${qtbase.qtPluginPrefix}";
        };
      }
    );
}
  
