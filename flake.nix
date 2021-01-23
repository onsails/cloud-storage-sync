{
  inputs = {
    nixpkgs-unstable.url = github:NixOS/nixpkgs/nixpkgs-unstable;
    utils.url = github:numtide/flake-utils;
  };

  outputs = { self, nixpkgs-unstable, utils }:
    utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs-unstable { inherit system; };
        devShell = pkgs.mkShell {
          buildInputs = with pkgs; [
            cargo
            clippy
            rustc
            rustfmt

            cargo-release

            pkg-config
            openssl
          ];
        };

      in
      {
        packages = {
          devShell = devShell.inputDerivation;
        };

        inherit devShell;
      }
    );
}
