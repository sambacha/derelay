{
  description = "A Nix flake for the derelay Go project";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable"; # Using unstable for potentially newer Go versions
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        # Placeholder - needs calculation after first build attempt
        # Run `nix build` and replace this with the hash Nix provides.
        vendorSha256 = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        # Attempt to get version from go.mod or set manually
        # This part might need refinement depending on go.mod content
        version = "0.0.1"; # Placeholder version
      in
      {
        # The build output
        packages.default = pkgs.buildGoModule {
          pname = "derelay";
          inherit version;

          src = ./.;

          inherit vendorSha256;

          # No CGO needed based on user confirmation
          # No specific Go version needed, will use nixpkgs default

          # Assuming the main package is at the root
          # subPackages = [ "." ]; # Usually not needed if main.go is at root
        };

        # Development environment
        devShells.default = pkgs.mkShell {
          buildInputs = [
            pkgs.go
            pkgs.gopls # Language server
            pkgs.golangci-lint # Linter
            # No other dev tools requested
          ];
        };
      });
}
