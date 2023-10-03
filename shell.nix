let
  oka-nix = import ../oka-nix {};
  pkgs    = oka-nix.pkgs { inherit config overlays; };
  config = {
    allowUnfree = true;
  };
  overlays = [
    oka-nix.hackage
    oka-nix.overlay
  ];
in
pkgs.oka.haskell94.shellFor {
  packages = hs: [
    (hs.callCabal2nix "oka-metadata" ./. {})
  ];
}
