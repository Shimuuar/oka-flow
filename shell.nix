let
  oka-nix = import ../oka-nix {};
  pkgs    = oka-nix.pkgs { inherit config overlays; };
  config = {
    allowUnfree = true;
  };
  overlays    = [ oka-nix.overlay ];
in
pkgs.haskellPackages.shellFor {
  packages = hs: [
    (hs.callCabal2nix "oka-metadata" ./. {})
  ];
}
