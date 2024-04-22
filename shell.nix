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
  #--
  pkgs_hask = pkgs.oka.haskell94.ghcWithPackages (p: with p; [
     aeson
     async
     base16-bytestring
     bytestring
     containers
     cryptohash-sha1
     directory
     filepath
     fixed-vector
     generic-arbitrary
     lens
     mtl
     operational
     process
     quickcheck-instances
     scientific
     stm
     tasty
     tasty-hunit
     tasty-quickcheck
     temporary
     text
     these
     time
     transformers
     typed-process
     unix
     unordered-containers
     vector
     yaml
  ]);
in
pkgs.mkShell {
  buildInputs = [
    pkgs_hask
  ];
}
