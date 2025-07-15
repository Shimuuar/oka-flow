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
  #---
  hask = pkgs.oka.haskell94.ghcWithPackages (p: with p; [
    aeson
    async
    base16-bytestring
    bytestring
    containers
    cryptohash-sha1
    directory
    effectful-core
    filepath
    fixed-vector
    generic-arbitrary
    lens
    mtl
    optparse-applicative
    process
    quickcheck-instances
    random
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
  pkgs_py = pkgs.python3.withPackages (ps: with ps;
    [ pip build pydantic jsbeautifier mypy
    ]);
in
pkgs.mkShell {
  buildInputs = [
    hask
    pkgs_py
  ];
}
