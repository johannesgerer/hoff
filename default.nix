# -*- compile-command: "direnv exec . bash -c 'pip list; cabal exec -- ghc-pkg list'"; -*-
# -*- compile-command: "nix-shell --run 'cabal exec -- ghc-pkg list'"; -*-
{ sources ? import ./nix/sources.nix {}, pkgs ? null }:
  # https://github.com/NixOS/nixpkgs/blob/master/pkgs/development/haskell-modules/make-package-set.nix
let pkgs2 = if pkgs != null then pkgs else import sources.nixpkgs { overlays = [cabalHashes]; }; 
    cabalHashes = self: super: { inherit (sources) all-cabal-hashes; };
in
with pkgs2.haskell.lib.compose;
let laPatch = pkgs2.writeText "laPatch" ''
diff --git a/large-anon.cabal b/large-anon.cabal
index 01d341f..0802f6f 100644
--- a/large-anon.cabal
+++ b/large-anon.cabal
@@ -206,10 +206,15 @@ Executable large-anon-testsuite-fourmolu-preprocessor
       -Wall

   -- Fourmolu is only compatible with RDP syntax from ghc 9.2 and up.
-  if impl(ghc < 9.2)
+  if impl(ghc < 9.2) || flag(disableFourmoluExec)
     buildable: False

 Flag debug
   Description: Enable internal debugging features
   Default: False
   Manual: True
+
+Flag disableFourmoluExec
+  Description: Disable executable large-anon-testsuite-fourmolu-preprocessor
+  Default: False
+  Manual: True
'';
    pkgs = pkgs2;
    mach-nix = import sources.mach-nix {
      pkgs = import (import "${sources.mach-nix}/mach_nix/nix/nixpkgs-src.nix") {
        config = { allowUnfree = true; }; overlays = []; };
    };
    pyEnv = mach-nix.mkPython {
      # _.tomli.buildInputs.add = [pkgs.python310Packages.flit-core];
      # _.distlib.buildInputs.add = [pkgs.python310Packages.flit-core];
      # _.jedi.buildInputs.add = [pkgs.python310Packages.platformdirs];
      # _.black.buildInputs.add = [pkgs.python310Packages.platformdirs];
      # _.black.propagatedBuildInputs.mod = pySelf: _: old:
        # old ++ [pkgs.python310Packages.platformdirs];
      # providers =  {default = "nixpkgs,conda,wheel,sdist";
      #               tomli = "conda,wheel,sdist";
      #              };
      requirements = ''
        pip
        pandas
        cbor2
        ipython
        platformdirs
      '';
      # probagatedBuildInputs = [pkgs.pkg-config];
    };
    source-overrides = {
      vector-algorithms = "0.9.0.1";
      ghc-tcplugin-api = "0.10.0.0";
      large-generics = "0.2.1";
      typelet = "0.1.3";
      # fourmolu = "0.10.1.0";
      # Cabal-syntax = "3.8.1.0";
      # ghc-lib-parser = "9.4.1.20220807";
    };
    overrides = self: super: let
      minimalCompilation = x: disableLibraryProfiling (dontHaddock x);
      minSource = name: minimalCompilation (self.callCabal2nix name sources.${name} {});
      large-anon-drv = self.callCabal2nix "large-anon" "${sources.large-records}/large-anon" {};
      # large-anon-drv = self.callCabal2nix "large-anon" /home/data/code/large-records/large-anon {};
    in {
      yahp    = minSource "yahp";
      chronos = minSource "chronos";

      odbc    = dontCheck (minSource "odbc");
      # odbc = dontCheck (minimalCompilation (self.callCabal2nix "odbc" /home/data/code/odbc {}));

      # odbc    = appendPatch (dontCheck (minimalCompilation( self.callHackage "odbc" "0.2.6" {})))
        # ./odbc.patch;

      # this is needed because the default sqlite lib that comes with the package misses a lot of
      # extensions like ENABLE_MATH_FUNCTIONS
      direct-sqlite    = addExtraLibraries [ pkgs.sqlite ]
        (enableCabalFlag "systemlib" super.direct-sqlite);

      # target syntax: https://downloads.haskell.org/~cabal/Cabal-3.0.0.0/doc/users-guide/nix-local-build.html
      # jailbreak to ignore bounds on unuseg executable dependency 'fourmolu'
      large-anon        = enableCabalFlag "disableFourmoluExec" (appendPatch laPatch
        (overrideCabal (_: { executableHaskellDepends = [];})
          (dontCheck (minimalCompilation large-anon-drv))));

      # ghc-lib-parser =  super.ghc-lib-parser_9_4_4_20221225;
      # Cabal-syntax = super.Cabal-syntax_3_8_1_0;
      # fourmolu = self.fourmolu_0_10_1_0;
    };
    this = pkgs.haskellPackages.developPackage {
      root = ./.;
      withHoogle = false;
      returnShellEnv = false;
      inherit source-overrides overrides;
      modifier = drv:
        disableLibraryProfiling (dontHaddock (addBuildTools 
          (with pkgs.haskellPackages; [ cabal-install ghcid pyEnv]) drv));
    };
in this
   // { inherit source-overrides overrides;
        env = this.env.overrideAttrs(_: prev: { shellHook = prev.shellHook + ''
   export PYTHON_BIN=${pyEnv}/bin/python
   ''; });}
