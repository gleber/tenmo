with (import <nixpkgs> {});
let
  my-python-packages = python-packages: with python-packages; [
    requests
    psycopg2
    websockets
  ];
in
mkShell {
  buildInputs = [
    ephemeralpg
    postgresql_12
    (python3.withPackages my-python-packages)
  ];
}
