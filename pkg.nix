{ buildPythonPackage
, lib
, pdm-backend

, rraft-py

, loguru
, grpcio
}:

buildPythonPackage {
  name = "frango";
  pyproject = true;
  nativeBuildInputs = [ pdm-backend ];

  src = with lib.fileset; toSource {
    root = ./.;
    fileset = unions [
      ./frango
      ./pyproject.toml
      ./pdm.lock
    ];
  };

  propagatedBuildInputs = [
    loguru
    grpcio
    rraft-py
  ];

  doCheck = false; # since we have no test
}

