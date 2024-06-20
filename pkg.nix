{ buildPythonPackage
, lib
, pdm-backend

, rraft-py

, pytest
, loguru
, grpcio
, dataclass-wizard
, sqlglot
, rich

, pytestCheckHook
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
      ./README.md
    ];
  };

  propagatedBuildInputs = [
    pytest
    loguru
    # https://github.com/protocolbuffers/protobuf/commit/5b32936822e64b796fa18fcff53df2305c6b7686 will fix a warning
    grpcio
    rraft-py
    dataclass-wizard
    sqlglot
    rich
  ];

  nativeCheckInputs = [ pytestCheckHook ];

  pythonImportsCheck = [ "frango" ];
}

