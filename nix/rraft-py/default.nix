{ lib
, buildPythonApplication
, fetchPypi
, protobuf
, rustPlatform
, pytest
, pytest-benchmark
}:

buildPythonApplication rec {
  pname = "rraft-py";
  version = "0.2.27";
  pyproject = true;

  src = fetchPypi {
    pname = "rraft_py";
    inherit version;
    hash = "sha256-33fSaKldNmRJAX16shCcNyDthIbDQ0sCUlLKA1U7JoU=";
  };

  cargoDeps = rustPlatform.importCargoLock {
    lockFile = ./Cargo.lock;
    outputHashes = {
      "raft-0.7.0" = "sha256-6NlUotaVTG9Qzhmu4asGXzmkqlVuXXWK2G4kRZ8j6fE=";
    };
  };

  nativeBuildInputs = [
    protobuf
    rustPlatform.cargoSetupHook
    rustPlatform.maturinBuildHook
  ];

  propagatedBuildInputs = [
    pytest
    pytest-benchmark
  ];

  pythonImportsCheck = [ "rraft" ];

  meta = with lib; {
    description = "Unofficial Python Binding of the tikv/raft-rs";
    homepage = "https://pypi.org/project/rraft-py";
    license = licenses.asl20;
    mainProgram = "rraft-py";
  };
}
