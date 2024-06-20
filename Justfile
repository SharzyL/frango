pb_dir := "./frango/pb"
pb_gen_dir := "./frango/pb/generated"

gen-grpc:
  mkdir -p {{pb_gen_dir}}
  python -m grpc_tools.protoc \
    -I {{pb_dir}} \
    --python_out {{pb_gen_dir}} \
    --pyi_out {{pb_gen_dir}} \
    --grpc_python_out {{pb_gen_dir}} \
    {{pb_dir}}/node.proto

  # Ref: https://github.com/protocolbuffers/protobuf/issues/1491
  sed -i 's/import node_pb2/import frango.pb.generated.node_pb2/' {{pb_gen_dir}}/node_pb2_grpc.py

clean-grpc:
  rm -rf {{pb_gen_dir}}
