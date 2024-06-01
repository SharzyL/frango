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

clean-grpc:
  rm -rf {{pb_gen_dir}}
