import sys
import os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)) + "/generated")

from .generated import node_pb2 as node_pb, node_pb2_grpc as node_grpc
