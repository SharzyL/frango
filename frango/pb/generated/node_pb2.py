# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: node.proto
# Protobuf Python Version: 4.25.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\nnode.proto\x12\x06\x66rango\"\x07\n\x05\x45mpty\"<\n\x08PingResp\x12\n\n\x02id\x18\x01 \x01(\x05\x12\x16\n\tleader_id\x18\x02 \x01(\x05H\x00\x88\x01\x01\x42\x0c\n\n_leader_id\"!\n\x0cRRaftMessage\x12\x11\n\tthe_bytes\x18\x01 \x01(\x0c\"\x1d\n\x08QueryReq\x12\x11\n\tquery_str\x18\x01 \x01(\t\"@\n\tQueryResp\x12\r\n\x05\x65rror\x18\x01 \x01(\x05\x12\x0e\n\x06header\x18\x02 \x03(\t\x12\x14\n\x0crows_in_json\x18\x03 \x03(\t2\xc2\x01\n\nFrangoNode\x12\'\n\x04Ping\x12\r.frango.Empty\x1a\x10.frango.PingResp\x12,\n\x05RRaft\x12\x14.frango.RRaftMessage\x1a\r.frango.Empty\x12,\n\x05Query\x12\x10.frango.QueryReq\x1a\x11.frango.QueryResp\x12/\n\x08SubQuery\x12\x10.frango.QueryReq\x1a\x11.frango.QueryRespb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'node_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_EMPTY']._serialized_start=22
  _globals['_EMPTY']._serialized_end=29
  _globals['_PINGRESP']._serialized_start=31
  _globals['_PINGRESP']._serialized_end=91
  _globals['_RRAFTMESSAGE']._serialized_start=93
  _globals['_RRAFTMESSAGE']._serialized_end=126
  _globals['_QUERYREQ']._serialized_start=128
  _globals['_QUERYREQ']._serialized_end=157
  _globals['_QUERYRESP']._serialized_start=159
  _globals['_QUERYRESP']._serialized_end=223
  _globals['_FRANGONODE']._serialized_start=226
  _globals['_FRANGONODE']._serialized_end=420
# @@protoc_insertion_point(module_scope)
