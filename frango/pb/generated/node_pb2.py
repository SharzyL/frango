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




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\nnode.proto\x12\x06\x66rango\"\x07\n\x05\x45mpty\"<\n\x08PingResp\x12\n\n\x02id\x18\x01 \x01(\x05\x12\x16\n\tleader_id\x18\x02 \x01(\x05H\x00\x88\x01\x01\x42\x0c\n\n_leader_id\"!\n\x0cRRaftMessage\x12\x11\n\tthe_bytes\x18\x01 \x01(\x0c\"2\n\x08QueryReq\x12\x11\n\tquery_str\x18\x01 \x01(\t\x12\x13\n\x0bparams_json\x18\x02 \x01(\t\"w\n\tQueryResp\x12\x14\n\x07\x65rr_msg\x18\x01 \x01(\tH\x00\x88\x01\x01\x12\x0e\n\x06header\x18\x02 \x03(\t\x12\x14\n\x0crows_in_json\x18\x03 \x03(\t\x12\x10\n\x08is_valid\x18\x04 \x01(\x08\x12\x10\n\x08is_error\x18\x05 \x01(\x08\x42\n\n\x08_err_msg\"n\n\x15LocalQueryCompleteReq\x12\x34\n\x06\x61\x63tion\x18\x01 \x01(\x0e\x32$.frango.LocalQueryCompleteReq.Action\"\x1f\n\x06\x41\x63tion\x12\n\n\x06\x43OMMIT\x10\x00\x12\t\n\x05\x41\x42ORT\x10\x01\"\xa2\x01\n\x0ePopularRankReq\x12\x0b\n\x03\x64\x61y\x18\x01 \x01(\t\x12H\n\x14temporal_granularity\x18\x02 \x01(\x0e\x32*.frango.PopularRankReq.TemporalGranularity\"9\n\x13TemporalGranularity\x12\t\n\x05\x44\x41ILY\x10\x00\x12\n\n\x06WEEKLY\x10\x01\x12\x0b\n\x07MONTHLY\x10\x02\x32\xc2\x02\n\nFrangoNode\x12\'\n\x04Ping\x12\r.frango.Empty\x1a\x10.frango.PingResp\x12,\n\x05RRaft\x12\x14.frango.RRaftMessage\x1a\r.frango.Empty\x12,\n\x05Query\x12\x10.frango.QueryReq\x1a\x11.frango.QueryResp\x12\x38\n\x0bPopularRank\x12\x16.frango.PopularRankReq\x1a\x11.frango.QueryResp\x12\x31\n\nLocalQuery\x12\x10.frango.QueryReq\x1a\x11.frango.QueryResp\x12\x42\n\x12LocalQueryComplete\x12\x1d.frango.LocalQueryCompleteReq\x1a\r.frango.Emptyb\x06proto3')

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
  _globals['_QUERYREQ']._serialized_end=178
  _globals['_QUERYRESP']._serialized_start=180
  _globals['_QUERYRESP']._serialized_end=299
  _globals['_LOCALQUERYCOMPLETEREQ']._serialized_start=301
  _globals['_LOCALQUERYCOMPLETEREQ']._serialized_end=411
  _globals['_LOCALQUERYCOMPLETEREQ_ACTION']._serialized_start=380
  _globals['_LOCALQUERYCOMPLETEREQ_ACTION']._serialized_end=411
  _globals['_POPULARRANKREQ']._serialized_start=414
  _globals['_POPULARRANKREQ']._serialized_end=576
  _globals['_POPULARRANKREQ_TEMPORALGRANULARITY']._serialized_start=519
  _globals['_POPULARRANKREQ_TEMPORALGRANULARITY']._serialized_end=576
  _globals['_FRANGONODE']._serialized_start=579
  _globals['_FRANGONODE']._serialized_end=901
# @@protoc_insertion_point(module_scope)
