# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: client_query.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import table_model_pb2 as table__model__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x12\x63lient_query.proto\x1a\x11table_model.proto\"*\n\x12TableComputeResult\x12\x14\n\x0c\x63ompute_uuid\x18\x01 \x01(\t2@\n\x12TableClientService\x12*\n\tSendTable\x12\x06.Table\x1a\x13.TableComputeResult\"\x00\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'client_query_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _TABLECOMPUTERESULT._serialized_start=41
  _TABLECOMPUTERESULT._serialized_end=83
  _TABLECLIENTSERVICE._serialized_start=85
  _TABLECLIENTSERVICE._serialized_end=149
# @@protoc_insertion_point(module_scope)
