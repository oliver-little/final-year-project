# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: worker_query.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import table_model_pb2 as table__model__pb2
import cassandra_source_pb2 as cassandra__source__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x12worker_query.proto\x1a\x11table_model.proto\x1a\x16\x63\x61ssandra_source.proto\"\x9b\x01\n$ComputePartialResultCassandraRequest\x12\x1f\n\x0ftransformations\x18\x01 \x01(\x0b\x32\x06.Table\x12(\n\ndataSource\x18\x02 \x01(\x0b\x32\x14.CassandraDataSource\x12(\n\ntokenRange\x18\x03 \x01(\x0b\x32\x14.CassandraTokenRange\"6\n#ComputePartialResultCassandraResult\x12\x0f\n\x07success\x18\x01 \x01(\x08\x32\x84\x01\n\x14WorkerComputeService\x12l\n\x1d\x43omputePartialResultCassandra\x12%.ComputePartialResultCassandraRequest\x1a$.ComputePartialResultCassandraResultB!\n\x1forg.oliverlittle.clusterprocessb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'worker_query_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n\037org.oliverlittle.clusterprocess'
  _COMPUTEPARTIALRESULTCASSANDRAREQUEST._serialized_start=66
  _COMPUTEPARTIALRESULTCASSANDRAREQUEST._serialized_end=221
  _COMPUTEPARTIALRESULTCASSANDRARESULT._serialized_start=223
  _COMPUTEPARTIALRESULTCASSANDRARESULT._serialized_end=277
  _WORKERCOMPUTESERVICE._serialized_start=280
  _WORKERCOMPUTESERVICE._serialized_end=412
# @@protoc_insertion_point(module_scope)