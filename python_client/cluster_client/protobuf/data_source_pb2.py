# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: data_source.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x11\x64\x61ta_source.proto\"A\n\nDataSource\x12)\n\tcassandra\x18\x01 \x01(\x0b\x32\x14.CassandraDataSourceH\x00\x42\x08\n\x06source\"J\n\x13\x43\x61ssandraDataSource\x12\x12\n\nserver_url\x18\x01 \x01(\t\x12\x10\n\x08keyspace\x18\x02 \x01(\t\x12\r\n\x05table\x18\x03 \x01(\t\"1\n\x13\x43\x61ssandraTokenRange\x12\r\n\x05start\x18\x01 \x01(\x12\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x12\x42!\n\x1forg.oliverlittle.clusterprocessb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'data_source_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'\n\037org.oliverlittle.clusterprocess'
  _DATASOURCE._serialized_start=21
  _DATASOURCE._serialized_end=86
  _CASSANDRADATASOURCE._serialized_start=88
  _CASSANDRADATASOURCE._serialized_end=162
  _CASSANDRATOKENRANGE._serialized_start=164
  _CASSANDRATOKENRANGE._serialized_end=213
# @@protoc_insertion_point(module_scope)