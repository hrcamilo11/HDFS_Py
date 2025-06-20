# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: namenode.proto
# Protobuf Python Version: 5.29.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    0,
    '',
    'namenode.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0enamenode.proto\"\"\n\x0fRegisterRequest\x12\x0f\n\x07node_id\x18\x01 \x01(\t\"#\n\x10RegisterResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\"#\n\x10HeartbeatRequest\x12\x0f\n\x07node_id\x18\x01 \x01(\t\"$\n\x11HeartbeatResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\"<\n\x15\x41llocateBlocksRequest\x12\x10\n\x08username\x18\x01 \x01(\t\x12\x11\n\tfile_size\x18\x02 \x01(\x03\"+\n\x16\x41llocateBlocksResponse\x12\x11\n\tblock_ids\x18\x01 \x03(\t\"(\n\x14\x42lockLocationRequest\x12\x10\n\x08\x62lock_id\x18\x01 \x01(\t\")\n\x15\x42lockLocationResponse\x12\x10\n\x08node_ids\x18\x01 \x03(\t\"8\n\x11\x46ileBlocksRequest\x12\x10\n\x08username\x18\x01 \x01(\t\x12\x11\n\tfile_path\x18\x02 \x01(\t\"\'\n\x12\x46ileBlocksResponse\x12\x11\n\tblock_ids\x18\x01 \x03(\t\"H\n\x0e\x41\x64\x64\x46ileRequest\x12\x10\n\x08username\x18\x01 \x01(\t\x12\x11\n\tfile_path\x18\x02 \x01(\t\x12\x11\n\tblock_ids\x18\x03 \x03(\t\"5\n\x0f\x41\x64\x64\x46ileResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12\x11\n\tfile_path\x18\x02 \x01(\t\"6\n\x10ListFilesRequest\x12\x10\n\x08username\x18\x01 \x01(\t\x12\x10\n\x08\x64ir_path\x18\x02 \x01(\t\"\"\n\x11ListFilesResponse\x12\r\n\x05items\x18\x01 \x03(\t\"2\n\x0cMkdirRequest\x12\x10\n\x08username\x18\x01 \x01(\t\x12\x10\n\x08\x64ir_path\x18\x02 \x01(\t\" \n\rMkdirResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\"2\n\x0cRmdirRequest\x12\x10\n\x08username\x18\x01 \x01(\t\x12\x10\n\x08\x64ir_path\x18\x02 \x01(\t\" \n\rRmdirResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\"8\n\x11RemoveFileRequest\x12\x10\n\x08username\x18\x01 \x01(\t\x12\x11\n\tfile_path\x18\x02 \x01(\t\"%\n\x12RemoveFileResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\"N\n\x0bMoveRequest\x12\x10\n\x08username\x18\x01 \x01(\t\x12\x13\n\x0bsource_path\x18\x02 \x01(\t\x12\x18\n\x10\x64\x65stination_path\x18\x03 \x01(\t\"0\n\x0cMoveResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12\x0f\n\x07message\x18\x02 \x01(\t\" \n\x0cLoginRequest\x12\x10\n\x08username\x18\x01 \x01(\t\"1\n\rLoginResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12\x0f\n\x07message\x18\x02 \x01(\t\"!\n\rLogoutRequest\x12\x10\n\x08username\x18\x01 \x01(\t\"2\n\x0eLogoutResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12\x0f\n\x07message\x18\x02 \x01(\t2\xa0\x05\n\x0fNameNodeService\x12\x37\n\x10RegisterDataNode\x12\x10.RegisterRequest\x1a\x11.RegisterResponse\x12\x32\n\tHeartbeat\x12\x11.HeartbeatRequest\x1a\x12.HeartbeatResponse\x12\x41\n\x0e\x41llocateBlocks\x12\x16.AllocateBlocksRequest\x1a\x17.AllocateBlocksResponse\x12\x42\n\x11GetBlockLocations\x12\x15.BlockLocationRequest\x1a\x16.BlockLocationResponse\x12\x38\n\rGetFileBlocks\x12\x12.FileBlocksRequest\x1a\x13.FileBlocksResponse\x12,\n\x07\x41\x64\x64\x46ile\x12\x0f.AddFileRequest\x1a\x10.AddFileResponse\x12\x32\n\tListFiles\x12\x11.ListFilesRequest\x1a\x12.ListFilesResponse\x12&\n\x05Mkdir\x12\r.MkdirRequest\x1a\x0e.MkdirResponse\x12&\n\x05Rmdir\x12\r.RmdirRequest\x1a\x0e.RmdirResponse\x12\x35\n\nRemoveFile\x12\x12.RemoveFileRequest\x1a\x13.RemoveFileResponse\x12#\n\x04Move\x12\x0c.MoveRequest\x1a\r.MoveResponse\x12&\n\x05Login\x12\r.LoginRequest\x1a\x0e.LoginResponse\x12)\n\x06Logout\x12\x0e.LogoutRequest\x1a\x0f.LogoutResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'namenode_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_REGISTERREQUEST']._serialized_start=18
  _globals['_REGISTERREQUEST']._serialized_end=52
  _globals['_REGISTERRESPONSE']._serialized_start=54
  _globals['_REGISTERRESPONSE']._serialized_end=89
  _globals['_HEARTBEATREQUEST']._serialized_start=91
  _globals['_HEARTBEATREQUEST']._serialized_end=126
  _globals['_HEARTBEATRESPONSE']._serialized_start=128
  _globals['_HEARTBEATRESPONSE']._serialized_end=164
  _globals['_ALLOCATEBLOCKSREQUEST']._serialized_start=166
  _globals['_ALLOCATEBLOCKSREQUEST']._serialized_end=226
  _globals['_ALLOCATEBLOCKSRESPONSE']._serialized_start=228
  _globals['_ALLOCATEBLOCKSRESPONSE']._serialized_end=271
  _globals['_BLOCKLOCATIONREQUEST']._serialized_start=273
  _globals['_BLOCKLOCATIONREQUEST']._serialized_end=313
  _globals['_BLOCKLOCATIONRESPONSE']._serialized_start=315
  _globals['_BLOCKLOCATIONRESPONSE']._serialized_end=356
  _globals['_FILEBLOCKSREQUEST']._serialized_start=358
  _globals['_FILEBLOCKSREQUEST']._serialized_end=414
  _globals['_FILEBLOCKSRESPONSE']._serialized_start=416
  _globals['_FILEBLOCKSRESPONSE']._serialized_end=455
  _globals['_ADDFILEREQUEST']._serialized_start=457
  _globals['_ADDFILEREQUEST']._serialized_end=529
  _globals['_ADDFILERESPONSE']._serialized_start=531
  _globals['_ADDFILERESPONSE']._serialized_end=584
  _globals['_LISTFILESREQUEST']._serialized_start=586
  _globals['_LISTFILESREQUEST']._serialized_end=640
  _globals['_LISTFILESRESPONSE']._serialized_start=642
  _globals['_LISTFILESRESPONSE']._serialized_end=676
  _globals['_MKDIRREQUEST']._serialized_start=678
  _globals['_MKDIRREQUEST']._serialized_end=728
  _globals['_MKDIRRESPONSE']._serialized_start=730
  _globals['_MKDIRRESPONSE']._serialized_end=762
  _globals['_RMDIRREQUEST']._serialized_start=764
  _globals['_RMDIRREQUEST']._serialized_end=814
  _globals['_RMDIRRESPONSE']._serialized_start=816
  _globals['_RMDIRRESPONSE']._serialized_end=848
  _globals['_REMOVEFILEREQUEST']._serialized_start=850
  _globals['_REMOVEFILEREQUEST']._serialized_end=906
  _globals['_REMOVEFILERESPONSE']._serialized_start=908
  _globals['_REMOVEFILERESPONSE']._serialized_end=945
  _globals['_MOVEREQUEST']._serialized_start=947
  _globals['_MOVEREQUEST']._serialized_end=1025
  _globals['_MOVERESPONSE']._serialized_start=1027
  _globals['_MOVERESPONSE']._serialized_end=1075
  _globals['_LOGINREQUEST']._serialized_start=1077
  _globals['_LOGINREQUEST']._serialized_end=1109
  _globals['_LOGINRESPONSE']._serialized_start=1111
  _globals['_LOGINRESPONSE']._serialized_end=1160
  _globals['_LOGOUTREQUEST']._serialized_start=1162
  _globals['_LOGOUTREQUEST']._serialized_end=1195
  _globals['_LOGOUTRESPONSE']._serialized_start=1197
  _globals['_LOGOUTRESPONSE']._serialized_end=1247
  _globals['_NAMENODESERVICE']._serialized_start=1250
  _globals['_NAMENODESERVICE']._serialized_end=1922
# @@protoc_insertion_point(module_scope)
