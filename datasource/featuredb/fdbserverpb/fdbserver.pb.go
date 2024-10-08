// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.21.12
// source: fdbserverpb/fdbserver.proto

package fdbserverpb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type KVData struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	Ts    int64  `protobuf:"varint,3,opt,name=ts,proto3" json:"ts,omitempty"`
}

func (x *KVData) Reset() {
	*x = KVData{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fdbserverpb_fdbserver_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *KVData) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KVData) ProtoMessage() {}

func (x *KVData) ProtoReflect() protoreflect.Message {
	mi := &file_fdbserverpb_fdbserver_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KVData.ProtoReflect.Descriptor instead.
func (*KVData) Descriptor() ([]byte, []int) {
	return file_fdbserverpb_fdbserver_proto_rawDescGZIP(), []int{0}
}

func (x *KVData) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *KVData) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *KVData) GetTs() int64 {
	if x != nil {
		return x.Ts
	}
	return 0
}

type BatchWriteKVReqeust struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	DatabaseName string    `protobuf:"bytes,1,opt,name=database_name,json=databaseName,proto3" json:"database_name,omitempty"`
	SchemaName   string    `protobuf:"bytes,2,opt,name=schema_name,json=schemaName,proto3" json:"schema_name,omitempty"`
	TableName    string    `protobuf:"bytes,3,opt,name=table_name,json=tableName,proto3" json:"table_name,omitempty"`
	Kvs          []*KVData `protobuf:"bytes,4,rep,name=kvs,proto3" json:"kvs,omitempty"`
}

func (x *BatchWriteKVReqeust) Reset() {
	*x = BatchWriteKVReqeust{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fdbserverpb_fdbserver_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchWriteKVReqeust) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchWriteKVReqeust) ProtoMessage() {}

func (x *BatchWriteKVReqeust) ProtoReflect() protoreflect.Message {
	mi := &file_fdbserverpb_fdbserver_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchWriteKVReqeust.ProtoReflect.Descriptor instead.
func (*BatchWriteKVReqeust) Descriptor() ([]byte, []int) {
	return file_fdbserverpb_fdbserver_proto_rawDescGZIP(), []int{1}
}

func (x *BatchWriteKVReqeust) GetDatabaseName() string {
	if x != nil {
		return x.DatabaseName
	}
	return ""
}

func (x *BatchWriteKVReqeust) GetSchemaName() string {
	if x != nil {
		return x.SchemaName
	}
	return ""
}

func (x *BatchWriteKVReqeust) GetTableName() string {
	if x != nil {
		return x.TableName
	}
	return ""
}

func (x *BatchWriteKVReqeust) GetKvs() []*KVData {
	if x != nil {
		return x.Kvs
	}
	return nil
}

type BatchWriteKVResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Success       bool      `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	TotalCount    int32     `protobuf:"varint,2,opt,name=total_count,json=totalCount,proto3" json:"total_count,omitempty"`
	SuccessCount  int32     `protobuf:"varint,3,opt,name=success_count,json=successCount,proto3" json:"success_count,omitempty"`
	ErrorMessages []string  `protobuf:"bytes,4,rep,name=error_messages,json=errorMessages,proto3" json:"error_messages,omitempty"`
	FailKvs       []*KVData `protobuf:"bytes,5,rep,name=fail_kvs,json=failKvs,proto3" json:"fail_kvs,omitempty"`
}

func (x *BatchWriteKVResponse) Reset() {
	*x = BatchWriteKVResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fdbserverpb_fdbserver_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BatchWriteKVResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchWriteKVResponse) ProtoMessage() {}

func (x *BatchWriteKVResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fdbserverpb_fdbserver_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchWriteKVResponse.ProtoReflect.Descriptor instead.
func (*BatchWriteKVResponse) Descriptor() ([]byte, []int) {
	return file_fdbserverpb_fdbserver_proto_rawDescGZIP(), []int{2}
}

func (x *BatchWriteKVResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *BatchWriteKVResponse) GetTotalCount() int32 {
	if x != nil {
		return x.TotalCount
	}
	return 0
}

func (x *BatchWriteKVResponse) GetSuccessCount() int32 {
	if x != nil {
		return x.SuccessCount
	}
	return 0
}

func (x *BatchWriteKVResponse) GetErrorMessages() []string {
	if x != nil {
		return x.ErrorMessages
	}
	return nil
}

func (x *BatchWriteKVResponse) GetFailKvs() []*KVData {
	if x != nil {
		return x.FailKvs
	}
	return nil
}

type TestBloomItemsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	DatabaseName string   `protobuf:"bytes,1,opt,name=database_name,json=databaseName,proto3" json:"database_name,omitempty"`
	SchemaName   string   `protobuf:"bytes,2,opt,name=schema_name,json=schemaName,proto3" json:"schema_name,omitempty"`
	TableName    string   `protobuf:"bytes,3,opt,name=table_name,json=tableName,proto3" json:"table_name,omitempty"`
	Key          string   `protobuf:"bytes,4,opt,name=key,proto3" json:"key,omitempty"`
	Items        []string `protobuf:"bytes,5,rep,name=items,proto3" json:"items,omitempty"`
}

func (x *TestBloomItemsRequest) Reset() {
	*x = TestBloomItemsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fdbserverpb_fdbserver_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TestBloomItemsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TestBloomItemsRequest) ProtoMessage() {}

func (x *TestBloomItemsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fdbserverpb_fdbserver_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TestBloomItemsRequest.ProtoReflect.Descriptor instead.
func (*TestBloomItemsRequest) Descriptor() ([]byte, []int) {
	return file_fdbserverpb_fdbserver_proto_rawDescGZIP(), []int{3}
}

func (x *TestBloomItemsRequest) GetDatabaseName() string {
	if x != nil {
		return x.DatabaseName
	}
	return ""
}

func (x *TestBloomItemsRequest) GetSchemaName() string {
	if x != nil {
		return x.SchemaName
	}
	return ""
}

func (x *TestBloomItemsRequest) GetTableName() string {
	if x != nil {
		return x.TableName
	}
	return ""
}

func (x *TestBloomItemsRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *TestBloomItemsRequest) GetItems() []string {
	if x != nil {
		return x.Items
	}
	return nil
}

type TestBloomItemsResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Tests []bool `protobuf:"varint,1,rep,packed,name=tests,proto3" json:"tests,omitempty"`
}

func (x *TestBloomItemsResponse) Reset() {
	*x = TestBloomItemsResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fdbserverpb_fdbserver_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TestBloomItemsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TestBloomItemsResponse) ProtoMessage() {}

func (x *TestBloomItemsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_fdbserverpb_fdbserver_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TestBloomItemsResponse.ProtoReflect.Descriptor instead.
func (*TestBloomItemsResponse) Descriptor() ([]byte, []int) {
	return file_fdbserverpb_fdbserver_proto_rawDescGZIP(), []int{4}
}

func (x *TestBloomItemsResponse) GetTests() []bool {
	if x != nil {
		return x.Tests
	}
	return nil
}

var File_fdbserverpb_fdbserver_proto protoreflect.FileDescriptor

var file_fdbserverpb_fdbserver_proto_rawDesc = []byte{
	0x0a, 0x1b, 0x66, 0x64, 0x62, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x70, 0x62, 0x2f, 0x66, 0x64,
	0x62, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0b, 0x66,
	0x64, 0x62, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x70, 0x62, 0x22, 0x40, 0x0a, 0x06, 0x4b, 0x56,
	0x44, 0x61, 0x74, 0x61, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x0e, 0x0a, 0x02,
	0x74, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x02, 0x74, 0x73, 0x22, 0xa1, 0x01, 0x0a,
	0x13, 0x42, 0x61, 0x74, 0x63, 0x68, 0x57, 0x72, 0x69, 0x74, 0x65, 0x4b, 0x56, 0x52, 0x65, 0x71,
	0x65, 0x75, 0x73, 0x74, 0x12, 0x23, 0x0a, 0x0d, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65,
	0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x64, 0x61, 0x74,
	0x61, 0x62, 0x61, 0x73, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1f, 0x0a, 0x0b, 0x73, 0x63, 0x68,
	0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a,
	0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x61,
	0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09,
	0x74, 0x61, 0x62, 0x6c, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x25, 0x0a, 0x03, 0x6b, 0x76, 0x73,
	0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x66, 0x64, 0x62, 0x73, 0x65, 0x72, 0x76,
	0x65, 0x72, 0x70, 0x62, 0x2e, 0x4b, 0x56, 0x44, 0x61, 0x74, 0x61, 0x52, 0x03, 0x6b, 0x76, 0x73,
	0x22, 0xcd, 0x01, 0x0a, 0x14, 0x42, 0x61, 0x74, 0x63, 0x68, 0x57, 0x72, 0x69, 0x74, 0x65, 0x4b,
	0x56, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x75, 0x63,
	0x63, 0x65, 0x73, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x75, 0x63, 0x63,
	0x65, 0x73, 0x73, 0x12, 0x1f, 0x0a, 0x0b, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x5f, 0x63, 0x6f, 0x75,
	0x6e, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0a, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x43,
	0x6f, 0x75, 0x6e, 0x74, 0x12, 0x23, 0x0a, 0x0d, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x5f,
	0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0c, 0x73, 0x75, 0x63,
	0x63, 0x65, 0x73, 0x73, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x25, 0x0a, 0x0e, 0x65, 0x72, 0x72,
	0x6f, 0x72, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28,
	0x09, 0x52, 0x0d, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x73,
	0x12, 0x2e, 0x0a, 0x08, 0x66, 0x61, 0x69, 0x6c, 0x5f, 0x6b, 0x76, 0x73, 0x18, 0x05, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x13, 0x2e, 0x66, 0x64, 0x62, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x70, 0x62,
	0x2e, 0x4b, 0x56, 0x44, 0x61, 0x74, 0x61, 0x52, 0x07, 0x66, 0x61, 0x69, 0x6c, 0x4b, 0x76, 0x73,
	0x22, 0xa4, 0x01, 0x0a, 0x15, 0x54, 0x65, 0x73, 0x74, 0x42, 0x6c, 0x6f, 0x6f, 0x6d, 0x49, 0x74,
	0x65, 0x6d, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x23, 0x0a, 0x0d, 0x64, 0x61,
	0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0c, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12,
	0x1f, 0x0a, 0x0b, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x4e, 0x61, 0x6d, 0x65,
	0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12,
	0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65,
	0x79, 0x12, 0x14, 0x0a, 0x05, 0x69, 0x74, 0x65, 0x6d, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x09,
	0x52, 0x05, 0x69, 0x74, 0x65, 0x6d, 0x73, 0x22, 0x2e, 0x0a, 0x16, 0x54, 0x65, 0x73, 0x74, 0x42,
	0x6c, 0x6f, 0x6f, 0x6d, 0x49, 0x74, 0x65, 0x6d, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x65, 0x73, 0x74, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x08,
	0x52, 0x05, 0x74, 0x65, 0x73, 0x74, 0x73, 0x42, 0x56, 0x5a, 0x54, 0x67, 0x69, 0x74, 0x68, 0x75,
	0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x6c, 0x69, 0x79, 0x75, 0x6e, 0x2f, 0x61, 0x6c, 0x69,
	0x79, 0x75, 0x6e, 0x2d, 0x70, 0x61, 0x69, 0x2d, 0x66, 0x65, 0x61, 0x74, 0x75, 0x72, 0x65, 0x73,
	0x74, 0x6f, 0x72, 0x65, 0x2d, 0x67, 0x6f, 0x2d, 0x73, 0x64, 0x6b, 0x2f, 0x76, 0x32, 0x2f, 0x64,
	0x61, 0x74, 0x61, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x2f, 0x66, 0x65, 0x61, 0x74, 0x75, 0x72,
	0x65, 0x64, 0x62, 0x2f, 0x66, 0x64, 0x62, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x70, 0x62, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_fdbserverpb_fdbserver_proto_rawDescOnce sync.Once
	file_fdbserverpb_fdbserver_proto_rawDescData = file_fdbserverpb_fdbserver_proto_rawDesc
)

func file_fdbserverpb_fdbserver_proto_rawDescGZIP() []byte {
	file_fdbserverpb_fdbserver_proto_rawDescOnce.Do(func() {
		file_fdbserverpb_fdbserver_proto_rawDescData = protoimpl.X.CompressGZIP(file_fdbserverpb_fdbserver_proto_rawDescData)
	})
	return file_fdbserverpb_fdbserver_proto_rawDescData
}

var file_fdbserverpb_fdbserver_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_fdbserverpb_fdbserver_proto_goTypes = []interface{}{
	(*KVData)(nil),                 // 0: fdbserverpb.KVData
	(*BatchWriteKVReqeust)(nil),    // 1: fdbserverpb.BatchWriteKVReqeust
	(*BatchWriteKVResponse)(nil),   // 2: fdbserverpb.BatchWriteKVResponse
	(*TestBloomItemsRequest)(nil),  // 3: fdbserverpb.TestBloomItemsRequest
	(*TestBloomItemsResponse)(nil), // 4: fdbserverpb.TestBloomItemsResponse
}
var file_fdbserverpb_fdbserver_proto_depIdxs = []int32{
	0, // 0: fdbserverpb.BatchWriteKVReqeust.kvs:type_name -> fdbserverpb.KVData
	0, // 1: fdbserverpb.BatchWriteKVResponse.fail_kvs:type_name -> fdbserverpb.KVData
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_fdbserverpb_fdbserver_proto_init() }
func file_fdbserverpb_fdbserver_proto_init() {
	if File_fdbserverpb_fdbserver_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_fdbserverpb_fdbserver_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*KVData); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_fdbserverpb_fdbserver_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchWriteKVReqeust); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_fdbserverpb_fdbserver_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BatchWriteKVResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_fdbserverpb_fdbserver_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TestBloomItemsRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_fdbserverpb_fdbserver_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TestBloomItemsResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_fdbserverpb_fdbserver_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_fdbserverpb_fdbserver_proto_goTypes,
		DependencyIndexes: file_fdbserverpb_fdbserver_proto_depIdxs,
		MessageInfos:      file_fdbserverpb_fdbserver_proto_msgTypes,
	}.Build()
	File_fdbserverpb_fdbserver_proto = out.File
	file_fdbserverpb_fdbserver_proto_rawDesc = nil
	file_fdbserverpb_fdbserver_proto_goTypes = nil
	file_fdbserverpb_fdbserver_proto_depIdxs = nil
}
