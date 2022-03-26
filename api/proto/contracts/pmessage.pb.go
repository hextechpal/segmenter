// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.26.0
// 	protoc        v3.19.4
// source: api/proto/contracts/pmessage.proto

package contracts

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

type PMessage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Metadata     map[string]string `protobuf:"bytes,1,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Data         []byte            `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	PartitionKey string            `protobuf:"bytes,3,opt,name=partition_key,json=partitionKey,proto3" json:"partition_key,omitempty"`
}

func (x *PMessage) Reset() {
	*x = PMessage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_proto_contracts_pmessage_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PMessage) ProtoMessage() {}

func (x *PMessage) ProtoReflect() protoreflect.Message {
	mi := &file_api_proto_contracts_pmessage_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PMessage.ProtoReflect.Descriptor instead.
func (*PMessage) Descriptor() ([]byte, []int) {
	return file_api_proto_contracts_pmessage_proto_rawDescGZIP(), []int{0}
}

func (x *PMessage) GetMetadata() map[string]string {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *PMessage) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *PMessage) GetPartitionKey() string {
	if x != nil {
		return x.PartitionKey
	}
	return ""
}

var File_api_proto_contracts_pmessage_proto protoreflect.FileDescriptor

var file_api_proto_contracts_pmessage_proto_rawDesc = []byte{
	0x0a, 0x22, 0x61, 0x70, 0x69, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x74,
	0x72, 0x61, 0x63, 0x74, 0x73, 0x2f, 0x70, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x22, 0xb5, 0x01, 0x0a, 0x08, 0x50, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x12, 0x33, 0x0a, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x50, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e, 0x4d,
	0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x08, 0x6d, 0x65,
	0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x12, 0x23, 0x0a, 0x0d, 0x70, 0x61,
	0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0c, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x4b, 0x65, 0x79, 0x1a,
	0x3b, 0x0a, 0x0d, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x45, 0x6e, 0x74, 0x72, 0x79,
	0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b,
	0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x42, 0x35, 0x5a, 0x33,
	0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x68, 0x65, 0x78, 0x74, 0x65,
	0x63, 0x68, 0x70, 0x61, 0x6c, 0x2f, 0x73, 0x65, 0x67, 0x6d, 0x65, 0x6e, 0x74, 0x65, 0x72, 0x2f,
	0x61, 0x70, 0x69, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x61,
	0x63, 0x74, 0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_api_proto_contracts_pmessage_proto_rawDescOnce sync.Once
	file_api_proto_contracts_pmessage_proto_rawDescData = file_api_proto_contracts_pmessage_proto_rawDesc
)

func file_api_proto_contracts_pmessage_proto_rawDescGZIP() []byte {
	file_api_proto_contracts_pmessage_proto_rawDescOnce.Do(func() {
		file_api_proto_contracts_pmessage_proto_rawDescData = protoimpl.X.CompressGZIP(file_api_proto_contracts_pmessage_proto_rawDescData)
	})
	return file_api_proto_contracts_pmessage_proto_rawDescData
}

var file_api_proto_contracts_pmessage_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_api_proto_contracts_pmessage_proto_goTypes = []interface{}{
	(*PMessage)(nil), // 0: PMessage
	nil,              // 1: PMessage.MetadataEntry
}
var file_api_proto_contracts_pmessage_proto_depIdxs = []int32{
	1, // 0: PMessage.metadata:type_name -> PMessage.MetadataEntry
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_api_proto_contracts_pmessage_proto_init() }
func file_api_proto_contracts_pmessage_proto_init() {
	if File_api_proto_contracts_pmessage_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_api_proto_contracts_pmessage_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PMessage); i {
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
			RawDescriptor: file_api_proto_contracts_pmessage_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_api_proto_contracts_pmessage_proto_goTypes,
		DependencyIndexes: file_api_proto_contracts_pmessage_proto_depIdxs,
		MessageInfos:      file_api_proto_contracts_pmessage_proto_msgTypes,
	}.Build()
	File_api_proto_contracts_pmessage_proto = out.File
	file_api_proto_contracts_pmessage_proto_rawDesc = nil
	file_api_proto_contracts_pmessage_proto_goTypes = nil
	file_api_proto_contracts_pmessage_proto_depIdxs = nil
}
