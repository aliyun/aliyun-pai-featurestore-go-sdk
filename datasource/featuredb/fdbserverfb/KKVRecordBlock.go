// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package fdbserverfb

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type KKVRecordBlock struct {
	_tab flatbuffers.Table
}

func GetRootAsKKVRecordBlock(buf []byte, offset flatbuffers.UOffsetT) *KKVRecordBlock {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &KKVRecordBlock{}
	x.Init(buf, n+offset)
	return x
}

func FinishKKVRecordBlockBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsKKVRecordBlock(buf []byte, offset flatbuffers.UOffsetT) *KKVRecordBlock {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &KKVRecordBlock{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedKKVRecordBlockBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *KKVRecordBlock) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *KKVRecordBlock) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *KKVRecordBlock) Values(obj *KKVData, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *KKVRecordBlock) ValuesLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func KKVRecordBlockStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func KKVRecordBlockAddValues(builder *flatbuffers.Builder, values flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(values), 0)
}
func KKVRecordBlockStartValuesVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func KKVRecordBlockEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}