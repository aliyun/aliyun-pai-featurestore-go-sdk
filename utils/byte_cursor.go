package utils

import (
	"encoding/binary"
	"errors"
	"math"
)

var (
	ErrUnexpectedEOF = errors.New("unexpected EOF while reading binary data")
	ErrInvalidLength = errors.New("invalid negative length encountered")
)

type ByteCursor struct {
	Data []byte
	Off  int
	Err  error
}

func NewByteCursor(data []byte) *ByteCursor {
	return &ByteCursor{Data: data}
}

func (c *ByteCursor) HasMore() bool {
	return c.Err == nil && c.Off < len(c.Data)
}

func (c *ByteCursor) TryReadUint8() (uint8, bool) {
	if c.Err != nil || c.Off >= len(c.Data) {
		return 0, false
	}
	v := c.Data[c.Off]
	c.Off++
	return v, true
}

func (c *ByteCursor) ReadUint8() uint8 {
	if c.Err != nil || c.Off+1 > len(c.Data) {
		c.Err = ErrUnexpectedEOF
		return 0
	}
	v := c.Data[c.Off]
	c.Off++
	return v
}

func (c *ByteCursor) ReadUint32() uint32 {
	if c.Err != nil || c.Off+4 > len(c.Data) {
		c.Err = ErrUnexpectedEOF
		return 0
	}
	v := binary.LittleEndian.Uint32(c.Data[c.Off:])
	c.Off += 4
	return v
}

func (c *ByteCursor) ReadUint64() uint64 {
	if c.Err != nil || c.Off+8 > len(c.Data) {
		c.Err = ErrUnexpectedEOF
		return 0
	}
	v := binary.LittleEndian.Uint64(c.Data[c.Off:])
	c.Off += 8
	return v
}

func (c *ByteCursor) ReadInt32() int32 {
	return int32(c.ReadUint32())
}

func (c *ByteCursor) ReadInt64() int64 {
	return int64(c.ReadUint64())
}

func (c *ByteCursor) ReadFloat32() float32 {
	return math.Float32frombits(c.ReadUint32())
}

func (c *ByteCursor) ReadFloat64() float64 {
	return math.Float64frombits(c.ReadUint64())
}

func (c *ByteCursor) ReadBool() bool {
	return c.ReadUint8() == 1
}

func (c *ByteCursor) ReadBytes(n int) []byte {
	if c.Err != nil {
		return nil
	}
	if n == 0 {
		return []byte{}
	}
	if n < 0 || c.Off+n > len(c.Data) {
		c.Err = ErrUnexpectedEOF
		return nil
	}
	v := c.Data[c.Off : c.Off+n]
	c.Off += n
	return v
}

func (c *ByteCursor) Skip(n int) {
	if c.Err != nil || n <= 0 {
		return
	}
	if c.Off+n > len(c.Data) {
		c.Err = ErrUnexpectedEOF
		return
	}
	c.Off += n
}

func (c *ByteCursor) ReadInt32Slice(l uint32) []int32 {
	if c.Err != nil {
		return nil
	}
	if l == 0 {
		return []int32{}
	}
	n := int(l)
	if n < 0 || c.Off+int(n*4) > len(c.Data) {
		c.Err = ErrUnexpectedEOF
		return nil
	}
	res := make([]int32, n)
	for i := 0; i < n; i++ {
		res[i] = c.ReadInt32()
	}
	return res
}

func (c *ByteCursor) ReadUint32Slice(l uint32) []uint32 {
	if c.Err != nil {
		return nil
	}
	if l == 0 {
		return []uint32{}
	}
	n := int(l)
	if n < 0 || c.Off+int(n*4) > len(c.Data) {
		c.Err = ErrUnexpectedEOF
		return nil
	}
	res := make([]uint32, n)
	for i := 0; i < n; i++ {
		res[i] = c.ReadUint32()
	}
	return res
}

func (c *ByteCursor) ReadInt64Slice(l uint32) []int64 {
	if c.Err != nil {
		return nil
	}
	if l == 0 {
		return []int64{}
	}
	n := int(l)
	if n < 0 || c.Off+(n*8) > len(c.Data) {
		c.Err = ErrUnexpectedEOF
		return nil
	}
	res := make([]int64, n)
	for i := 0; i < n; i++ {
		res[i] = c.ReadInt64()
	}
	return res
}

func (c *ByteCursor) ReadFloat32Slice(l uint32) []float32 {
	if c.Err != nil {
		return nil
	}
	if l == 0 {
		return []float32{}
	}
	n := int(l)
	if n < 0 || c.Off+(n*4) > len(c.Data) {
		c.Err = ErrUnexpectedEOF
		return nil
	}
	res := make([]float32, n)
	for i := 0; i < n; i++ {
		res[i] = c.ReadFloat32()
	}
	return res
}

func (c *ByteCursor) ReadFloat64Slice(l uint32) []float64 {
	if c.Err != nil {
		return nil
	}
	if l == 0 {
		return []float64{}
	}
	n := int(l)
	if n < 0 || c.Off+(n*8) > len(c.Data) {
		c.Err = ErrUnexpectedEOF
		return nil
	}
	res := make([]float64, n)
	for i := 0; i < n; i++ {
		res[i] = c.ReadFloat64()
	}
	return res
}

func (c *ByteCursor) ReadString() string {
	l := int(c.ReadUint32())
	if c.Err != nil {
		return ""
	}
	return string(c.ReadBytes(l))
}

func (c *ByteCursor) ReadStringArray(l uint32) []string {
	if c.Err != nil {
		return nil
	}
	if l == 0 {
		return []string{}
	}

	count := int(l)
	if count < 0 || c.Off+((count+1)*4) > len(c.Data) {
		c.Err = ErrUnexpectedEOF
		return nil
	}

	offsets := make([]uint32, count+1)
	for i := 0; i <= count; i++ {
		offsets[i] = c.ReadUint32()
	}

	totalByteLen := int(offsets[count])
	data := c.ReadBytes(totalByteLen)
	if c.Err != nil {
		return nil
	}

	res := make([]string, count)
	for i := 0; i < count; i++ {
		start, end := offsets[i], offsets[i+1]
		if start > end || int(end) > len(data) {
			c.Err = ErrInvalidLength
			return nil
		}
		res[i] = string(data[start:end])
	}
	return res
}

func (c *ByteCursor) SkipStringArray(l uint32) {
	if c.Err != nil || l == 0 {
		return
	}
	count := int(l)
	c.Skip(count * 4)
	totalLen := int(c.ReadUint32())
	c.Skip(totalLen)
}
