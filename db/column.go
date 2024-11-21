// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"strconv"
)

// Column stores name and value etc.
type Column struct {
	Name       string // 列名
	Value      string // 列值
	Timestamp  int64  // 时间戳
	size       int32  // 列大小，未在代码中使用
	deleteMark bool   // 删除标记，表示该列是否被删除
}

func (c Column) addColumn(column IColumn) {
	log.Printf("Invalid method: Column doesn't support addColumn\n")
}

func (c Column) getMarkedForDeleteAt() int64 {
	if c.isMarkedForDelete() == false {
		log.Fatal("column is not marked for delete")
	}
	return c.Timestamp
}

func (c Column) getLocalDeletionTime() int {
	return 0 // miao miao miao ?
}

func (c Column) isMarkedForDelete() bool {
	return c.deleteMark
}

// IsMarkedForDelete ...
func (c Column) IsMarkedForDelete() bool {
	return c.deleteMark
}

func (c Column) getValue() []byte {
	return []byte(c.Value)
}

// GetValue ..
func (c Column) GetValue() []byte {
	return []byte(c.Value)
}

func (c Column) getSize() int32 {
	// size of a column:
	//  4 bytes for name length
	//  # bytes for name string bytes
	//  8 bytes for timestamp
	//  4 bytes for value byte array
	//  # bytes for value bytes
	return int32(4 + 8 + 4 + len(c.Name) + len(c.Value))
}

// delete deletes a Column
/*func (c Column) delete() {
	if c.isMarkedForDelete == false {
		c.isMarkedForDelete = true
		c.Value = ""
	}
}*/

// 如果 column 的时间戳较新，则将当前列的 Value 和 Timestamp 更新为新 column 的值。
func (c Column) repair(column Column) {
	if c.Timestamp < column.Timestamp {
		c.Value = column.Value
		c.Timestamp = column.Timestamp
	}
}

// 比较当前列和 column 的时间戳，若 column 的时间戳较新，则返回一个新列，包含 column 的 Name、Value 和 Timestamp。
func (c Column) diff(column Column) Column {
	var columnDiff Column
	if c.Timestamp < column.Timestamp {
		columnDiff.Name = column.Name
		columnDiff.Value = column.Value
		columnDiff.Timestamp = column.Timestamp
	}
	return columnDiff
}

func getObjectCount() int {
	return 1
}

// 将列转换为字符串，格式为：{Name}:{Timestamp}:{ValueLength}:{Value}
func (c Column) toString() string {
	var buffer bytes.Buffer
	buffer.WriteString(c.Name)
	buffer.WriteString(":")
	//buffer.WriteString(strconv.FormatBool(c.isMarkedForDelete))
	buffer.WriteString(":")
	buffer.WriteString(strconv.FormatInt(c.Timestamp, 10))
	buffer.WriteString(":")
	buffer.WriteString(strconv.FormatInt(int64(len(c.Value)), 10))
	buffer.WriteString(":")
	buffer.WriteString(c.Value)
	buffer.WriteString(":")
	return buffer.String()
}

// 生成列的摘要，由 Name 和 Timestamp 组成。
func (c Column) digest() string {
	var buffer bytes.Buffer
	buffer.WriteString(c.Name)
	//buffer.WriteString(c.Seperator)
	buffer.WriteString(strconv.FormatInt(c.Timestamp, 10))
	return buffer.String()
}

// NewColumn constructs a Column
//
// 构造一个新的 Column，提供列名、值、时间戳和删除标记。
func NewColumn(name, value string, timestamp int64, deleteMark bool) Column {
	c := Column{}
	c.Name = name
	c.Value = value
	c.Timestamp = timestamp
	c.deleteMark = deleteMark
	return c
}

// NewColumnKV ..
//
// NewColumnKV 是 NewColumn 的一个简化版本，仅使用列名和值，时间戳默认为 0，删除标记默认为 false 。
func NewColumnKV(name, value string) Column {
	return NewColumn(name, value, 0, false)
}

// toByteArray ..
//
// 将列转换为字节数组，格式包括：列名长度、列名、删除标记、时间戳、值长度和值。
func (c Column) toByteArray() []byte {
	buf := make([]byte, 0)
	// write column name length
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(len(c.Name)))
	buf = append(buf, b4...)
	// write column name
	buf = append(buf, []byte(c.Name)...)
	// write deleteMark
	if c.deleteMark {
		buf = append(buf, byte(1))
	} else {
		buf = append(buf, byte(0))
	}
	// write timestamp
	b8 := make([]byte, 8)
	binary.BigEndian.PutUint64(b8, uint64(c.Timestamp))
	buf = append(buf, b8...)
	// write value length
	binary.BigEndian.PutUint32(b4, uint32(len(c.Value)))
	buf = append(buf, b4...)
	// write value bytes
	buf = append(buf, []byte(c.Value)...)
	return buf
}

// 计算并返回列的序列化大小，包括列名长度(4)、列名(n)、删除标记(1)、时间戳(8)、值长度(4)和列值(n)的字节数。
func (c Column) serializedSize() uint32 {
	// 4 byte: length of column name
	// # bytes: column name bytes
	// 1 byte:  deleteMark
	// 8 bytes: timestamp
	// 4 bytes: length of value
	// # bytes: value bytes
	return uint32(4 + 1 + 8 + 4 + len(c.Name) + len(c.Value))
}

func (c Column) getObjectCount() int {
	return 1
}

func (c Column) timestamp() int64 {
	return c.Timestamp
}

// GetTimestamp ...
func (c Column) GetTimestamp() int64 {
	return c.Timestamp
}

// 比较两个列的优先级，优先级基于时间戳和是否标记为删除。如果列被标记为删除（Tombstone），则它的优先级更高。
func (c Column) comparePriority(o Column) int64 {
	if c.isMarkedForDelete() {
		// tombstone always wins ties
		if c.Timestamp < o.Timestamp {
			return -1
		}
		return 1
	}
	return c.Timestamp - o.Timestamp
}

// 将传入的列与当前列进行比较，根据时间戳决定是否更新当前列。如果传入列的时间戳较新，则返回 false，否则返回 true。
func (c Column) putColumn(column IColumn) bool {
	// resolve the column by comparing timestamps.
	// if a newer value is being put, take the change.
	// else ignore.
	_, ok := column.(Column)
	if !ok {
		log.Fatal("Only Column objects should be put here")
	}
	if c.Name != column.getName() {
		log.Fatal("The name should match the name of the current column")
	}
	if c.Timestamp <= column.timestamp() {
		return true
	}
	return false
}

func (c Column) getName() string {
	return c.Name
}

// GetName ...
func (c Column) GetName() string {
	return c.Name
}

// 这些方法表示列的子列，这些操作不被支持，会直接抛出错误。
func (c Column) getSubColumns() map[string]IColumn {
	log.Fatal("This operation is not supported on simple columns")
	return nil
}

// GetSubColumns ...
func (c Column) GetSubColumns() map[string]IColumn {
	log.Fatal("This operation is not supported on simple columns")
	return nil
}

func (c Column) mostRecentChangeAt() int64 {
	return c.Timestamp
}

// CSerializer ...
var CSerializer = NewColumnSerializer()

// ColumnSerializer ...
//
// 用于列的序列化和反序列化
type ColumnSerializer struct {
	dummy int
}

// NewColumnSerializer ...
func NewColumnSerializer() *ColumnSerializer {
	return &ColumnSerializer{0}
}

// 将列的字段（名称、删除标记、时间戳、值）序列化到文件中。
func (c *ColumnSerializer) serialize(column IColumn, dos *os.File) {
	writeString(dos, column.getName())
	writeBool(dos, column.isMarkedForDelete())
	writeInt64(dos, column.timestamp())
	writeBytes(dos, column.getValue()) // will first write byte length, the bytes
}

// 将列的字段（名称、删除标记、时间戳、值）序列化到字节数组中。
func (c *ColumnSerializer) serializeB(column IColumn, dos []byte) {
	writeStringB(dos, column.getName())
	writeBoolB(dos, column.isMarkedForDelete())
	writeInt64B(dos, column.timestamp())
	writeBytesB(dos, column.getValue()) // will first write byte length, the bytes
}

// 从文件中反序列化数据并返回一个新的列。
//
// +----------------------------------------------------------------+
// | Field      | Length   | Value                                  |
// +----------------------------------------------------------------+
// | Column Name| 4 bytes  | "col1" (4 bytes)                       |
// | DeleteMark | 1 byte   | true (1 byte)                          |
// | Timestamp  | 8 bytes  | 1610000000000 (8 bytes)                |
// | Value      | 10 bytes | "value_data" (10 bytes)                |
// +----------------------------------------------------------------+
func (c *ColumnSerializer) deserialize(dis *os.File) IColumn {
	name, _ := readString(dis)
	deleteMark, _ := readBool(dis)
	timestamp := readInt64(dis)
	value, _ := readBytes(dis)
	return NewColumn(name, string(value), timestamp, deleteMark)
}
