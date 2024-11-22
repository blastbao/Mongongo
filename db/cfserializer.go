// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"os"

	"github.com/davecgh/go-spew/spew"
)

// CFSerializer ...
var CFSerializer = NewCFSerializer()

// ColumnFamilySerializer ...
type ColumnFamilySerializer struct {
	dummy int
}

// NewCFSerializer ...
func NewCFSerializer() *ColumnFamilySerializer {
	c := &ColumnFamilySerializer{0}
	return c
}

func (c *ColumnFamilySerializer) serialize(cf *ColumnFamily, dos []byte) {
	writeStringB(dos, cf.ColumnFamilyName) // 列族名
	writeStringB(dos, cf.ColumnType)       // 列族类型
	c.serializeForSSTable(cf, dos)         //
}

func (c *ColumnFamilySerializer) deserializeFromSSTableNoColumns(cf *ColumnFamily, input *os.File) *ColumnFamily {
	localtime := readInt(input)
	timestamp := readInt64(input)
	cf.updateDeleteTime(localtime, timestamp)
	return cf
}

func (c *ColumnFamilySerializer) serializeWithIndexes(cf *ColumnFamily, dos []byte) {
	// 对 cf 进行列排序、建立 bf 、建立列索引后，写入到 dos 中；
	CIndexer.serialize(cf, dos)
	// 将 cf 中的列逐个写入到 dos 中；
	c.serializeForSSTable(cf, dos)
}

func (c *ColumnFamilySerializer) serializeForSSTable(cf *ColumnFamily, dos []byte) {
	// 将 localDeletionTime 以 Int32 写入 dos
	writeIntB(dos, cf.localDeletionTime)
	// 将 markedForDeleteAt 以 Int64 写入 dos
	writeInt64B(dos, cf.markedForDeleteAt)
	// 按照列名顺序排列 cf.columns 并返回
	columns := cf.GetSortedColumns()
	// 将列总数写入 dos
	writeIntB(dos, len(columns))
	// 将列逐个写入 dos
	for _, column := range columns {
		spew.Printf("cf: %#+v\n", cf)
		spew.Printf("cf column serializer: %#+v\n", cf.getColumnSerializer())
		// 普通列
		// +----------------------------------------------------------------+
		// | Field      | Length   | Value                                  |
		// +----------------------------------------------------------------+
		// | Column Name| 4 bytes  | "col1" (4 bytes)                       |
		// | DeleteMark | 1 byte   | true (1 byte)                          |
		// | Timestamp  | 8 bytes  | 1610000000000 (8 bytes)                |
		// | Value      | 10 bytes | "value_data" (10 bytes)                |
		// +----------------------------------------------------------------+
		cf.getColumnSerializer().serializeB(column, dos)
	}
}
