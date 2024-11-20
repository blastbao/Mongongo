// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"encoding/binary"
	"sync/atomic"
)

// Row for key and cf
type Row struct {
	Table          string                   // 表名
	Key            string                   // 主键
	ColumnFamilies map[string]*ColumnFamily // 关联的列族，每个列族代表一组列的集合
	Size           int32                    // 行的总大小可能包括所有列族和列族数据的大小）
}

// NewRow init a Row with given key
func NewRow(key string) *Row {
	r := &Row{}
	r.Key = key
	r.ColumnFamilies = make(map[string]*ColumnFamily)
	r.Size = 0
	return r
}

// NewRowT init a Row with given table name and key name
func NewRowT(table, key string) *Row {
	r := &Row{}
	r.Table = table
	r.Key = key
	r.ColumnFamilies = make(map[string]*ColumnFamily)
	return r
}

func (r *Row) getColumnFamilies() map[string]*ColumnFamily {
	return r.ColumnFamilies
}

func (r *Row) addColumnFamily(cf *ColumnFamily) {
	r.ColumnFamilies[cf.ColumnFamilyName] = cf
	atomic.AddInt32(&r.Size, cf.getSize())
}

// 将当前行（Row）序列化为字节数组。
//   - 写入 Key ：首先写入主键 Key 的长度（uint32 类型，4 字节），然后写入实际的 Key 字符串。
//   - 写入列族数量：接着写入列族的数量。
//   - 写入列族数据：如果行的大小大于 0（即包含列族数据），则遍历所有列族并将其序列化为字节数组后添加到 buf 中。
func (r *Row) toByteArray() []byte {
	buf := make([]byte, 0)
	// write key length
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(len(r.Key)))
	buf = append(buf, b4...)
	// write key string
	buf = append(buf, []byte(r.Key)...)
	// write cf size
	binary.BigEndian.PutUint32(b4, uint32(len(r.ColumnFamilies)))
	buf = append(buf, b4...)
	// write cf bytes
	if r.Size > 0 {
		for _, columnFamily := range r.ColumnFamilies {
			buf = append(buf, columnFamily.toByteArray()...)
		}
	}
	return buf
}

func (r *Row) clear() {
	r.ColumnFamilies = make(map[string]*ColumnFamily)
}

// 将一个 Row 对象序列化为字节数据，并将序列化结果写入 dos 中。
//   - 将 Table/Key 写入 dos
//   - 写入列族数目
//   - 将每个列族序列化到 dos
func rowSerialize(row *Row, dos []byte) {
	writeStringB(dos, row.Table)
	writeStringB(dos, row.Key)
	columnFamilies := row.getColumnFamilies()
	size := len(columnFamilies)
	writeInt32B(dos, int32(size))
	for _, cf := range columnFamilies {
		CFSerializer.serialize(cf, dos)
	}
}
