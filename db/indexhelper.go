// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"os"
	"sort"

	"github.com/DistAlchemist/Mongongo/utils"
)

// IndexInfo ...
//
// IndexInfo 并不是针对单个列的，而是用来记录一组列（或列范围）的信息。
// 它记录的内容通常是一个列范围内的第一个列和最后一个列的信息，以及这个范围内所有列在磁盘中存储的偏移量和数据大小。
//
// 字段说明：
//   - firstName：该列范围的第一个列的名称。
//   - lastName：该列范围的最后一个列的名称。
//   - offset：该范围中第一个列的偏移量，即从磁盘起始位置开始，这个列的数据是从哪里开始的。
//   - width：该范围的大小（即从第一个列到最后一个列之间的数据总大小）。
type IndexInfo struct {
	width     int64  // 列宽度，即该列在序列化字节流中的长度。
	lastName  []byte // 列的结束名称。
	firstName []byte // 列的起始名称。
	offset    int64  // 列的偏移量，是个相对偏移，需要先定位到数据部分的基址
}

// NewIndexInfo ...
func NewIndexInfo(firstName []byte, lastName []byte, offset int64, width int64) *IndexInfo {
	r := &IndexInfo{}
	r.firstName = firstName
	r.lastName = lastName
	r.offset = offset
	r.width = width
	return r
}

// 将 IndexInfo 对象序列化到 dos 中
func (r *IndexInfo) serialize(dos []byte) {
	// 将 firstName 和 lastName 以 []byte 写入 dos 。
	writeBytesB(dos, r.firstName)
	writeBytesB(dos, r.lastName)
	// 将 offset 和 width 以 64 位整数写入 dos 。
	writeInt64B(dos, r.offset)
	writeInt64B(dos, r.width)
}

// 返回 IndexInfo 对象序列化后的字节数：
//   - 4 + len(firstName)
//   - 4 + len(lastName)
//   - 8 + 8：表示 offset 和 width，各占 8 字节。
func (r *IndexInfo) serializedSize() int {
	return 4 + len(r.firstName) + 4 + len(r.lastName) + 8 + 8
}

// 从文件中读取 IndexInfo 对象的字节流并反序列化回 IndexInfo 结构体
func indexInfoDeserialize(dis *os.File) *IndexInfo {
	r := &IndexInfo{}
	str, _ := readString(dis)
	r.firstName = []byte(str)
	str, _ = readString(dis)
	r.lastName = []byte(str)
	r.offset = readInt64(dis)
	r.width = readInt64(dis)
	return r
}

// 从文件中反序列化多个 IndexInfo 对象，并返回一个包含这些对象的切片（[]*IndexInfo）
func deserializeIndex(in *os.File) []*IndexInfo {
	indexList := make([]*IndexInfo, 0)
	columnIndexSize := readInt(in) // 读取一个 4 字节的整数，表示后续字节流中索引部分的总字节数。
	start := getCurrentPos(in)
	for getCurrentPos(in) < start+int64(columnIndexSize) {
		indexList = append(indexList, indexInfoDeserialize(in))
	}
	return indexList
}

// 查找列名 name 在索引中归属的索引块，支持正向和反向查找。
func indexFor(name []byte, indexList []*IndexInfo, reversed bool) int {
	// the index of the IndexInfo in which name will be found.
	// if the index is len(indexList), the name appears nowhere
	if len(name) == 0 && reversed {
		return len(indexList) - 1
	}
	// target := NewIndexInfo(name, name, 0, 0)
	index := sort.Search(len(indexList), func(i int) bool {
		return (string(name) <= string(indexList[i].lastName) && string(name) >= string(indexList[i].firstName)) ||
			(string(name) <= string(indexList[i].firstName) && string(name) <= string(indexList[i].lastName))
	})
	return index
}

// 从文件中反序列化布隆过滤器
func defreezeBloomFilter(file *os.File) *utils.BloomFilter {
	// size := readInt(file)
	bytes, _ := readBytes(file)
	return utils.BFSerializer.DeserializeB(bytes)
}
