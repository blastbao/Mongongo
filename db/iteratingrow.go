// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"log"
	"os"
)

// IteratingRow ...
//
// 迭代 key 在 sstable 中的每个 column
type IteratingRow struct {
	key               string
	finishedAt        int64
	emptyColumnFamily *ColumnFamily
	file              *os.File
}

// NewIteratingRow ...
func NewIteratingRow(file *os.File, sstable *SSTableReader) *IteratingRow {
	r := &IteratingRow{}
	r.file = file
	r.key, _ = readString(file)      // len(key) + key
	dataSize := int64(readInt(file)) // 数据大小
	dataStart := getCurrentPos(file)
	r.finishedAt = dataStart + dataSize // 数据区域结束地址
	skipBloomFilterAndIndex(file)       // 跳过 bf 和 index
	r.emptyColumnFamily = CFSerializer.deserializeFromSSTableNoColumns(sstable.makeColumnFamily(), file)
	readInt(file) // 读取列的数目
	return r
}

func (r *IteratingRow) hasNext() bool {
	// 是否结束
	return r.finishedAt != getCurrentPos(r.file)
}

func (r *IteratingRow) next() IColumn {
	// 是否结束
	if r.finishedAt == getCurrentPos(r.file) {
		log.Fatal("reach end of row")
		return Column{}
	}
	// 读取一个列
	return r.emptyColumnFamily.columnSerializer.deserialize(r.file)
}

func (r *IteratingRow) getEmptyColumnFamily() *ColumnFamily {
	return r.emptyColumnFamily
}

func (r *IteratingRow) skipRemaining() {
	// 直接跳转到当前 key 的数据区域尾部
	r.file.Seek(r.finishedAt, 0)
}

func skipBloomFilterAndIndex(in *os.File) int {
	return skipBloomFilter(in) + skipIndex(in)
}

func skipBloomFilter(in *os.File) int {
	totalBytesRead := 0
	// size of the bloom filter
	size := readInt(in)
	totalBytesRead += 4
	// skip the serialized bloom filter
	curPos := getCurrentPos(in)
	n, err := in.Seek(curPos+int64(size), 0)
	if err != nil {
		log.Fatal(err)
	}
	if int64(size) != n {
		log.Fatal("reach EOF")
	}
	totalBytesRead += size
	return totalBytesRead
}

func skipIndex(file *os.File) int {
	// read only the column index list
	columnIndexSize := readInt(file)
	totalBytesRead := 4
	// skip the column index data
	curPos := getCurrentPos(file)
	n, err := file.Seek(curPos+int64(columnIndexSize), 0)
	if err != nil {
		log.Fatal(err)
	}
	if int64(columnIndexSize) != n {
		log.Fatal("read EOF")
	}
	totalBytesRead += columnIndexSize
	return totalBytesRead
}
