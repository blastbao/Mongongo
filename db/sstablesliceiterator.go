// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"log"
	"os"

	"gopkg.in/karalabe/cookiejar.v1/collections/deque"
)

// SSTableSliceIterator ...
type SSTableSliceIterator struct {
	reversed    bool               // 正序读取还是逆序读取
	startColumn []byte             // 起始列，为空则从第一列开始
	reader      *ColumnGroupReader // 读取列数据
	nextValue   IColumn            // 存储下一个返回的列数据
	nextRead    bool               // 是否已经读取了下一个值
}

// NewSSTableSliceIterator ...
func NewSSTableSliceIterator(ssTable *SSTableReader, key string, startColumn []byte, reversed bool) *SSTableSliceIterator {
	s := &SSTableSliceIterator{}
	s.reversed = reversed
	s.startColumn = startColumn
	s.nextValue = nil
	s.nextRead = false

	// 定位到 key 的存储位置
	decoratedKey := ssTable.partitioner.DecorateKey(key)
	position := ssTable.getPosition(decoratedKey)
	if position >= 0 {
		// 在数据文件中从 position 开始解析 key 关联的列族信息，并构建 Iterator 用来从 startColumn 开始按照 reversed 序遍历 column 。
		s.reader = NewColumnGroupReader(ssTable, decoratedKey, position, startColumn, reversed)
	}
	return s
}

// 是否还有下一个列
func (s *SSTableSliceIterator) hasNext() bool {
	if s.nextRead == true {
		return s.nextValue != nil
	}
	s.nextRead = true
	s.nextValue = s.computeNext()
	return s.nextValue != nil
}

func (s *SSTableSliceIterator) next() IColumn {
	if s.nextRead == true {
		s.nextRead = false
		return s.nextValue
	}
	return s.computeNext()
}

func (s *SSTableSliceIterator) computeNext() IColumn {
	if s.reader == nil {
		return nil
	}
	for {
		// 取一个列
		column := s.reader.pollColumn()
		if column == nil {
			return nil
		}
		// 判断当前列是否符合遍历条件
		if s.isColumnNeeded(column) {
			return column
		}
	}
}

// 判断当前列是否符合遍历条件
//   - 正序遍历: 列名必须大于或等于 startColumn
//   - 逆序遍历: 列名必须小于或等于 startColumn ，当 startColumn 为空时不做限制
func (s *SSTableSliceIterator) isColumnNeeded(column IColumn) bool {
	if s.reversed {
		return len(s.startColumn) == 0 || column.getName() <= string(s.startColumn)
	}
	return column.getName() >= string(s.startColumn)
}

func (s *SSTableSliceIterator) close() {
	if s.reader != nil {
		s.reader.close()
	}
}

func (s *SSTableSliceIterator) getColumnFamily() *ColumnFamily {
	return s.reader.getEmptyColumnFamily()
}

// ColumnGroupReader finds the block for a starting
// column and returns blocks before/after it for each
// next call. This function assumes that the CF is
// sorted by name and exploits the name index
type ColumnGroupReader struct {
	emptyColumnFamily   *ColumnFamily
	indices             []*IndexInfo
	columnStartPosition int64
	file                *os.File
	curRangeIndex       int
	blockColumns        *deque.Deque
	reversed            bool
}

// NewColumnGroupReader ...
//
// 作用：
//   - 打开 ssTable 数据文件并定位 position 位置上，该位置存储了 key 的数据。
//   - 跳过布隆过滤器和读取其他元数据。
//   - 读取索引块信息，根据 startColumn 和 reversed 定位目标索引块。
//   - 返回 ColumnGroupReader 用于从目标块开始遍历各个块内的 column 信息。
func NewColumnGroupReader(ssTable *SSTableReader, key string, position int64, startColumn []byte, reversed bool) *ColumnGroupReader {
	c := &ColumnGroupReader{}
	var err error

	// 打开文件
	c.file, err = os.Open(ssTable.getFilename())
	if err != nil {
		log.Fatal(err)
	}

	// 定位到指定的位置，通常是 SSTable 中某个 key 的存储偏移
	c.file.Seek(position, 0)

	// 读取 key 并校验
	keyInDisk, _ := readString(c.file)
	if key != keyInDisk {
		log.Fatal("key should equals to keyInDisk")
	}
	// 读取 value size
	readInt(c.file) // 读取一个 uint32 // read row size
	// 跳过布隆过滤器
	skipBloomFilter(c.file)
	// 读取多个 IndexInfo 对象，用于快速定位不同列的存储位置。
	c.indices = deserializeIndex(c.file)

	// ???
	c.emptyColumnFamily = CFSerializer.deserializeFromSSTableNoColumns(ssTable.makeColumnFamily(), c.file)

	// 读取列数量
	readInt(c.file)

	// 列数据的起始偏移位置
	c.columnStartPosition = getCurrentPos(c.file)
	// 查找列名 startColumn 在索引 c.indices 归属的索引块
	c.curRangeIndex = indexFor(startColumn, c.indices, reversed)
	// 正序 or 逆序遍历
	c.reversed = reversed
	// 如果是反向遍历，且 curRangeIndex 超出了索引的边界（即当前索引是最后一个），则将索引向前调整一个位置，确保能够正确读取数据。
	if reversed && c.curRangeIndex == len(c.indices) {
		c.curRangeIndex--
	}
	return c
}

func (c *ColumnGroupReader) getEmptyColumnFamily() *ColumnFamily {
	return c.emptyColumnFamily
}

func (c *ColumnGroupReader) close() {
	c.file.Close()
}

// 检查当前列块是否为空，如果不为空从中取出一个列并返回，否则加载下一个数据块后，再取出一列返回。
func (c *ColumnGroupReader) pollColumn() IColumn {
	if c.blockColumns.Size() == 0 {
		if !c.getNextBlock() {
			return nil
		}
	}
	return c.blockColumns.PopLeft().(IColumn)
}

// 读取一个索引块，然后将该索引块指向的所有底层 column 从 data file 中读出来，存储到 c.blockColumns 中。
func (c *ColumnGroupReader) getNextBlock() bool {
	// 索引边界检查
	if c.curRangeIndex < 0 || c.curRangeIndex >= len(c.indices) {
		return false
	}

	// seek to the correct offset to the data, and calculate the data size

	// 当前索引块
	curColPosition := c.indices[c.curRangeIndex]
	// 定位到当前索引块指向的数据位置
	c.file.Seek(c.columnStartPosition+curColPosition.offset, 0)
	// 遍历数据
	for getCurrentPos(c.file) < c.columnStartPosition+curColPosition.offset+curColPosition.width {
		// 读取一个列
		column := c.emptyColumnFamily.getColumnSerializer().deserialize(c.file)
		// 保存到 queue 中
		if c.reversed {
			c.blockColumns.PushLeft(column)
		} else {
			c.blockColumns.PushRight(column)
		}
	}

	// 定位到下一个索引块
	if c.reversed {
		c.curRangeIndex--
	} else {
		c.curRangeIndex++
	}

	return true
}
