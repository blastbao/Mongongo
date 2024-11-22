// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"log"

	"github.com/DistAlchemist/Mongongo/config"
	"github.com/DistAlchemist/Mongongo/utils"
)

// CIndexer ...
var CIndexer = &ColumnIndexer{}

// ColumnIndexer ...
type ColumnIndexer struct{}

func (c *ColumnIndexer) serialize(cf *ColumnFamily, dos []byte) {
	// 按列名顺序
	columns := cf.GetSortedColumns()
	// 创建 column bf ，用于快速确定一个 col 是否不存在
	bf := c.createColumnBloomFilter(columns)
	// 把 bf 序列化到 buf 中
	buf := make([]byte, 0)
	utils.BFSerializer.SerializeB(bf, buf)
	// 把 len(buf) + buf 写入到 dos 中
	writeBytesB(dos, buf)
	// 建立列索引表 []*IndexInfo 并序列化存入 dos 中；IndexInfo 记录列在数据文件中的偏移（相对偏移）和大小，用于加速查找。
	c.doIndexing(columns, dos)
}

// 布隆过滤器用于快速判断列是否存在，以减少了不必要的开销。
//   - 如果布隆过滤器表明该列不存在，那么你可以直接跳过读取操作。
//   - 如果布隆过滤器表明该列可能存在，你才会进一步实际访问列的数据。
//
// 步骤：
//   - 统计总列数
//   - 创建布隆过滤器
//   - 填充布隆过滤器：将列族中每个列名（包括超列及其子列）都写入布隆过滤器。
func (c *ColumnIndexer) createColumnBloomFilter(columns []IColumn) *utils.BloomFilter {
	columnCount := 0
	for _, column := range columns {
		columnCount += column.getObjectCount()
	}
	bf := utils.NewBloomFilter(columnCount, 4)
	for _, column := range columns {
		bf.Fill(column.getName())
		// if this is super column type
		// we need to get the subColumns too
		_, ok := column.(SuperColumn)
		if ok {
			subColumns := column.GetSubColumns()
			for _, subColumn := range subColumns {
				bf.Fill(subColumn.getName())
			}
		}
	}
	return bf
}

// [重要]
// 在 Cassandra 或类似的数据库中，列是由一个唯一的列名标识的。
// 假设你已经知道要查找的列名，接下来的任务就是通过索引来定位该列在存储中的位置。
//
// 索引为列名和它们在数据文件中的位置创建了映射，查找时使用列名来在索引中找到对应的列的存储偏移量和数据长度。
// 查找过程的基本步骤：
//   - 索引查找：通过给定的列名，找到它所在的索引块，索引块的区间由 firstName 和 lastName 来定义，可以通过二分查找来定位列归属的区间对应的索引块。
//   - 获取存储偏移量：一旦找到列名对应的索引块，你就可以通过 IndexInfo 对象获取列在存储中的偏移量（即它在磁盘文件中的位置）和列数据的大小。
//   - 进一步在这个索引范围内查找目标列
//
// 在索引中，IndexInfo 记录的是 多个列的连续范围的偏移量和宽度，而不是单独每一列的偏移量。
// 由于 IndexInfo 包含的是一个列范围，而不是单独列的信息，所以如果你要查找的列恰好落在这个范围内，并不一定会直接得到该列的偏移量和大小。
// 我们就需要进一步在这个索引范围内查找目标列。
func (c *ColumnIndexer) doIndexing(columns []IColumn, dos []byte) {
	// Given the collection of columns in the column family,
	// the name index is generated and written into the provided
	// stream

	// 没有列要索引，直接将索引大小设为 0 。
	if len(columns) == 0 {
		// empty write index size 0
		writeIntB(dos, 0) // writeIntB 用于将整数以大端字节序写入 dos 。
		return
	}

	// indexList maintains a list of ColumnIndexInfo objects
	// for the columns in this column family. The key is the
	// column name and the position is the relative offset of
	// that column name from the start of the list. Doing this
	// so that we don't read all the columns into memory.
	indexList := make([]*IndexInfo, 0)  //
	endPosition, startPosition := 0, -1 // 标记当前索引块的开始和结束位置
	indexSizeInBytes := 0               // 记录所有索引的总字节数
	var column, firstColumn IColumn     // 表示当前列和第一个列，用于生成索引

	// column offsets at the right thresholds into the index map
	for _, column = range columns {
		// 如果是新索引的第一个列，列名和起始位置会被保存下来。
		if firstColumn == nil {
			firstColumn = column
			startPosition = endPosition
		}
		// 累加每个列的序列化大小
		endPosition += int(column.serializedSize())

		// if we hit the column index size that we have to index after, go ahead and index it
		// 如果累积的列大小超过了阈值（config.GetColumnIndexSize()），则生成一个 IndexInfo 并添加到 indexList 中
		if endPosition-startPosition >= config.GetColumnIndexSize() {
			cIndexInfo := NewIndexInfo(
				[]byte(firstColumn.getName()),    // 第一个列的列名
				[]byte(column.getName()),         // 当前列的列名
				int64(startPosition),             // 第一个列的起始位置
				int64(endPosition-startPosition), // 这几个列的总长度
			)
			indexList = append(indexList, cIndexInfo)       // 添加到 indexList 中
			indexSizeInBytes += cIndexInfo.serializedSize() // 更新索引大小
			firstColumn = nil                               // 从头开始生成新索引
		}
	}

	// the last column may have fallen on an index boundary already. if not, index it explicitly.
	//
	// 在循环结束后，如果 indexList 中没有索引，或者最后一列已经超出了索引边界，则手动创建一个新的索引并添加到 indexList。
	if len(indexList) == 0 || string(indexList[len(indexList)-1].lastName) != column.getName() {
		cIndexInfo := NewIndexInfo(
			[]byte(firstColumn.getName()),
			[]byte(column.getName()),
			int64(startPosition),
			int64(endPosition-startPosition),
		)
		indexList = append(indexList, cIndexInfo)
		indexSizeInBytes += cIndexInfo.serializedSize()
	}

	// 没有有效的索引信息，程序会触发错误并终止
	if indexSizeInBytes <= 0 {
		log.Fatal("index size should > 0")
	}

	// 将索引总大小 indexSizeInBytes 写入dos 。
	writeIntB(dos, indexSizeInBytes)
	// 将每个 IndexInfo 对象序列化并写入字节流
	for _, cIndexInfo := range indexList {
		cIndexInfo.serialize(dos)
	}
}
