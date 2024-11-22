// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"log"
	"os"
)

// SSTableNamesIterator ...
type SSTableNamesIterator struct {
	cf       *ColumnFamily
	curIndex int
	columns  [][]byte
	iter     []IColumn
}

// NewSSTableNamesIterator ...
func NewSSTableNamesIterator(sstable *SSTableReader, key string, columns [][]byte) *SSTableNamesIterator {
	r := &SSTableNamesIterator{}
	r.columns = columns
	r.curIndex = 0

	// 根据索引文件获取 key 在数据文件中的存储位置
	decoratedKey := sstable.partitioner.DecorateKey(key)
	position := sstable.getPosition(decoratedKey)
	if position < 0 {
		return r
	}

	// 打开数据文件，定位到 key 的数据存储位置
	file, err := os.Open(sstable.dataFileName)
	if err != nil {
		log.Fatal(err)
	}
	if _, err = file.Seek(position, 0); err != nil {
		log.Fatal(err)
	}

	// key 的一个列族下多个列是整体存储的，

	// len(key)
	// key
	// len(val)
	// bloom filter
	// column index list ===>  [<column_name, offset>, <column_name, offset>,...]
	// column count
	// ===== column data =======
	// 	---------
	//  Column Name
	//  DeleteMark
	//  Timestamp
	//	Value
	//	---------
	//  Column Name
	//  DeleteMark
	//  Timestamp
	//	Value
	//	---------
	//  ...

	keyInDisk, _ := readString(file)
	if keyInDisk != decoratedKey {
		log.Fatal("keyInDisk should == decoratedKey")
	}

	readInt(file)
	// read the bloom filter that summarizing the columns
	// bf 记录了 key 可能包含的列，不是准确的
	bf := defreezeBloomFilter(file)
	filteredColumnNames := make([][]byte, len(columns))
	for _, name := range columns {
		if bf.IsPresent(string(name)) {
			filteredColumnNames = append(filteredColumnNames, name)
		}
	}
	if len(filteredColumnNames) == 0 {
		return r
	}

	// index 中包含了 key 的各个列存储的位置索引
	indexList := deserializeIndex(file)
	cf := CFSerializer.deserializeFromSSTableNoColumns(sstable.makeColumnFamily(), file)
	// 读取 Key 的列的数目
	readInt(file) // columncount
	ranges := make([]*IndexInfo, 0)
	// get the various column ranges we have to read
	for _, name := range filteredColumnNames {
		// 查找列在哪个索引块中
		index := indexFor(name, indexList, false)
		// 不存在
		if index == len(indexList) {
			continue
		}
		// 不合法
		indexInfo := indexList[index]
		if string(name) < string(indexInfo.firstName) {
			continue
		}
		// 找到目标索引块
		ranges = append(ranges, indexInfo)
	}

	// seek to the correct offset to the data
	columnBegin := getCurrentPos(file)
	// now read all the columns from the ranges
	for _, indexInfo := range ranges {
		// 定位到索引块指向的存储偏移
		file.Seek(columnBegin+indexInfo.offset, 0)
		// 从 file 中不断读取 column ，找到目标 column 存入 cf 中
		for getCurrentPos(file) < columnBegin+indexInfo.offset+indexInfo.width {
			column := cf.getColumnSerializer().deserialize(file)
			// we check vs the origin list, not the filtered list for efficiency
			if containsC(columns, column.getName()) {
				cf.addColumn(column)
			}
		}
	}

	file.Close()
	r.iter = cf.GetSortedColumns()
	return r
}

func (r *SSTableNamesIterator) getColumnFamily() *ColumnFamily {
	return r.cf
}

func (r *SSTableNamesIterator) computeNext() IColumn {
	if r.iter == nil || !r.hasNext() {
		return nil
	}
	r.curIndex++
	return r.iter[r.curIndex-1]
}

func (r *SSTableNamesIterator) hasNext() bool {
	return r.curIndex < len(r.iter)
}

func (r *SSTableNamesIterator) next() IColumn {
	return r.computeNext()
}

func (r *SSTableNamesIterator) close() {}
