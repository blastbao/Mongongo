// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

// CollatedIterator 将多个列迭代器合并成一个迭代器，以便按列名顺序（字典序）返回每一列的数据。

// CollatedIterator ...
type CollatedIterator struct {
	iterators []ColumnIterator // 多个迭代器，每个 ColumnIterator 用来遍历一个特定的列族数据源（如 Memtable、SSTable 等）。
	buf       map[int]IColumn  // 每个迭代器指向的列
}

// NewCollatedIterator ...
func NewCollatedIterator(iterators []ColumnIterator) *CollatedIterator {
	c := &CollatedIterator{}
	c.iterators = iterators
	c.buf = make(map[int]IColumn)
	for i := range iterators {
		c.buf[i] = nil
	}
	return c
}

// 从多个列族迭代器中获取下一个列（IColumn）。
// 通过比较各个迭代器当前指向的列的列名，返回列名字典序最小的那个列，确保返回的 IColumn 是按照名称从小到大排序的。
func (c *CollatedIterator) next() IColumn {
	minIndex := 0
	var minCol IColumn
	nilCnt := 0

	for i := range c.buf {
		if c.buf[i] == nil {
			c.buf[i] = c.iterators[i].next() // 获取当前迭代器的下一个列
		}
		if c.buf[i] == nil { // 迭代器已经读取完毕
			nilCnt++
		} else {
			// 获取列名最小的哪个列的索引
			if minCol == nil {
				minIndex = i
				minCol = c.buf[i]
			} else {
				if c.buf[i].getName() < minCol.getName() {
					minIndex = i
					minCol = c.buf[i]
				}
			}
		}
	}

	c.buf[minIndex] = nil
	return minCol
}
