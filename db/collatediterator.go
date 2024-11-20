// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

// CollatedIterator 将多个列迭代器合并成一个迭代器，以便按列名顺序（字典序）返回每一列的数据。

// CollatedIterator ...
type CollatedIterator struct {
	iterators []ColumnIterator // 多个迭代器
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

func (c *CollatedIterator) next() IColumn {
	minIndex := 0
	var minCol IColumn
	nilCnt := 0

	for i := range c.buf {
		if c.buf[i] == nil {
			c.buf[i] = c.iterators[i].next()
		}
		if c.buf[i] == nil {
			nilCnt++
		} else {
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
