// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

// 当某个 row 的一个列族包含多个列时，使用 ColumnIterator 可以顺序访问这些列值；

// ColumnIterator ...
type ColumnIterator interface {
	getColumnFamily() *ColumnFamily // 返回与当前列迭代器关联的列族
	close()                         // 关闭当前迭代器，释放资源
	hasNext() bool                  // 判断是否还有下一个元素
	next() IColumn                  // 返回下一个列对象
}

// SimpleColumnIterator ...
type SimpleColumnIterator struct {
	curIndex     int           // 迭代器指向的列索引
	columnFamily *ColumnFamily // 迭代器所在的列族
	columns      [][]byte
}

// NewSColumnIterator ...
func NewSColumnIterator(curIndex int, cf *ColumnFamily, columns [][]byte) *SimpleColumnIterator {
	c := &SimpleColumnIterator{}
	c.curIndex = 0
	c.columnFamily = cf
	c.columns = columns
	return c
}

func (c *SimpleColumnIterator) getColumnFamily() *ColumnFamily {
	return c.columnFamily
}
func (c *SimpleColumnIterator) close() {
	return
}
func (c *SimpleColumnIterator) hasNext() bool {
	return c.curIndex < len(c.columns)
}
func (c *SimpleColumnIterator) next() IColumn {
	if c.hasNext() == false {
		return nil
	}
	c.curIndex++
	return c.columnFamily.GetColumn(string(c.columns[c.curIndex]))
}

// AbstractColumnIterator ...
type AbstractColumnIterator struct {
	curIndex     int
	columnFamily *ColumnFamily
	columns      []IColumn
}

// NewAColumnIterator ...
func NewAColumnIterator(curIndex int, columnFamily *ColumnFamily, columns []IColumn) *AbstractColumnIterator {
	c := &AbstractColumnIterator{}
	c.curIndex = curIndex
	c.columnFamily = columnFamily
	c.columns = columns
	return c
}

func (c *AbstractColumnIterator) getColumnFamily() *ColumnFamily {
	return c.columnFamily
}

func (c *AbstractColumnIterator) close() {}

func (c *AbstractColumnIterator) hasNext() bool {
	return c.curIndex < len(c.columns)
}

func (c *AbstractColumnIterator) next() IColumn {
	if c.hasNext() == false {
		return nil
	}
	c.curIndex++
	return c.columns[c.curIndex-1]
}
