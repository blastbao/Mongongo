// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

// NamesQueryFilter ...
type NamesQueryFilter struct {
	key     string     // 指定读取哪个 key
	path    *QueryPath //
	columns [][]byte   // 指定读取哪些列
}

// NewNamesQueryFilter ...
//
// 获取 key 的一个 Column
func NewNamesQueryFilter(key string, columnParent *QueryPath, column []byte) QueryFilter {
	n := &NamesQueryFilter{}
	n.key = key
	n.path = columnParent
	n.columns = make([][]byte, 1)
	n.columns = append(n.columns, column)
	return n
}

// NewNamesQueryFilterS ...
//
// 获取 key 的多个 Column
func NewNamesQueryFilterS(key string, columnParent *QueryPath, columns [][]byte) QueryFilter {
	n := &NamesQueryFilter{}
	n.key = key
	n.path = columnParent
	n.columns = columns
	return n
}
func (filter *NamesQueryFilter) getKey() string {
	return filter.key
}
func (filter *NamesQueryFilter) getPath() *QueryPath {
	return filter.path
}

func containsC(list [][]byte, name string) bool {
	for _, elem := range list {
		if string(elem) == name {
			return true
		}
	}
	return false
}
func (filter *NamesQueryFilter) filterSuperColumn(superColumn SuperColumn, gcBefore int) SuperColumn {
	for name := range superColumn.getSubColumns() {
		if containsC(filter.columns, name) == false {
			superColumn.Remove(name)
		}
	}
	return superColumn
}

func (filter *NamesQueryFilter) getMemColumnIterator(memtable *Memtable) ColumnIterator {
	return memtable.getNamesIterator(filter)
}

func (filter *NamesQueryFilter) getSSTableColumnIterator(sstable *SSTableReader) ColumnIterator {
	return NewSSTableNamesIterator(sstable, filter.key, filter.columns)
}

func (filter *NamesQueryFilter) collectCollatedColumns(returnCF *ColumnFamily, collatedColumns *CollatedIterator, gcBefore int) {
	return
}
