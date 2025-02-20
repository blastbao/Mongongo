// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import "log"

// SliceQueryFilter ...
type SliceQueryFilter struct {
	*AQueryFilter
	start    []byte // 查询范围的起始位置
	finish   []byte // 查询范围的结束位置
	reversed bool   // 是否反向查询
	count    int    // 查询的列数量
}

// NewSliceQueryFilter ...
func NewSliceQueryFilter(
	key string, // 主键
	columnParent *QueryPath, // 列族路径
	start, finish []byte, // 查询列的范围
	reversed bool, // 反向查询
	count int, // 返回的最大列数
) *SliceQueryFilter {

	s := &SliceQueryFilter{}
	s.AQueryFilter = NewAQueryFilter(key, columnParent)
	s.start = start
	s.finish = finish
	s.reversed = reversed
	s.count = count
	return s
}

func (s *SliceQueryFilter) getMemColumnIterator(memtable *Memtable) ColumnIterator {
	return memtable.getSliceIterator(s)
}

func (s *SliceQueryFilter) getSSTableColumnIterator(sstable *SSTableReader) ColumnIterator {
	return NewSSTableSliceIterator(sstable, s.key, s.start, s.reversed)
}

func (s *SliceQueryFilter) collectCollatedColumns(returnCF *ColumnFamily, collatedColumns *CollatedIterator, gcBefore int) {
	// define a 'reduced' iterator that merges columns with the same
	// name, which greatly simplies computing liveColumns in the
	// presence of tombstones.
	// BUT I will omit this part :)
	// TODO make a reduce iterator
	s.collectReducedColumns(returnCF, collatedColumns, gcBefore)
}

// 主要作用：此方法的主要作用是收集有效的列，并根据查询范围、删除标记、删除时间、最大列数等条件进行过滤，最终将有效列添加到目标列族 returnCF 中。
// 垃圾回收：方法通过 gcBefore 时间点来判断哪些列应该被丢弃（即已经标记为删除的列），并确保返回的数据不包含这些已删除的列。
// 反向查询：通过 reversed 标志，支持反向顺序查询，这对于一些特定的查询需求非常有用。
// 限制数量：通过 count 参数限制返回的列数，确保返回的数据符合请求的最大列数。
func (s *SliceQueryFilter) collectReducedColumns(
	returnCF *ColumnFamily, // 目标列族（最终将收集有效列的地方）
	reducedColumns *CollatedIterator, // 合并过的列迭代器（输入迭代器）
	gcBefore int, // 垃圾回收时间点（用于过滤已删除的列）
) {
	// 跟踪当前收集的有效列数
	liveColumns := 0
	for {
		// 按照列名字典序从小到大遍历 memtable/unflushed memtable/sstable 中属于 key 的 cf 的每一个列
		column := reducedColumns.next()
		if column == nil {
			break
		}

		// 限制返回列的数量
		if liveColumns >= s.count {
			break
		}

		log.Printf("collecting columns\n")
		// 检查列是否超出 finish 范围
		if len(s.finish) > 0 &&
			(!s.reversed && column.getName() > string(s.finish) ||
				s.reversed && column.getName() < string(s.finish)) {
			break
		}

		// only count live columns towards the `count` criteria
		// 检查列是否被标记为删除，如果列没被删除且它的最近更新时间晚于列族（returnCF）的删除时间，认为它是一个“活跃”的列（即有效列）
		if !column.isMarkedForDelete() &&
			(!returnCF.isMarkedForDelete() || column.mostRecentChangeAt() > returnCF.getMarkedForDeleteAt()) {
			liveColumns++
		}

		// but we need to add all non-gc-able columns to the result of read repair:
		// the column itself must be not gc-able, (1)
		// and if its container is deleted, the column must be changed more
		// recently than the container tombstone (2)
		// (since otherwise, the only thing repair cares about is the container tombstone)
		//
		// 检查列是否可被修复：
		// (1) 列未被删除 或者 列被删除但删除时间晚于 gcBefore 时间点，该列被视为可修复的列；
		// (2) 列族未被删除 或者 列族被删除但列的最近更新时间晚于列族的删除时间，该列被视为可修复的列；
		// 当列满足这两个条件时，才会被视为有效列，添加到目标列族中。
		if (!column.isMarkedForDelete() || column.getLocalDeletionTime() > gcBefore) && // (1)
			(!returnCF.isMarkedForDelete() || column.mostRecentChangeAt() > returnCF.markedForDeleteAt) { // (2)
			returnCF.addColumn(column) // 如果 returnCF 中已有同名 column ，根据优先级(timestamp)决定保留谁。
		}

	}
}
