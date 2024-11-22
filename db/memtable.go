// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/DistAlchemist/Mongongo/config"
)

// Memtable specifies memtable
type Memtable struct {
	flushKey         string
	threshold        int32
	thresholdCnt     int32
	currentSize      int32
	currentObjectCnt int32
	// table and cf name are used to determine the
	// ColumnFamilyStore
	tableName string
	cfName    string
	// creation time of this memtable
	creationTime   int64
	isFrozen       bool
	isDirty        bool
	isFlushed      bool
	columnFamilies map[string]ColumnFamily
	// lock and condition for notifying new clients about Memtable switches
	mu   sync.Mutex
	cond *sync.Cond
}

// NewMemtable initializes a new memtable
func NewMemtable(table, cfName string) *Memtable {
	m := &Memtable{}
	m.flushKey = "FlushKey"
	m.threshold = int32(config.MemtableSize * 1024 * 1024)
	m.thresholdCnt = int32(config.MemtableObjectCount * 1024 * 1024)
	m.currentSize = 0
	m.currentObjectCnt = 0
	m.isFrozen = false
	m.isDirty = false
	m.isFlushed = false
	m.columnFamilies = make(map[string]ColumnFamily)
	m.cond = sync.NewCond(&m.mu)
	m.tableName = table
	m.cfName = cfName
	m.creationTime = time.Now().UnixNano() / int64(time.Millisecond)
	return m
}

func (m *Memtable) put(key string, columnFamily *ColumnFamily) {
	// should only be called by ColumnFamilyStore.apply
	// 如果 Memtable 被标记为冻结状态，报错
	if m.isFrozen {
		log.Fatal("memtable is frozen!")
	}
	// 更新标记位
	m.isDirty = true
	// 保存 key 列族信息
	m.runResolve(key, columnFamily)
}

// 如果 Memtable 中已经存在指定 key 的列族（ColumnFamily），则更新这个列族；如果不存在，则将新的列族数据添加到 Memtable 中。
func (m *Memtable) runResolve(key string, newcf *ColumnFamily) {
	// 检查 Memtable 中是否已经存在该 key 对应的列族
	cf, ok := m.columnFamilies[key]
	if ok {
		// 如果 Memtable 中存在该 key 的列族，进行合并，根据时间戳决定相同的列留谁
		oldSize := cf.size
		oldColCount := cf.getColumnCount()
		cf.addColumns(newcf) // 把 newcf 中的 column 添加到 cf 中，如果有相同 column 根据优先级(timestamp)决定保留谁。
		newSize := cf.size
		newColCount := cf.getColumnCount()
		m.resolveSize(oldSize, newSize)          // 更新 m.currentSize
		m.resolveCount(oldColCount, newColCount) // 更新 m.currentObjectCnt
		cf.deleteCF(newcf)                       // 合并 cf.localDeletionTime, cf.markedForDeleteAt 并更新
	} else {
		// 如果 Memtable 中没有该 key 的列族，则直接保存新列族
		m.columnFamilies[key] = *newcf
		atomic.AddInt32(&m.currentSize, newcf.size+int32(len(key)))         // 更新 m.currentSize
		atomic.AddInt32(&m.currentObjectCnt, int32(newcf.getColumnCount())) // 更新 m.currentObjectCnt
	}
}

func (m *Memtable) resolveSize(oldSize, newSize int32) {
	atomic.AddInt32(&m.currentSize, int32(newSize-oldSize))
}

func (m *Memtable) resolveCount(oldCount, newCount int) {
	atomic.AddInt32(&m.currentObjectCnt, int32(newCount-oldCount))
}

// 判断 Memtable 当前的大小/对象数目是否超过了阈值
func (m *Memtable) isThresholdViolated() bool {
	if m.currentSize >= m.threshold || m.currentObjectCnt >= m.thresholdCnt {
		return true
	}
	return false
}

// 将 Memtable 中的数据写入到磁盘上的 SSTable 文件中，确保数据不会丢失。
func (m *Memtable) flush(cLogCtx *CommitLogContext) {
	// 1. 一张表有很多个列族，一个 Memtable 只负责一个列族，先获取对应的列族存储
	cfStore := OpenTable(m.tableName).columnFamilyStores[m.cfName]
	// 2. 创建一个新的 SSTable 写入器，用于将 Memtable 中的数据写入到 SSTable 临时文件中，len(m.columnFamilies) 是 key 的数目。
	writer := NewSSTableWriter(cfStore.getTmpSSTablePath(), len(m.columnFamilies))
	// 3. 要将 keys 按序写入到 sstable 中。
	orderedKeys := make([]string, 0)
	for cfName := range m.columnFamilies {
		orderedKeys = append(orderedKeys, writer.partitioner.DecorateKey(cfName))
	}
	sort.Sort(ByKey(orderedKeys))

	// 4. 逐个 key 写入 SSTable 文件
	for _, key := range orderedKeys {
		// 获取原始键
		k := writer.partitioner.UndecorateKey(key)
		// 获取列族
		columnFamily, _ := m.columnFamilies[k]
		// 将列族序列化（列排序、bf、列索引、列数据）后写入到 SSTable
		buf := make([]byte, 0)
		CFSerializer.serializeWithIndexes(&columnFamily, buf)
		writer.append(key, buf)
	}

	// 5. 完成写入并获取 SSTable 文件的读取器
	ssTable := writer.closeAndOpenReader()
	// 6. 通知列族存储 Memtable 已经被刷新
	cfStore.onMemtableFlush(cLogCtx)
	// 7. 存储该 SSTable 文件的位置
	cfStore.storeLocation(ssTable)
	// 8. 设置 Memtable 为已刷新状态
	m.isFlushed = true
	// 9. 输出日志，表示 Memtable 刷新完成
	log.Print("Completed flushing ", ssTable.getFilename())
}

func (m *Memtable) freeze() {
	m.isFrozen = true
}

func reverse(a []IColumn) {
	for i, j := 0, len(a)-1; i < j; i, j = i+1, j-1 {
		a[i], a[j] = a[j], a[i]
	}
}

func (m *Memtable) getNamesIterator(filter *NamesQueryFilter) ColumnIterator {
	// 1. 从 Memtable 中查找与给定 key 对应的 ColumnFamily
	cf, ok := m.columnFamilies[filter.key]
	spew.Printf("\tcf get from memtable: %#+v\n\n", cf)
	// 2. 如果 Memtable 中没有该 key 对应的 ColumnFamily，则创建一个新的 ColumnFamily
	var columnFamily *ColumnFamily
	if ok == false {
		columnFamily = createColumnFamily(m.tableName, filter.path.ColumnFamilyName)
	} else {
		// 如果 Memtable 中已经有该 key 对应的 ColumnFamily，直接使用它
		// columnFamily = cf.cloneMeShallow()
		columnFamily = &cf
		spew.Printf("\tshould enter here, cf: %#+v\n\n", columnFamily)
	}
	// 3. 返回一个新的 ColumnIterator，传入列族、起始位置、查询的列，用于遍历该列族中的数据。
	return NewSColumnIterator(0, columnFamily, filter.columns)
}

// obtain an iterator of columns in this memtable in the specified
// order starting from a given column
func (m *Memtable) getSliceIterator(filter *SliceQueryFilter) ColumnIterator {
	// 1. 从 Memtable 中查找对应 key 的列族
	cf, ok := m.columnFamilies[filter.key] // rowKey -> column family
	var columnFamily *ColumnFamily
	var columns []IColumn
	// 2. 如果 Memtable 中没有找到该列族，则创建新的列族并获取排序后的列
	if ok == false {
		columnFamily = createColumnFamily(m.tableName, filter.path.ColumnFamilyName)
		columns = columnFamily.GetSortedColumns()
	} else {
		// 如果找到了列族，克隆一份列族数据并获取排序后的列
		columnFamily = cf.cloneMeShallow()
		columns = cf.GetSortedColumns()
	}
	// 3. 如果查询条件指定了倒序，反转列的顺序，支持从后往前遍历列族数据。
	if filter.reversed == true {
		reverse(columns)
	}
	// 4. 根据查询条件创建起始列，决定从哪个列开始遍历
	// startIColumn 是查询的起始列，它由 filter.start（指定的列名）决定。
	// 如果列族类型是 "Standard"，则创建一个普通列（NewColumn）；否则，创建一个超级列（NewSuperColumn）。
	var startIColumn IColumn
	if config.GetColumnTypeTableName(m.tableName, filter.path.ColumnFamilyName) == "Standard" {
		startIColumn = NewColumn(string(filter.start), "", 0, false)
	} else {
		startIColumn = NewSuperColumn(string(filter.start))
	}
	// 5. 根据起始列名计算出起始索引
	// 通过二分查找（sort.Search）确定查询的起始列在 columns 列表中的位置，即找到第一个列名大于或等于 startIColumn.getName() 的位置。
	index := 0
	if len(filter.start) == 0 && filter.reversed {
		// scan from the largest column in descending order
		// 如果没有指定起始列并且需要倒序，则从最大的列开始
		index = 0
	} else {
		// 否则，查找第一个列的名字大于等于 `startIColumn` 的位置
		index = sort.Search(len(columns), func(i int) bool {
			return columns[i].getName() >= startIColumn.getName()
		})
	}

	// 6. 返回一个列族的迭代器（ColumnIterator），从指定的 startIndex 索引位置开始，遍历 columns 中的列数据。
	startIndex := index
	return NewAColumnIterator(startIndex, columnFamily, columns)
}

func (m *Memtable) isClean() bool {
	return m.isDirty == false
}
