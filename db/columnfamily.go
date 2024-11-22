// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"encoding/binary"
	"sort"
	"sync/atomic"

	"github.com/DistAlchemist/Mongongo/config"
)

// ColumnFamily definition
type ColumnFamily struct {
	ColumnFamilyName  string             // 列族名
	ColumnType        string             // 列族类型
	Factory           AColumnFactory     // 列族工厂，负责创建列或超级列
	Columns           map[string]IColumn // 列族中的列
	size              int32              // 列族的总大小
	deleteMark        bool               // 列族是否被删除
	localDeletionTime int                // 本地删除时间
	markedForDeleteAt int64              // 删除时间
	columnSerializer  IColumnSerializer  // 序列化器，负责列数据的序列化和反序列化
}

var typeToColumnFactory = map[string]AColumnFactory{
	"Standard": ColumnFactory{},
	"Super":    SuperColumnFactory{},
}

// NewColumnFamily create a new column family, set columnfactory according to its type
//
// NewColumnFamily 用于创建新的列族对象。
//   - 根据列族类型，选择合适的列族工厂（ColumnFactory 或 SuperColumnFactory）。
//   - 根据列族类型，选择合适的列序列化器（ColumnSerializer 或 SuperColumnSerializer）。
func NewColumnFamily(columnFamilyName, columnType string) *ColumnFamily {
	cf := &ColumnFamily{}
	cf.ColumnFamilyName = columnFamilyName
	cf.ColumnType = columnType
	cf.Columns = make(map[string]IColumn)
	cf.Factory = typeToColumnFactory[columnType]
	cf.deleteMark = false
	if "Standard" == cf.ColumnType {
		cf.columnSerializer = NewColumnSerializer()
	} else {
		cf.columnSerializer = NewSuperColumnSerializer()
	}
	return cf
}

func createColumnFamily(tableName, cfName string) *ColumnFamily {
	columnType := config.GetColumnTypeTableName(tableName, cfName)
	return NewColumnFamily(cfName, columnType)
}

// CreateColumn setup a new column in columnFamily
func (cf *ColumnFamily) CreateColumn(columnName, value string, timestamp int64) {
	column := cf.Factory.createColumn(columnName, value, timestamp)
	cf.addColumn(column)
}

// If we find and old column that has the same name, then ask it to resolve itself, else we add the new column
func (cf *ColumnFamily) addColumn(col IColumn) {
	name := col.getName()
	// 检查列族是否已经有同名列
	oldColumn, ok := cf.Columns[name]
	if ok {
		// 如果是 SuperColumn 类型...
		_, yes := oldColumn.(SuperColumn)
		if yes { // is SuperColumn
			oldSize := oldColumn.getSize()
			oldColumn.putColumn(col)
			atomic.AddInt32(&cf.size, int32(oldColumn.getSize()-oldSize))
		} else {
			// 如果是普通列，根据优先级决定保留谁
			if oldColumn.(Column).comparePriority(col.(Column)) <= 0 {
				cf.Columns[name] = col
				atomic.AddInt32(&cf.size, col.getSize()-oldColumn.getSize())
			}
		}
	} else {
		// 没有直接保存
		atomic.AddInt32(&cf.size, col.getSize())
		cf.Columns[name] = col
	}
}

// with query path as argument
// in most places the cf must be part of a query path but
// here it is ignored.
func (cf *ColumnFamily) addColumnQP(path *QueryPath, value string, timestamp int64, deleted bool) {
	var column IColumn
	if path.SuperColumnName == nil {
		column = NewColumn(string(path.ColumnName), value, timestamp, deleted)
	} else {
		column = NewSuperColumn(string(path.SuperColumnName))
		column.addColumn(NewColumn(string(path.ColumnFamilyName), value, timestamp, deleted))
	}
	cf.addColumn(column)
}

func (cf *ColumnFamily) isSuper() bool {
	return cf.ColumnType == "Super"
}

// IsSuper ...
func (cf *ColumnFamily) IsSuper() bool {
	return cf.ColumnType == "Super"
}

// GetColumn ...
func (cf *ColumnFamily) GetColumn(name string) IColumn {
	return cf.Columns[name]
}

func (cf *ColumnFamily) getSize() int32 {
	if cf.size == 0 {
		for cfName := range cf.Columns {
			atomic.AddInt32(&cf.size, cf.Columns[cfName].getSize())
		}
	}
	return cf.size
}

func (cf *ColumnFamily) isMarkedForDelete() bool {
	return cf.deleteMark
}

// 序列化过程：
// - 列族名的长度和名称本身。
// - 删除标记（如果该列族被标记为删除，则该字段为 1，否则为 0）。
// - 列族中的列的数量。
// - 列族中的每一列的序列化数据。
func (cf *ColumnFamily) toByteArray() []byte {
	buf := make([]byte, 0)
	// write cf name length
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(len(cf.ColumnFamilyName)))
	buf = append(buf, b4...)
	// write cf name bytes
	buf = append(buf, []byte(cf.ColumnFamilyName)...)
	// write if this cf is marked for updateDeleteTime
	if cf.deleteMark {
		buf = append(buf, byte(1))
	} else {
		buf = append(buf, byte(0))
	}
	// write column size
	binary.BigEndian.PutUint32(b4, uint32(len(cf.Columns)))
	buf = append(buf, b4...)
	// write column bytes
	for _, column := range cf.Columns {
		buf = append(buf, column.toByteArray()...)
	}
	return buf
}

func (cf *ColumnFamily) getColumnCount() int {
	count := 0
	columns := cf.Columns
	if columns != nil {
		if config.GetColumnType(cf.ColumnFamilyName) != "Super" {
			count = len(columns)
		} else {
			for _, column := range columns {
				count += column.getObjectCount()
			}
		}
	}
	return count
}

// 把 newcf 中的 column 添加到 cf 中，如果有相同 column 根据优先级(timestamp)决定保留谁。
func (cf *ColumnFamily) addColumns(newcf *ColumnFamily) {
	for _, column := range newcf.Columns {
		cf.addColumn(column)
	}
}

func (cf *ColumnFamily) getMarkedForDeleteAt() int64 {
	return cf.markedForDeleteAt
}

func (cf *ColumnFamily) getLocalDeletionTime() int {
	return cf.localDeletionTime
}

func (cf *ColumnFamily) deleteCF(newcf *ColumnFamily) {
	// 取最新的删除时间
	t := cf.localDeletionTime
	if t < newcf.localDeletionTime {
		t = newcf.localDeletionTime
	}
	// 取最新的删除时间
	m := cf.getMarkedForDeleteAt()
	if m < newcf.getMarkedForDeleteAt() {
		m = newcf.getMarkedForDeleteAt()
	}
	// 更新删除时间
	cf.updateDeleteTime(t, m)
}

func (cf *ColumnFamily) remove(columnName string) {
	delete(cf.Columns, columnName)
}

// GetSortedColumns ...
func (cf *ColumnFamily) GetSortedColumns() []IColumn {
	// 对列名进行排序
	names := make([]string, 0)
	for name := range cf.Columns {
		names = append(names, name)
	}
	sort.Sort(ByKey(names))
	// 按照列名顺序排列 cf.columns 并返回
	res := make([]IColumn, 0)
	for _, name := range names {
		res = append(res, cf.Columns[name])
	}
	return res
}

func (cf *ColumnFamily) cloneMeShallow() *ColumnFamily {
	c := NewColumnFamily(cf.ColumnFamilyName, cf.ColumnType)
	c.markedForDeleteAt = cf.markedForDeleteAt
	c.localDeletionTime = cf.localDeletionTime
	return c
}

func (cf *ColumnFamily) clear() {
	cf.Columns = make(map[string]IColumn)
}

func (cf *ColumnFamily) updateDeleteTime(localtime int, timestamp int64) {
	cf.localDeletionTime = localtime
	cf.markedForDeleteAt = timestamp
}

func (cf *ColumnFamily) getColumnSerializer() IColumnSerializer {
	if cf.columnSerializer == nil {
		// hardcoded for now .. FIXME
		cf.columnSerializer = NewColumnSerializer()
	}
	return cf.columnSerializer
}
