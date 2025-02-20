// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"encoding/binary"
	"log"
	"strings"

	"github.com/DistAlchemist/Mongongo/config"
	"github.com/DistAlchemist/Mongongo/utils"
)

// HINT string
var (
	HINT     = "HINT"
	DoREPAIR = "READ-REPAIR"
)

// RowMutation definition
// RowMutation 是一种在表中进行数据修改（插入、更新、删除）的操作。
type RowMutation struct {
	TableName    string                   // 表
	RowKey       string                   // 键
	Modification map[string]*ColumnFamily // 存储每个列族的修改（列族名 -> 列族对象）
}

// NewRowMutation creates a new row mutation
func NewRowMutation(tableName, rowKey string) RowMutation {
	rm := RowMutation{}
	rm.TableName = tableName
	rm.RowKey = rowKey
	rm.Modification = make(map[string]*ColumnFamily)
	return rm
}

// NewRowMutationR init it with given row
func NewRowMutationR(tableName string, row *Row) *RowMutation {
	rm := &RowMutation{}
	rm.TableName = tableName
	rm.RowKey = row.Key
	rm.Modification = make(map[string]*ColumnFamily)
	for _, cf := range row.ColumnFamilies {
		rm.AddCF(cf)
	}
	return rm
}

// AddHints ...
func (rm *RowMutation) AddHints(key, host string) {
	path := NewQueryPath(config.HintsCF, []byte(key), []byte(host))
	rm.AddQ(path, nil, utils.CurrentTimeMillis())
}

// AddQ ...
//
// 把统一列族的修改聚合存储，以便后续一起更新
func (rm *RowMutation) AddQ(path *QueryPath, value []byte, timestamp int64) {
	columnFamily := rm.Modification[path.ColumnFamilyName]
	if columnFamily == nil {
		columnFamily = createColumnFamily(rm.TableName, path.ColumnFamilyName)
	}
	columnFamily.addColumnQP(path, string(value), timestamp, false)
	rm.Modification[path.ColumnFamilyName] = columnFamily
}

// AddCF adds column family to modification
func (rm *RowMutation) AddCF(columnFamily *ColumnFamily) {
	// 列族名 => 列族对象
	rm.Modification[columnFamily.ColumnFamilyName] = columnFamily
}

// Add store columnFamilyName and columnName inside rowMutation
func (rm *RowMutation) Add(columnFamilyColumn, value string, timestamp int64) {
	cfColumn := strings.Split(columnFamilyColumn, ":")
	sz := len(cfColumn)
	if sz == 0 || sz == 1 || sz > 3 {
		log.Printf("Invalid format: %v. Must be <column family>:<column>\n", cfColumn)
	}
	columnFamilyName := cfColumn[0]
	var columnFamily *ColumnFamily
	if sz == 2 {
		columnName := cfColumn[1]
		columnFamily = NewColumnFamily(columnFamilyName, "Standard")
		columnFamily.CreateColumn(columnName, value, timestamp)
	} else if sz == 3 {
		columnName := cfColumn[1] + ":" + cfColumn[2]
		columnFamily = NewColumnFamily(columnFamilyName, "Super")
		columnFamily.CreateColumn(columnName, value, timestamp)

	}
	if rm.Modification == nil {
		rm.Modification = make(map[string]*ColumnFamily)
	}
	rm.Modification[columnFamilyName] = columnFamily
}

// Apply is equivalent to calling commit. This will
// applies the changes to the table that is obtained
// by calling Table.open()
func (rm *RowMutation) Apply(row *Row) {
	table := OpenTable(rm.TableName)
	for cfName := range rm.Modification {
		if !table.isValidColumnFamily(cfName) {
			log.Printf("Column Family %v has not been defined.", cfName)
		} else {
			row.addColumnFamily(rm.Modification[cfName])
		}
	}
	table.apply(row)
}

// ApplyE receives empty argument
func (rm *RowMutation) ApplyE() {
	row := NewRowT(rm.TableName, rm.RowKey)
	rm.Apply(row)
}

// Delete ...
func (rm *RowMutation) Delete(path *QueryPath, timestamp int64) {
	cfName := path.ColumnFamilyName
	_, ok := rm.Modification[cfName]
	if ok {
		log.Fatal("ColumnFamily " + cfName + " is already being modified")
	}
	localDeleteTime := int(getCurrentTimeInMillis() / 1000)
	columnFamily := createColumnFamily(rm.TableName, cfName)
	if path.SuperColumnName == nil && path.ColumnName == nil {
		columnFamily.updateDeleteTime(localDeleteTime, timestamp)
	} else if path.ColumnName == nil {
		sc := NewSuperColumn(string(path.SuperColumnName))
		sc.markForDeleteAt(localDeleteTime, timestamp)
		columnFamily.addColumn(sc)
	} else {
		b4 := make([]byte, 4)
		binary.BigEndian.PutUint32(b4, uint32(localDeleteTime))
		columnFamily.addColumnQP(path, string(b4), timestamp, true)
	}
}
