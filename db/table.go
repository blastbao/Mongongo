// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
)

var (
	tableInstances   = map[string]*Table{}
	tableMetadataMap = map[string]*TableMetadata{}
	idCFMap          = map[int]string{} // 列族 ID => 列族名
	tCreateLock      sync.Mutex
)

// Table ...
type Table struct {
	tableName          string
	tableMetadata      *TableMetadata
	columnFamilyStores map[string]*ColumnFamilyStore
}

// OpenTable ...
//
// 通过表名获取一个 Table 实例。如果这个表还没有创建，会通过 NewTable 创建一个新的表实例并缓存。
func OpenTable(table string) *Table {
	tableInstance, ok := tableInstances[table]
	if !ok {
		// read config to know the column families for this table.
		tCreateLock.Lock()
		defer tCreateLock.Unlock()
		tableInstance = NewTable(table)
		tableInstances[table] = tableInstance
	}
	return tableInstance
}

func getColumnFamilyCount() int {
	return len(idCFMap)
}

// NewTable create a Table
func NewTable(tableName string) *Table {
	t := &Table{}
	t.tableName = tableName
	t.tableMetadata = getTableMetadataInstance(t.tableName) // 获取该表的元数据（包括列族信息）。
	t.columnFamilyStores = make(map[string]*ColumnFamilyStore)
	cfIDMap := t.tableMetadata.cfIDs
	for columnFamily := range cfIDMap {
		// 为每个列族创建一个 ColumnFamilyStore 实例，负责列族的数据管理。
		t.columnFamilyStores[columnFamily] = NewColumnFamilyStore(tableName, columnFamily)
	}
	return t
}

func (t *Table) get(key string) *Row {
	// 创建一个行对象
	row := NewRowT(t.tableName, key)
	// 遍历表中的所有列族
	for columnFamily := range t.getColumnFamilies() {
		// 获取列族数据
		cf := t.getCF(key, columnFamily)
		if cf != nil {
			// 将该列族的数据添加到当前行中
			row.addColumnFamily(cf)
		}
	}
	// 返回构建好的行数据
	return row
}

func (t *Table) getCF(key, cfName string) *ColumnFamily {
	cfStore, ok := t.columnFamilyStores[cfName]
	if ok == false {
		log.Fatal("Column family" + cfName + " has not been defined")
	}
	return cfStore.getColumnFamily(NewIdentityQueryFilter(key, NewQueryPathCF(cfName)))
}

func (t *Table) getColumnFamilies() map[string]int {
	return t.tableMetadata.cfIDs
}

func (t *Table) getColumnFamilyStore(cfName string) *ColumnFamilyStore {
	return t.columnFamilyStores[cfName]
}

func getTableMetadataInstance(tableName string) *TableMetadata {
	tableMetadata, ok := tableMetadataMap[tableName]
	if !ok {
		tableMetadata = NewTableMetadata()
		tableMetadataMap[tableName] = tableMetadata
	}
	return tableMetadata
}

func (t *Table) loadTableMetadata(fileName string) {
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(f)
	sizeStr, err := reader.ReadString(' ')
	if err != nil {
		log.Fatal(err)
	}
	size, err := strconv.Atoi(sizeStr)
	if err != nil {
		log.Fatal(err)
	}
	tmetadata := NewTableMetadata()
	for i := 0; i < size; i++ {
		cfName, err := reader.ReadString(' ')
		if err != nil {
			log.Fatal(err)
		}
		idStr, err := reader.ReadString(' ')
		if err != nil {
			log.Fatal(err)
		}
		id, err := strconv.Atoi(idStr)
		if err != nil {
			log.Fatal(err)
		}
		typeName, err := reader.ReadString(' ')
		if err != nil {
			log.Fatal(err)
		}
		tmetadata.Add(cfName, id, typeName)
	}
	t.tableMetadata = tmetadata
}

func (t *Table) onStart() {
	cfs := t.tableMetadata.cfIDs
	for cf := range cfs {
		cfStore := t.columnFamilyStores[cf]
		cfStore.onStart()
	}
}

func (t *Table) isValidColumnFamily(columnFamily string) bool {
	return t.tableMetadata.isValidColumnFamily(columnFamily)
}

func (t *Table) getNumberOfColumnFamilies() int {
	return t.tableMetadata.getSize()
}

// First adds the row to the commit log associated with this
// table. Then the data associated with the individual column
// families is also written to the column family store's memtable
//
// [重要]
func (t *Table) apply(row *Row) {
	key := row.Key
	start := time.Now().UnixNano() / int64(time.Millisecond)
	spew.Printf("table: %v \n -- table: %+v\n", t, t)
	log.Printf("size: %v\n", t.tableMetadata.getSize())
	// 先将行数据写入到提交日志
	cmtLogCtx := openCommitLogE().add(row)
	// 然后将数据写入到对应的列族存储
	for cfName, columnFamily := range row.ColumnFamilies {
		cfStore := t.columnFamilyStores[cfName]
		cfStore.apply(key, columnFamily, cmtLogCtx)
	}
	timeTaken := time.Now().UnixNano()/int64(time.Millisecond) - start
	log.Printf("table.apply(row) took %v ms\n", timeTaken)
}

func (t *Table) getColumnFamilyID(cfName string) int {
	return t.tableMetadata.getColumnFamilyID(cfName)
}

// 根据查询过滤器（QueryFilter）获取指定的 Row 。
func (t *Table) getRow(filter QueryFilter) *Row {
	// 获取列族存储
	cfStore := t.columnFamilyStores[filter.getPath().ColumnFamilyName]
	// 获取列族数据
	columnFamily := cfStore.getColumnFamily(filter)
	// 创建行，用于保存 table/key 相关的数据
	row := NewRowT(t.tableName, filter.getKey())
	spew.Printf("\tcfStore: %#+v\n\n", cfStore)
	spew.Printf("\trow: %#+v\n\n", row)
	spew.Printf("\tcf: %#+v\n\n", columnFamily)
	// 将列族添加到行中
	if columnFamily != nil {
		row.addColumnFamily(columnFamily)
	}
	// 返回行
	return row
}
