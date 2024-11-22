// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"os"

	"github.com/DistAlchemist/Mongongo/config"
)

var tableMetadata *TableMetadata

// TableMetadata stores infos about table and its columnFamilies
type TableMetadata struct {
	cfIDs   map[string]int    // 列族名 => 列族 ID
	cfTypes map[string]string // 列族名 => 列族类型
}

// NewTableMetadata initializes a TableMetadata
func NewTableMetadata() *TableMetadata {
	t := &TableMetadata{}
	t.cfIDs = make(map[string]int)
	t.cfTypes = make(map[string]string)
	return t
}

func (t *TableMetadata) isEmpty() bool {
	return t.cfIDs == nil
}

// Add adds column family, id and typename to table metadata
func (t *TableMetadata) Add(cf string, id int, tp string) {
	t.cfIDs[cf] = id   // 列族名 => 列族 ID
	t.cfTypes[cf] = tp // 列族名 => 列族类型
	idCFMap[id] = cf   // 列族 ID => 列族名
}

func getFileName() string {
	table := config.Tables[0]
	return config.MetadataDir + string(os.PathSeparator) + table + "-Metadata.db"
}

func (t *TableMetadata) isValidColumnFamily(cfName string) bool {
	_, ok := t.cfIDs[cfName]
	return ok
}

func (t *TableMetadata) getSize() int {
	return len(t.cfIDs)
}

func (t *TableMetadata) getColumnFamilyID(cfName string) int {
	return t.cfIDs[cfName]
}
