// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"log"
	"strings"
)

// AColumnFactory -> Abstract Column Factory
// ColumnFactory and SuperColumnFactory are two specific impl.
//
// 创建不同类型的列（Column 和 SuperColumn）
type AColumnFactory interface {
	createColumn(name, value string, timestamp int64) IColumn
}

// ColumnFactory implements AColumnFactory
//
// 创建普通列
type ColumnFactory struct{}

func (f ColumnFactory) createColumn(name, value string, timestamp int64) IColumn {
	c := Column{}
	c.Name, c.Value, c.Timestamp = name, value, timestamp
	return c
}

// SuperColumnFactory implements AColumnFactory
//
// 创建超级列
type SuperColumnFactory struct{}

func (f SuperColumnFactory) createColumn(name, value string, timestamp int64) IColumn {
	columnNames := strings.Split(name, ":")
	if len(columnNames) != 2 {
		log.Printf("Invalid argument: should be <super column>:<column>\n")
	}
	superColumnName, columnName := columnNames[0], columnNames[1]
	superColumn := NewSuperColumn(superColumnName)
	subColumn := NewColumn(columnName, value, timestamp, false)
	superColumn.addColumn(subColumn)
	return superColumn
}
