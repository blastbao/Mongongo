// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

// QueryPath ...
type QueryPath struct {
	ColumnFamilyName string // 列族
	SuperColumnName  []byte // 超列
	ColumnName       []byte // 列
}

// NewQueryPath ...
func NewQueryPath(columnFamilyName string, superColumnName, columnName []byte) *QueryPath {
	q := &QueryPath{}
	q.ColumnFamilyName = columnFamilyName
	q.SuperColumnName = superColumnName
	q.ColumnName = columnName
	return q
}

// NewQueryPathCF ...
func NewQueryPathCF(columnFamilyName string) *QueryPath {
	return NewQueryPath(columnFamilyName, nil, nil)
}
