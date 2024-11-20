// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import "sync"

// BinaryMemtable 作为一个内存存储结构，，通常用于暂存写入的数据，等待进一步的持久化。

// BinaryMemtable is the binary version of memtable
type BinaryMemtable struct {
	threshold      int // 当内存表的大小超过该阈值时，isFrozen 将内存表冻结，意味着无法再添加新的数据到该内存表，可能会触发数据持久化操作。
	currentSize    int32
	tableName      string
	cfName         string
	isFrozen       bool
	columnFamilies map[string][]byte // 存储了不同列族的数据，数据以二进制形式存储，可以进一步优化内存使用和磁盘 I/O
	mu             sync.Mutex
	cond           *sync.Cond
}

// NewBinaryMemtable initializes a BinaryMemtable
func NewBinaryMemtable(table, cfName string) *BinaryMemtable {
	b := &BinaryMemtable{}
	b.threshold = 512 * 1024 * 1024            // 默认阈值：512MB
	b.currentSize = 0                          // 初始大小为 0
	b.tableName = table                        // 表名
	b.cfName = cfName                          // 列族名
	b.isFrozen = false                         // 默认不冻结
	b.columnFamilies = make(map[string][]byte) // 初始化列族数据存储
	b.cond = sync.NewCond(&b.mu)               // 创建条件变量
	return b
}
