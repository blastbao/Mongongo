// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"encoding/binary"
	"log"
	"math"
	"os"
	"sync/atomic"
)

// SuperColumn implements IColumn interface
//
// SuperColumn 由多个普通列（sub-columns）组成的，类似于一个“列族”或“列集合”，每个子列都有自己的名称、时间戳、值等属性。
type SuperColumn struct {
	Name              string             // 超级列名
	Columns           map[string]IColumn // 子列集合
	deleteMark        bool               // 标记该超级列是否被删除
	size              int32              // 跟踪所有子列的总大小
	Timestamp         int64              // 最后更新时间
	localDeletionTime int                // 本地删除时间
	markedForDeleteAt int64              // 删除时间戳
}

func (sc SuperColumn) markForDeleteAt(localDeletionTime int, timestamp int64) {
	sc.localDeletionTime = localDeletionTime
	sc.markedForDeleteAt = timestamp
}

func (sc SuperColumn) getLocalDeletionTime() int {
	return sc.localDeletionTime
}

func (sc SuperColumn) getMarkedForDeleteAt() int64 {
	return sc.markedForDeleteAt
}

func (sc SuperColumn) isMarkedForDelete() bool {
	return sc.deleteMark
}

// IsMarkedForDelete ...
func (sc SuperColumn) IsMarkedForDelete() bool {
	return sc.deleteMark
}

func (sc SuperColumn) getValue() []byte {
	log.Fatal("super column doesn't support getValue")
	return []byte{}
}

// GetValue ...
// 超级列不能直接存储值，getValue 会触发 log.Fatal 。
func (sc SuperColumn) GetValue() []byte {
	log.Fatal("super column doesn't support getValue")
	return []byte{}
}

// 向超级列中添加一个子列。如果该子列已经存在且时间戳较新，则替换原子列。方法会根据子列的大小更新超级列的大小。
func (sc SuperColumn) addColumn(column IColumn) {
	name := column.getName()
	oldColumn, ok := sc.Columns[name]
	if !ok {
		sc.Columns[name] = column
		atomic.AddInt32(&sc.size, column.getSize())
	} else {
		if oldColumn.timestamp() <= column.timestamp() {
			sc.Columns[name] = column
			delta := int32(-1 * oldColumn.getSize())
			// subtruct the size of the oldColumn
			atomic.AddInt32(&sc.size, delta)
			atomic.AddInt32(&sc.size, int32(column.getSize()))
		}
	}
}

// NewSuperColumn constructs a SuperColumn
//
// 构造新的 SuperColumn 实例。
func NewSuperColumn(name string) SuperColumn {
	sc := SuperColumn{}
	sc.Name = name
	sc.Columns = make(map[string]IColumn)
	sc.deleteMark = false
	sc.size = 0
	sc.localDeletionTime = math.MinInt32
	sc.markedForDeleteAt = math.MinInt64
	return sc
}

// NewSuperColumnN constructs a SuperColumn
//
// 构造新的 SuperColumn 实例。
func NewSuperColumnN(name string, subcolumns map[string]IColumn) SuperColumn {
	sc := SuperColumn{}
	sc.Name = name
	sc.Columns = subcolumns
	sc.deleteMark = false
	sc.size = 0
	sc.localDeletionTime = math.MinInt32
	sc.markedForDeleteAt = math.MinInt64
	return sc
}

// 返回超级列的总大小，包含所有子列的大小。
func (sc SuperColumn) getSize() int32 {
	return sc.size
}

// 返回对象计数，包含超级列本身和所有子列。
func (sc SuperColumn) getObjectCount() int {
	return 1 + len(sc.Columns)
}

// 将超级列序列化为字节数组，包含列名、删除标记、子列数量、子列大小及每个子列的字节数据。
func (sc SuperColumn) toByteArray() []byte {
	buf := make([]byte, 0)
	// write supercolumn name length
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(len(sc.Name)))
	buf = append(buf, b4...)
	// write supercolumn name bytes
	buf = append(buf, []byte(sc.Name)...)
	// write deleteMark
	if sc.deleteMark {
		buf = append(buf, byte(1))
	} else {
		buf = append(buf, byte(0))
	}
	// write column size
	binary.BigEndian.PutUint32(b4, uint32(len(sc.Columns)))
	buf = append(buf, b4...)
	// write subcolumns total size, used to skip over
	// all these columns if we are not interested in
	// this super column
	binary.BigEndian.PutUint32(b4, sc.getSizeOfAllColumns())
	buf = append(buf, b4...)
	for _, column := range sc.Columns {
		buf = append(buf, column.toByteArray()...)
	}
	return buf
}

// 计算并返回所有子列的序列化大小。
func (sc SuperColumn) getSizeOfAllColumns() uint32 {
	res := uint32(0)
	for _, column := range sc.Columns {
		res += column.serializedSize()
	}
	return res
}

// 计算超级列序列化的总大小，包括列名长度、删除标记、子列数量、子列总大小等。
func (sc SuperColumn) serializedSize() uint32 {
	// 4 bytes: super column name length
	// # bytes: super column name bytes
	// 1 byte:  deleteMark
	// 4 bytes: number of sub-columns
	// 4 bytes: size of sub-columns
	// # bytes: size of all sub-columns
	return uint32(4+1+4+4+len(sc.Name)) + sc.getSizeOfAllColumns()
}

func (sc SuperColumn) timestamp() int64 {
	return sc.Timestamp
}

// GetTimestamp ...
func (sc SuperColumn) GetTimestamp() int64 {
	return sc.Timestamp
}

func (sc SuperColumn) getName() string {
	return sc.Name
}

// GetName ...
func (sc SuperColumn) GetName() string {
	return sc.Name
}

// Go through each subComlun. If it exists then resolve.
// Else create
//
// 向超级列中添加子列，如果子列已经存在并且时间戳较新，则进行替换，并更新删除标记和时间戳。
func (sc SuperColumn) putColumn(column IColumn) bool {
	_, ok := column.(SuperColumn)
	if !ok {
		log.Fatal("Only Super column objects should be put here")
	}
	if sc.Name != column.getName() {
		log.Fatal("The name should match the name of the current super column")
	}
	columns := column.GetSubColumns()
	for _, subColumn := range columns {
		sc.addColumn(subColumn)
	}
	if column.getMarkedForDeleteAt() > sc.markedForDeleteAt {
		sc.markForDeleteAt(column.getLocalDeletionTime(), column.getMarkedForDeleteAt())
	}
	return false
}

// 创建超级列的浅拷贝。只拷贝超级列的名称和删除相关的字段，不拷贝子列。
func (sc SuperColumn) cloneMeShallow() SuperColumn {
	s := NewSuperColumn(sc.Name)
	s.markForDeleteAt(sc.localDeletionTime, sc.markedForDeleteAt)
	return s
}

// 返回超级列中的所有子列。
func (sc SuperColumn) getSubColumns() map[string]IColumn {
	return sc.Columns
}

// GetSubColumns ...
func (sc SuperColumn) GetSubColumns() map[string]IColumn {
	return sc.Columns
}

// 遍历所有子列，返回子列中最近的更新时间戳。
func (sc SuperColumn) mostRecentChangeAt() int64 {
	res := int64(math.MinInt64)
	for _, column := range sc.Columns {
		if column.mostRecentChangeAt() > res {
			res = column.mostRecentChangeAt()
		}
	}
	return res
}

// Remove ...
//
// 从超级列中删除一个子列。
func (sc SuperColumn) Remove(name string) {
	delete(sc.Columns, name)
}

// SCSerializer ...
var SCSerializer = NewSuperColumnSerializer()

// SuperColumnSerializer ...
type SuperColumnSerializer struct {
	dummy int
}

// NewSuperColumnSerializer ...
func NewSuperColumnSerializer() *SuperColumnSerializer {
	return &SuperColumnSerializer{0}
}

// 将超级列及其所有子列序列化到文件中。
func (s *SuperColumnSerializer) serialize(column IColumn, dos *os.File) {
	superColumn := column.(SuperColumn)
	writeString(dos, column.getName())
	writeInt(dos, superColumn.getLocalDeletionTime())
	writeInt64(dos, superColumn.getMarkedForDeleteAt())
	columns := column.GetSubColumns()
	for _, subColumn := range columns {
		CSerializer.serialize(subColumn, dos)
	}
}

// 将超级列及其所有子列序列化到字节数组中。
func (s *SuperColumnSerializer) serializeB(column IColumn, dos []byte) {
	superColumn := column.(SuperColumn)
	writeStringB(dos, column.getName())
	writeIntB(dos, superColumn.getLocalDeletionTime())
	writeInt64B(dos, superColumn.getMarkedForDeleteAt())
	columns := column.GetSubColumns()
	for _, subColumn := range columns {
		CSerializer.serializeB(subColumn, dos)
	}
}

// 从文件中读取数据并反序列化为 SuperColumn 对象。
// 它读取列名、删除标记、时间戳等信息，并为每个子列调用 CSerializer.deserialize 来反序列化子列。
func (s *SuperColumnSerializer) deserialize(dis *os.File) IColumn {
	name, _ := readString(dis)
	superColumn := NewSuperColumn(name)
	localDeletionTime := readInt(dis)
	timestamp := readInt64(dis)
	superColumn.markForDeleteAt(localDeletionTime, timestamp)
	// read the number of columns
	size := readInt(dis)
	for i := 0; i < size; i++ {
		subColumn := CSerializer.deserialize(dis)
		superColumn.addColumn(subColumn)
	}
	return superColumn
}
