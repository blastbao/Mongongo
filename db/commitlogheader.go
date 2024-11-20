// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"log"

	"github.com/willf/bitset"
)

// CommitLogHeader represents the header of commit log
type CommitLogHeader struct {
	// header        []byte
	// position      []int
	// 跟踪哪些列族在提交日志中被修改过。
	// 每个列族有一个对应的标志位（true 表示该列族被修改过），通过这个标志可以确定是否需要在恢复数据时重放该列族的日志。
	dirty *bitset.BitSet
	// 当列族的数据被刷新到磁盘时，记录其在提交日志中的偏移位置，这样可以在恢复时定位到需要恢复的位置。
	lastFlushedAt []int
}

// NewCommitLogHeader creates a new commit log header
// size is the number of column families
func NewCommitLogHeader(size int) *CommitLogHeader {
	c := &CommitLogHeader{}
	// c.header = make([]byte, size)
	// c.position = make([]int, size)
	c.dirty = bitset.New(uint(size))
	c.lastFlushedAt = make([]int, size)
	return c
}

// NewCommitLogHeaderD used in deserializing
func NewCommitLogHeaderD(dirty *bitset.BitSet, lastFlushedAt []int) *CommitLogHeader {
	c := &CommitLogHeader{}
	c.dirty = dirty
	c.lastFlushedAt = lastFlushedAt
	return c
}

// NewCommitLogHeaderC used in copy
func NewCommitLogHeaderC(clHeader *CommitLogHeader) *CommitLogHeader {
	c := &CommitLogHeader{}
	c.dirty = clHeader.dirty.Clone()
	c.lastFlushedAt = make([]int, len(clHeader.lastFlushedAt))
	for _, x := range clHeader.lastFlushedAt {
		c.lastFlushedAt = append(c.lastFlushedAt, x)
	}
	return c
}

// 返回指定列族最后一次被刷新到磁盘的位置
func (c *CommitLogHeader) getPosition(index int) int {
	return c.lastFlushedAt[index]
}

// 将指定列族的 dirty 标志位设置为 true，并记录该列族数据在日志中的位置（position）。
func (c *CommitLogHeader) turnOn(index int, position int64) {
	c.dirty.Set(uint(index))
	c.lastFlushedAt[index] = int(position)
}

// 将指定列族的 dirty 标志位清除为 false，并将该列族的最后刷新位置重置为 0。
func (c *CommitLogHeader) turnOff(index int) {
	c.dirty.Clear(uint(index))
	c.lastFlushedAt[index] = 0
}

// 检查指定列族是否有修改，判断该列族是否需要被恢复。
func (c *CommitLogHeader) isDirty(index int) bool {
	return c.dirty.Test(uint(index))
}

// 检查是否可以删除当前的提交日志。如果有任何列族的数据还没有刷新到磁盘（即 dirty 位仍为 true），则不能删除日志。
func (c *CommitLogHeader) isSafeToDelete() bool {
	return c.dirty.Any()
}

// 将所有列族的 dirty 标志位重置为 false，并清空 lastFlushedAt 数组。
func (c *CommitLogHeader) clear() {
	c.dirty.ClearAll()
	c.lastFlushedAt = make([]int, 0)
}

// 序列化为字节数组，方便将其存储到文件中。
func (c *CommitLogHeader) toByteArray() []byte {
	bos := make([]byte, 0)
	clhSerialize(c, bos)
	return bos
}

// 将 CommitLogHeader 的 dirty 位图和 lastFlushedAt 数组序列化为字节流，并写入到字节切片 dos 中。
// 它首先将 dirty 位图转换为字节数组，然后将数组的长度和内容写入。
func clhSerialize(clHeader *CommitLogHeader, dos []byte) {
	dbytes, err := clHeader.dirty.MarshalBinary()
	if err != nil {
		log.Fatal(err)
	}
	writeIntB(dos, len(dbytes))
	dos = append(dos, dbytes...)
	writeIntB(dos, len(clHeader.lastFlushedAt))
	for _, position := range clHeader.lastFlushedAt {
		writeIntB(dos, position)
	}
}

// func clhDeserialize(dis []byte) *CommitLogHeader {
// 	dirtyLen := readInt(dis)
// }
