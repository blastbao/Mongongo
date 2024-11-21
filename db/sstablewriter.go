// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"log"
	"os"
	"strings"

	"github.com/DistAlchemist/Mongongo/utils"
)

// SSTableWriter ...
type SSTableWriter struct {
	*SSTable
	dataFile  *os.File
	indexFile *os.File
}

// NewSSTableWriter ...
func NewSSTableWriter(filename string, keyCount int) *SSTableWriter {
	s := &SSTableWriter{}
	s.SSTable = NewSSTable(filename)
	var err error

	// 打开数据文件
	s.dataFile, err = os.OpenFile(s.dataFileName, os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	// 打开索引文件
	s.indexFile, err = os.OpenFile(s.indexFilename(s.dataFileName), os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	// 初始化布隆过滤器（Bloom Filter），并设置初始容量为 keyCount，使用 15 个哈希函数
	s.bf = utils.NewBloomFilter(keyCount, 15)
	return s
}

func compare(s1, s2 string) bool {
	// currently I only use direct compare,
	// which corresponds to random partition strategy
	return s1 < s2
}

func (s *SSTableWriter) beforeAppend(decoratedKey string) int64 {
	// 检查键是否为空
	if decoratedKey == "" {
		log.Fatal("key must not be empty")
	}
	// 确保键按顺序写入，SSTable 中的键始终是有序的。
	if s.lastWrittenKey != "" && compare(s.lastWrittenKey, decoratedKey) == false {
		log.Printf("Last written key: %v\n", s.lastWrittenKey)
		log.Printf("Current key: %v\n", decoratedKey)
		log.Printf("Writing into file %v\n", s.dataFileName)
		log.Fatal("keys must be written in ascending order")
	}
	// 返回当前写入位置
	//	- 如果是第一个键，返回 0，表示文件的开始位置。
	//	- 如果不是第一个键，
	if s.lastWrittenKey == "" {
		return 0
	}
	return getCurrentPos(s.dataFile)
}

func (s *SSTableWriter) afterAppend(decoratedKey string, position int64) {
	// 更新布隆过滤器
	s.bf.Fill(decoratedKey)
	// 更新 lastWrittenKey
	s.lastWrittenKey = decoratedKey
	// 获取当前索引文件的偏移
	indexPosition := getCurrentPos(s.indexFile)
	// 更新索引文件 <key, data_file_offset>
	writeString(s.indexFile, decoratedKey)
	writeInt64(s.indexFile, position)

	// 控制每隔 SSTIndexInterval 个键写入一次索引
	if s.indexKeysWritten%SSTIndexInterval != 0 {
		s.indexKeysWritten++
		return // 若没有达到间隔条件，则返回，不进行索引写入
	}
	s.indexKeysWritten++
	if s.indexPositions == nil {
		s.indexPositions = make([]*KeyPositionInfo, 0)
	}
	// 将键的位置记录到索引中，<key, index_file_offset>
	s.indexPositions = append(s.indexPositions, NewKeyPositionInfo(decoratedKey, indexPosition))
}

// 将 <key, val(buf)> 写入到 SSTable 中。
func (s *SSTableWriter) append(decoratedKey string, buf []byte) {
	currentPos := s.beforeAppend(decoratedKey) // 检查 key 是否按序写入
	writeString(s.dataFile, decoratedKey)      // 写入 len(key) + key
	writeBytes(s.dataFile, buf)                // 写入 len(buf) + buf
	s.afterAppend(decoratedKey, currentPos)    // 更新索引
}

func (s *SSTableWriter) closeAndOpenReader() *SSTableReader {
	// renames temp SSTable files to valid data, index and bloom filter files
	// bloom filter file
	fos, err := os.OpenFile(s.filterFilename(s.dataFileName), os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	utils.BFSerializer.Serialize(s.bf, fos)
	fos.Sync()
	fos.Close()

	// index file
	s.indexFile.Sync()
	s.indexFile.Close()

	// main data
	s.dataFile.Sync()
	s.dataFile.Close()

	s.rename(s.indexFilename(s.dataFileName))
	s.rename(s.filterFilename(s.dataFileName))
	s.dataFileName = s.rename(s.dataFileName)

	return NewSSTableReaderI(s.dataFileName, s.indexPositions, s.bf)
}

// 重命名，去掉 "-tmp" 后缀
func (s *SSTableWriter) rename(tmpFilename string) string {
	filename := strings.Replace(tmpFilename, "-"+SSTableTmpFile, "", 1)
	os.Rename(tmpFilename, filename)
	return filename
}
