// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/DistAlchemist/Mongongo/utils"
)

var (
	// openedFiles for SSTableReader
	openedFiles = NewFileSSTableMap()
	srmu        sync.Mutex
)

// SSTableReader ...
type SSTableReader struct {
	*SSTable
}

// filename is the full path name with dir
func openSSTableReader(dataFilename string) *SSTableReader {
	// 从缓存中查找 SSTableReader ，避免重复构建
	sstable, ok := openedFiles.get(dataFilename)
	if !ok {
		// 创建一个新的 SSTableReader 实例
		sstable = NewSSTableReader(dataFilename)
		start := time.Now().UnixNano() / int64(time.Millisecond.Milliseconds())
		// 加载索引文件，保存了 key 的 data 存储在数据文件中的偏移，以加速查询。
		sstable.loadIndexFile()
		// 加载布隆过滤器，用于快速判断某个键是否存在
		sstable.loadBloomFilter()
		log.Printf("index load time for %v: %v ms.", dataFilename, time.Now().UnixNano()/int64(time.Millisecond)-start)
		// 将 SSTableReader 加入缓存
		openedFiles.put(dataFilename, sstable)
	}
	return sstable
}

func getSSTableReader(dataFileName string) *SSTableReader {
	srmu.Lock()
	defer srmu.Unlock()
	sstable, _ := openedFiles.get(dataFileName)
	return sstable
}

// NewSSTableReader ...
func NewSSTableReader(filename string) *SSTableReader {
	s := &SSTableReader{}
	s.SSTable = NewSSTable(filename)
	return s
}

// NewSSTableReaderI ...
func NewSSTableReaderI(filename string, indexPositions []*KeyPositionInfo, bf *utils.BloomFilter) *SSTableReader {
	s := &SSTableReader{}
	s.SSTable = NewSSTable(filename)
	s.bf = bf
	srmu.Lock()
	defer srmu.Unlock()
	openedFiles.put(filename, s)
	return s
}

func (s *SSTableReader) loadIndexFile() {
	/** Index file structure:
	 * decoratedKey (int32+string)
	 * index (int64)
	 * (repeat above two)
	 * */

	// 打开索引文件
	fd, err := os.Open(s.indexFilename(s.dataFileName))
	if err != nil {
		log.Fatal(err)
	}
	// 获取文件信息
	fileInfo, err := fd.Stat()
	if err != nil {
		log.Fatal(err)
	}
	// 获取文件大小（单位：字节）
	size := fileInfo.Size()
	i := 0
	for {
		// 获取当前文件指针位置
		pos := getCurrentPos(fd)
		// 读取到文件末尾，退出
		if pos == size {
			break
		}
		// 读取 Key
		decoratedKey, _ := readString(fd)
		// 读取 Key 的 Value 存储在数据文件的 Offset
		readInt64(fd)
		// 每读取到 `s.indexInterval` 个键，就将当前键和位置存入 `indexPositions` 列表
		if i%s.indexInterval == 0 {
			s.indexPositions = append(s.indexPositions, NewKeyPositionInfo(decoratedKey, pos))
		}
	}
}

func (s *SSTableReader) loadBloomFilter() {
	stream, err := os.Open(s.filterFilename(s.dataFileName))
	if err != nil {
		log.Fatal(err)
	}
	s.bf = utils.BFSerializer.Deserialize(stream)
}

func (s *SSTableReader) getFileStruct() *FileStruct {
	return NewFileStruct(s)
}

func (s *SSTableReader) getTableName() string {
	return s.parseTableName(s.dataFileName)
}

func (s *SSTableReader) makeColumnFamily() *ColumnFamily {
	return createColumnFamily(s.getTableName(), s.getColumnFamilyName())
}

func (s *SSTableReader) getIndexPositions() []*KeyPositionInfo {
	return s.indexPositions
}

func (s *SSTableReader) delete() {
	os.Remove(s.dataFileName)
	os.Remove(s.indexFilename(s.dataFileName))
	os.Remove(s.filterFilename(s.dataFileName))
	srmu.Lock()
	defer srmu.Unlock()
	openedFiles.remove(s.dataFileName)
}

// 假设索引如下:
//
// decoratedKey	position
//
//	"key1"	1000
//	"key3"	3000
//	"key5"	5000
//	"key7"	7000
//
// 查找时：
//
//	查找 "key3"：返回位置 3000 ，因为 "key3" 是第一个大于等于 "key3" 的索引键，且是 “等于” 意味着目标 key 就在这个索引区间中。
//	查找 "key2"：返回位置 1000 ，因为 "key3" 是第一个大于等于 "key2" 的索引键，且是 “大于” 意味着目标 key 在前一个索引区间中。
//	查找 "key8"：返回位置 7000 ，因为 "key8" 大于所有索引键，就返回最后一个索引键对应的偏移。
//	查找 "key0"：返回 -1 ，因为 "key0" 小于所有索引键，意味着不存在 key0 。
//
// 假设查找 key2 ，定位到 index offset = 1000 之后，从 1000 位置开始读取 IndexEntry<key, offset> ，如果匹配就返回 offset 否则 -1 。
func (s *SSTableReader) getIndexScanPosition(decoratedKey string) int64 {
	// get the position in the index file to start scanning
	// to find the given key (at most indexInterval keys away)
	if s.indexPositions == nil || len(s.indexPositions) == 0 {
		log.Fatal("indexPositions for sstable is empty!")
	}
	index := sort.Search(len(s.indexPositions), func(i int) bool {
		return s.partitioner.Compare(decoratedKey, s.indexPositions[i].key) <= 0
	})
	if index == len(s.indexPositions) {
		return s.indexPositions[index-1].position
	}
	if s.indexPositions[index].key != decoratedKey {
		if index == 0 {
			return -1
		}
		// binary search gives us the first index greater
		// than the key searched for, i.e. its insertion position
		return s.indexPositions[index-1].position
	}
	return s.indexPositions[index].position
}

func (s *SSTableReader) getPosition(decoratedKey string) int64 {
	// returns the position in the data file to
	// find the given key, or -1 if the key is not
	// present

	// 检查布隆过滤器，判断键是否存在，为 false 一定不存在
	if s.bf.IsPresent(decoratedKey) == false {
		return -1
	}

	// 获取索引文件的起始扫描位置
	start := s.getIndexScanPosition(decoratedKey)
	if start < 0 {
		return -1
	}

	// 打开索引文件
	input, err := os.Open(s.indexFilename(s.dataFileName))
	if err != nil {
		log.Fatal(err)
	}

	// 将文件指针移动到起始扫描位置
	input.Seek(start, 0)
	i := 0
	for {
		// 读取 indexed key
		indexDecoratedKey, _ := readString(input)
		// 读取 data store offset
		position := readInt64(input) // this is file position in Data file
		// 比较 key 和 indexed key
		v := s.partitioner.Compare(indexDecoratedKey, decoratedKey)

		// 如果匹配，返回对应的存储位置
		if v == 0 {
			return position
		}

		// 如果 indexed key 大于 key ，表示 key 不存在
		if v > 0 {
			break
		}

		i++
		// 如果扫描超过了一个索引区间，表示 key 不存在于当前区间，意味着 key 不存在
		if i >= SSTIndexInterval {
			break
		}
	}
	input.Close()
	return -1
}

// FileSSTableMap ...
type FileSSTableMap struct {
	m map[string]*SSTableReader
}

// NewFileSSTableMap ...
func NewFileSSTableMap() *FileSSTableMap {
	f := &FileSSTableMap{}
	f.m = make(map[string]*SSTableReader)
	return f
}

// Caution: the key is always full filename with dir
func (f *FileSSTableMap) get(filename string) (*SSTableReader, bool) {
	res, ok := f.m[filename]
	return res, ok
}

// Caution: the key is always full filename with dir
func (f *FileSSTableMap) put(filename string, value *SSTableReader) {
	f.m[filename] = value
}

func (f *FileSSTableMap) values() []*SSTableReader {
	res := make([]*SSTableReader, len(f.m))
	for _, value := range f.m {
		res = append(res, value)
	}
	return res
}

func (f *FileSSTableMap) clear() {
	f.m = make(map[string]*SSTableReader)
}

func (f *FileSSTableMap) remove(filename string) {
	delete(f.m, filename)
}
