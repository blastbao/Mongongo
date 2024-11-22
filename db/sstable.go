// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/willf/bitset"

	"github.com/DistAlchemist/Mongongo/config"
	"github.com/DistAlchemist/Mongongo/dht"
	"github.com/DistAlchemist/Mongongo/utils"
)

/**
 */
var (
	// SSTableTmpFile is the tmp file name for sstable
	SSTableTmpFile = "tmp"
	SSTVersion     = int64(0)

	SSTIndexMetadataMap map[string][]*KeyPositionInfo

	// every 128th key is an index
	SSTIndexInterval = 128
	// key associated with block index written to disk
	SSTBlockIndexKey = "BLOCK-INDEX"
	// position in SSTable after the first Block Index
	SSTPositionAfterFirstBlockIndex = int64(0)

	// SSTbfs this map has the SSTable as key and a BloomFilter
	// as value. This BloomFilter will tell us if a key/
	// column pair is in the SSTable. If not, we can avoid
	// scanning it.
	//
	// 存储了每个 SSTable 文件对应的 Bloom Filter。
	SSTbfs = make(map[string]*utils.BloomFilter)

	// maintains a touched set of keys
	SSTTouchCache = NewTouchKeyCache(config.TouchKeyCacheSize)
	bfMarker      = "Bloom-Filter"
	SSTBlkIdxKey  = "BLOCK-INDEX"
)

// KeyPositionInfo contains index key and its corresponding
// position in the data file. Binary search is performed on
// a list of these objects to lookup keys within the SSTable
// data file.
//
// 存储 key 的索引信息(index_block)在 SSTable 中的位置，用于快速查找
//
// 由于 SSTable 文件的索引块是按键升序排列的，第一个键通常是该索引块中最大的键。
// 因此，记录下每个索引块中第一个键的索引块位置非常重要。
// 这样在后续的查找中，依据这个位置快速定位该键所在的索引块，并进一步查找该键的数据位置。
//
// 换句话说，keyPositionInfos 的作用是建立每个索引块与其包含的最大键之间的关联，便于后续查找时快速定位对应的索引块。
type KeyPositionInfo struct {
	key      string
	position int64
}

// NewKeyPositionInfo ...
func NewKeyPositionInfo(key string, position int64) *KeyPositionInfo {
	k := &KeyPositionInfo{}
	k.key = key
	k.position = position
	return k
}

// SSTable is the struct for SSTable
type SSTable struct {
	dataFileName       string                      // 数据文件名
	columnFamilyName   string                      // 列族名，一个 sstable 只会存储一个列族
	bf                 *utils.BloomFilter          // key bf ，用于判断 key 是否不存在
	dataWriter         *os.File                    // 貌似没用到
	blockIndex         map[string]*BlockMetadata   // 貌似没用到
	blockIndexes       []map[string]*BlockMetadata // 貌似没用到
	indexPositions     []*KeyPositionInfo          // key index ，用于定位 key 的数据存储位置
	lastWrittenKey     string                      // 确保有序写入
	indexKeysWritten   int                         // 已写入的 key 数量
	indexInterval      int                         // 每隔 indexInterval 个 key 保存一个索引项到内存中
	firstBlockPosition int64                       // 貌似没用到
	partitioner        dht.IPartitioner            // 分区器
}

// NewSSTable initializes a SSTable
func NewSSTable(filename string) *SSTable {
	s := &SSTable{}
	s.indexKeysWritten = 0
	s.lastWrittenKey = ""
	s.indexInterval = 128
	s.dataFileName = filename // ilename is the full path filename: var/storage/data/tableName/<columnFamilyName>-<index>-Data.db
	s.columnFamilyName = getColumnFamilyFromFullPath(filename)
	if config.HashingStrategy == config.Random {
		s.partitioner = dht.NewRandomPartitioner()
	} else {
		s.partitioner = dht.NewOPP()
	}
	return s
}

func (s *SSTable) getFilename() string {
	return s.dataFileName
}

func (s *SSTable) indexFilename(dataFile string) string {
	// input: /var/storage/data/tableName/<cf>-<index>-Data.db
	// output:/var/storage/data/tableName/<cf>-<index>-Index.db
	parts := strings.Split(dataFile, "-")
	parts[len(parts)-1] = "Index.db"
	return strings.Join(parts, "-")
}

func (s *SSTable) filterFilename(dataFile string) string {
	// input: /var/storage/data/tableName/<cf>-<index>-Data.db
	// output:/var/storage/data/tableName/<cf>-<index>-Filter.db
	parts := strings.Split(dataFile, "-")
	parts[len(parts)-1] = "Filter.db"
	return strings.Join(parts, "-")
}

func (s *SSTable) parseTableName(filename string) string {
	// filename is of format:
	// /var/storage/data/tableName/<cf>-<index>-Data.db
	parts := strings.Split(filename, string(os.PathSeparator))
	return parts[len(parts)-2]
}

func (s *SSTable) getColumnFamilyName() string {
	return s.columnFamilyName
}

func getColumnFamilyFromFullPath(filename string) string {
	// filename of the type:
	//  var/storage/data/tableName/<columnFamilyName>-<index>-Data.db
	values := strings.Split(filename, string(os.PathSeparator))
	cfName := strings.Split(values[len(values)-1], "-")[0]
	return cfName
}

func (s *SSTable) loadIndexFile() {
	// 打开文件，获取文件大小
	file, err := os.Open(s.dataFileName) // filename: var/storage/data/tableName/<columnFamilyName>-<index>-Data.db
	if err != nil {
		log.Fatal(err)
	}
	fileInfo, err := file.Stat()
	size := fileInfo.Size()

	// 加载 Bloom Filter
	s.loadBloomFilter(file, size)

	// 文件末尾的第 16~23 的 8 个字节处存储着第一个索引块的位置（相对位置）
	file.Seek(size-16, 0)
	firstBlockIndexPosition := readInt64(file)
	// 定位到第一个索引块
	nextPosition := size - 16 - firstBlockIndexPosition
	file.Seek(nextPosition, 0)

	// the structure of an index block is as follows:
	//  * key(string) -> block key "BLOCK-INDEX"
	//  * blockIndexSize int32: block index size
	//  * numKeys int32: # of keys in the block
	//  for i in range numKeys:
	//    * keySize int32: lengh of keyInBlock, work around..
	//    * keyInBlock string (if i==0, this is the largest key)
	//    * keyOffset int64: relative offset in the block
	//    * dataSize int64: size of data for that key
	// The goal is to obtain KeyPositionInfo:
	//    pair: (largestKeyInBlock, indexBlockPosition)
	// The code below is really an ugly workaround....

	// 初始化索引信息表
	keyPositionInfos := make([]*KeyPositionInfo, 0)
	SSTIndexMetadataMap[s.dataFileName] = keyPositionInfos
	var currentBlockPosition int64
	// 循环读取每个索引块
	for {
		// 当前索引块的存储位置
		currentBlockPosition = nextPosition
		// 读取 11B 的块标识符 "BLOCK-INDEX" 并校验
		b11 := make([]byte, 11)
		blockIdxKey := readBlockIdxKey(file, b11)
		if blockIdxKey != SSTBlkIdxKey {
			// [重要] 退出条件：如果读取的块标识符与预期的不匹配，则跳出循环，表示索引的读取已完成。
			log.Printf("Done reading the block indexes\n")
			break
		}
		nextPosition -= 11
		// 读取索引块的大小(Bytes)，没用到
		readInt32(file)
		nextPosition -= 4
		// 读取索引块的键数，后面会循环读取所有键
		numKeys := readInt32(file)
		nextPosition -= 4
		// 读取索引块中的每个键：len(key)[4B] + key + data offset[8B] + data size[8B]
		for i := int32(0); i < numKeys; i++ {
			keyInBlock, size := readString(file)
			nextPosition -= size
			readInt64(file)
			readInt64(file)
			nextPosition -= 16
			// 如果是当前索引块中的第一个键（按照倒序排列，第一个键是最大键），将 <key, curr_index_block_offset> 存入索引。
			if i == 0 {
				keyPositionInfos = append(keyPositionInfos, &KeyPositionInfo{keyInBlock, currentBlockPosition})
			}
		}
	}
	// should also sort KeyPositionInfos, but I omit it. :)
}

func readBool(file *os.File) (bool, int) {
	b := make([]byte, 1)
	file.Read(b)
	if b[0] == 1 {
		return true, 1
	}
	return false, 1
}

func readBytes(file *os.File) ([]byte, int) {
	// 字节数组长度，4B
	size := readInt(file)
	// 读取 size 长度的 data ，返回总长度
	b := make([]byte, size)
	file.Read(b)
	return b, size + 4
}

func readString(file *os.File) (string, int64) {
	// 字符串长度，4B
	size := int(readInt32(file))
	bs := make([]byte, size)
	// 读取 size 长度的 data ，返回总长度
	return readBlockIdxKey(file, bs), int64(size + 4)
}

// 从 file 中读取指定大小 len(b) 的字节数据，并以字符串格式返回
func readBlockIdxKey(file *os.File, b []byte) string {
	n, err := file.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	if n != len(b) {
		log.Fatal("should read len(b) byte for block index key")
	}
	return string(b)
}

func readInt64(file *os.File) int64 {
	b8 := make([]byte, 8)
	n, err := file.Read(b8)
	if err != nil {
		log.Fatal(err)
	}
	if n != 8 {
		log.Fatal("should read 8 bytes")
	}
	return int64(binary.BigEndian.Uint64(b8))
}

func readInt32(file *os.File) int32 {
	b4 := make([]byte, 4)
	n, err := file.Read(b4)
	if err != nil {
		log.Fatal(err)
	}
	if n != 4 {
		log.Fatal("should read 4 bytes")
	}
	return int32(binary.BigEndian.Uint32(b4))
}

func readUint64(file *os.File) uint64 {
	b8 := make([]byte, 8)
	n, err := file.Read(b8)
	if err != nil {
		log.Fatal(err)
	}
	if n != 8 {
		log.Fatal("should read 8 bytes")
	}
	return binary.BigEndian.Uint64(b8)
}

func (s *SSTable) loadBloomFilter(file *os.File, size int64) {
	// 检查 Bloom Filter 是否已存在
	if _, ok := SSTbfs[s.dataFileName]; ok {
		return
	}

	// 从文件的最后 8 字节读取一个 int64 值，该值表示 Bloom Filter 相对于文件末尾的偏移量。
	// 通过这个偏移量可以定位到 Bloom Filter 的开始位置。
	file.Seek(size-8, 0)
	b8 := make([]byte, 8)
	_, err := file.Read(b8)
	if err != nil {
		log.Fatal(err)
	}
	position := int64(binary.BigEndian.Uint64(b8))
	// 定位到 Bloom Filter 的位置
	file.Seek(size-8-position, 0)

	// 读取&解析 Bloom Filter
	// 1. 总大小(Byte)
	n, err := file.Read(b8)
	if err != nil {
		log.Fatal(err)
	}
	if n != 8 {
		log.Fatal("should read 8 bytes")
	}
	// don't need this variable
	// totalDataSize := int64(binary.BigEndian.Uint64(b8))
	// 2. 存储的元素数量
	count := readInt32(file)
	// read hashes: the number of hash functions
	// 3. 哈希函数的数量
	hashes := readInt32(file)
	// read size: the number of bits of BitSet
	// 4. BitSet 的位数
	bitsize := readInt32(file)
	// convert to number of uint64
	// 5. 读取位图，底层存储是 []uint64
	num8byte := (bitsize-1)/64 + 1 // 根据 Bloom Filter 的位图大小 bitsize 计算需要多少个 uint64 值来表示整个位图
	buf := make([]uint64, num8byte)
	for i := int32(0); i < num8byte; i++ {
		buf = append(buf, readUint64(file))
	}
	bs := bitset.From(buf) // 将读取到的 uint64 数组转化为 bitset 对象
	// 6. 构建 Bloom Filter ，缓存到 SSTbfs 中
	SSTbfs[s.dataFileName] = utils.NewBloomFilterS(count, hashes, bitsize, bs)
}

func onSSTableStart(filenames []string) {
	for _, filename := range filenames {
		ssTable := NewSSTable(filename)
		ssTable.close()
	}
}

func (s *SSTable) close() {
	s.closeByte(make([]byte, 0), 0)
}

// TouchKeyCache implements LRU cache
//
// LRU 缓存，缓存访问过的键
type TouchKeyCache struct {
	size int
}

// NewTouchKeyCache initializes a cache with given size
func NewTouchKeyCache(size int) *TouchKeyCache {
	t := &TouchKeyCache{}
	t.size = size
	return t
}

// NewSSTableP is used for DB writes into the SSTable
// Use this version to write to the SSTable
func NewSSTableP(directory, filename, pType string) *SSTable {
	s := &SSTable{}
	// 数据文件名
	s.dataFileName = directory + string(os.PathSeparator) + filename + "-Data.db"
	var err error
	// 打开文件
	s.dataWriter, err = os.Create(s.dataFileName)
	if err != nil {
		log.Fatal(err)
	}
	SSTPositionAfterFirstBlockIndex = 0
	s.initBlockIndex(pType)
	s.blockIndexes = make([]map[string]*BlockMetadata, 0)
	return s
}

func (s *SSTable) initBlockIndex(pType string) {
	// TODO make ordered map
	switch pType {
	case config.Ophf:
		s.blockIndex = make(map[string]*BlockMetadata)
	default:
		s.blockIndex = make(map[string]*BlockMetadata)
	}
}

func (s *SSTable) beforeAppend(hash string) int64 {
	if hash == "" {
		log.Fatal("hash value shouldn't be empty")
	}
	if s.lastWrittenKey != "" {
		previousKey := s.lastWrittenKey
		if hash < previousKey {
			log.Printf("Last written key: %v\n", previousKey)
			log.Printf("Current key: %v\n", hash)
			log.Printf("Writing into file: %v\n", s.dataFileName)
			log.Fatal("Keys must be written in ascending order.")
		}
	}

	currentPos := SSTPositionAfterFirstBlockIndex
	if s.lastWrittenKey != "" {
		currentPos, err := s.dataWriter.Seek(0, 0)
		if err != nil {
			log.Fatal(err)
		}
		s.dataWriter.Seek(currentPos, 0)
	}
	return currentPos
}

func (s *SSTable) afterAppend(hash string, position, size int64) {
	// 更新已写入的索引键数
	s.indexKeysWritten++
	// 将当前键记录为最后写入的键
	key := hash
	s.lastWrittenKey = key
	// 将当前的块索引信息加入到索引映射中
	s.blockIndex[key] = NewBlockMetadata(position, size)
	// 如果写入的键数达到了预定的间隔，则处理并保存索引块
	if s.indexKeysWritten == s.indexInterval {
		// 将当前块的索引信息添加到块索引列表中
		s.blockIndexes = append(s.blockIndexes, s.blockIndex)
		// 初始化新的块索引，这会清空当前的索引映射
		s.initBlockIndex(config.HashingStrategy)
		// 重置已写入键数
		s.indexKeysWritten = 0
	}
}

func (s *SSTable) append(key, hash string, buf []byte) {
	currentPos := s.beforeAppend(hash)
	str := hash + ":" + key
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(len(str)))
	// write string length
	s.dataWriter.Write(b4)
	// write string bytes
	s.dataWriter.WriteString(str)
	binary.BigEndian.PutUint32(b4, uint32(len(buf)))
	// write byte slice lengh
	s.dataWriter.Write(b4)
	s.dataWriter.Write(buf)
	s.afterAppend(hash, currentPos, int64(len(buf)))
}

func (s *SSTable) closeBF(bf *utils.BloomFilter) {
	// any remnants in the blockIndex should be added to the dump
	s.blockIndexes = append(s.blockIndexes, s.blockIndex)
	s.dumpBlockIndexes()
	// serialize the bloom filter
	buf := bf.ToByteArray()
	s.closeByte(buf, len(buf))
}

func (s *SSTable) dumpBlockIndexes() {
	position, err := s.dataWriter.Seek(0, 0)
	if err != nil {
		log.Fatal(err)
	}
	s.dataWriter.Seek(position, 0)
	s.firstBlockPosition = position
	for _, block := range s.blockIndexes {
		s.dumpBlockIndex(block)
	}
}

// ByKey ...
type ByKey []string

// Len ...
func (p ByKey) Len() int {
	return len(p)
}

// Less ...
func (p ByKey) Less(i, j int) bool {
	return p[i] < p[j]
}

// Swap ...
func (p ByKey) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// 将一个索引块（blockIndex）写入到 SSTable 文件中，并记录相关的元数据。
func (s *SSTable) dumpBlockIndex(blockIndex map[string]*BlockMetadata) {
	// 如果 blockIndex 为空，直接返回，不做任何操作
	if len(blockIndex) == 0 {
		return
	}

	// record the position where we start writing the block index.
	// this will be used as the position of the lastWrittenKey in the block in the index file.
	position, err := s.dataWriter.Seek(0, 0)
	if err != nil {
		log.Fatal(err)
	}

	// 将所有的 key 按照字典顺序排序
	keys := make([]string, 0)
	for key := range blockIndex {
		keys = append(keys, key)
	}
	sort.Sort(ByKey(keys))

	// 写入索引块中 key 的数量
	buf := make([]byte, 0)
	b4 := make([]byte, 0)
	binary.BigEndian.PutUint32(b4, uint32(len(keys)))
	buf = append(buf, b4...)

	// 写入每个 key 相关的元数据
	b8 := make([]byte, 8)
	for _, key := range keys {
		// 写入 key 的长度
		binary.BigEndian.PutUint32(b4, uint32(len(key)))
		buf = append(buf, b4...)
		// 写入 key 的内容
		buf = append(buf, []byte(key)...)
		// 写入 key 相对于当前文件位置的偏移量
		blockMetadata := blockIndex[key]
		binary.BigEndian.PutUint64(b8, uint64(position-blockMetadata.position))
		buf = append(buf, b8...)
		// 写入该 key 对应的数据块大小
		binary.BigEndian.PutUint64(b8, uint64(blockMetadata.size))
		buf = append(buf, b8...)
	}

	// 将 buf 写入文件
	writeKV(s.dataWriter, SSTBlkIdxKey, buf) //data := len(key) + key + len(buf) + buf

	// 更新内存索引
	keyPositionInfos, ok := SSTIndexMetadataMap[s.dataFileName]
	if !ok {
		keyPositionInfos = make([]*KeyPositionInfo, 0)
		SSTIndexMetadataMap[s.dataFileName] = keyPositionInfos
	}
	keyPositionInfos = append(keyPositionInfos, NewKeyPositionInfo(keys[0], position))
}

// data := len(key) + key + len(buf) + buf
func writeKV(file *os.File, key string, buf []byte) {
	length := len(buf)
	// write key length int32
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(len(key)))
	file.Write(b4)
	// write key bytes
	file.Write([]byte(key))
	// write data length
	binary.BigEndian.PutUint32(b4, uint32(length))
	file.Write(b4)
	// write data bytes
	file.Write(buf)
	// flush writes
	file.Sync()
}

func writeFooter(file *os.File, footer []byte, size int) {
	// size if int32(marker length) + marker data + int32(data size) + data bytes
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(len(bfMarker)))
	// write marker size
	file.Write(b4)
	// write marker bytes
	file.Write([]byte(bfMarker))
	// write footer size
	binary.BigEndian.PutUint32(b4, uint32(size))
	file.Write(b4)
	// write footer bytes
	file.Write(footer)
}

// 完成 SSTable 文件写入后关闭。
//
// 主要工作：
//   - 写入 Bloom Filter。
//   - 写入版本号。
//   - 写入索引块的相对位置。
//   - 写入 Bloom Filter 的相对位置。
//   - 刷新并确保数据写入到磁盘。
func (s *SSTable) closeByte(footer []byte, size int) {
	// write the bloom filter for this SSTable
	// then write three int64:
	//  1. version
	//  2. a pointer to the last written block index
	//  3. position of the bloom filter
	if s.dataWriter == nil {
		return
	}

	// 获取当前写入位置，记录 Bloom Filter 的位置
	bloomFilterPosition, err := s.dataWriter.Seek(0, 0)
	if err != nil {
		log.Fatal(err)
	}
	s.dataWriter.Seek(bloomFilterPosition, 0)

	// 写入 footer 信息
	writeFooter(s.dataWriter, footer, size)
	// 写入版本信息
	b8 := make([]byte, 8)
	binary.BigEndian.PutUint64(b8, uint64(SSTVersion))
	s.dataWriter.Write(b8)
	// 写入第一个索引块的相对位置
	currentPos := getCurrentPos(s.dataWriter)
	blockPosition := currentPos - s.firstBlockPosition
	binary.BigEndian.PutUint64(b8, uint64(blockPosition))
	s.dataWriter.Write(b8)
	// 写入 Bloom Filter 的相对位置
	bloomFilterRelativePosition := getCurrentPos(s.dataWriter) - bloomFilterPosition
	binary.BigEndian.PutUint64(b8, uint64(bloomFilterRelativePosition))
	s.dataWriter.Write(b8)
	// 刷盘
	s.dataWriter.Sync()
}

// 获取当前文件指针的位置
func getCurrentPos(file *os.File) int64 {
	res, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Fatal(err) // 如果发生错误，程序会退出并打印错误信息
	}
	return res
}

// BlockMetadata ...
// 一个数据块的元数据，包括位置和大小。
type BlockMetadata struct {
	position int64
	size     int64
}

// NewBlockMetadata ...
func NewBlockMetadata(position, size int64) *BlockMetadata {
	b := &BlockMetadata{}
	b.position = position
	b.size = size
	return b
}
