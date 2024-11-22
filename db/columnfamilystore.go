// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

/*
Package db implements ...
*/
package db

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/DistAlchemist/Mongongo/config"
	"github.com/DistAlchemist/Mongongo/dht"
	"github.com/DistAlchemist/Mongongo/network"
	"github.com/DistAlchemist/Mongongo/utils"
)

var (
	memtablesPendingFlush = make(map[string][]*Memtable)
)

// ColumnFamilyStore provides storage specification of column family
type ColumnFamilyStore struct {
	threshold             int    // 内存表的阈值，超过此值会触发刷新。
	bufSize               int    // 缓冲区大小，用于数据压缩
	compactionMemoryThres int    // 压缩内存阈值，控制压缩操作的内存使用。
	tableName             string // 表的名称。
	columnFamilyName      string // 列族的名称。

	// to generate the next index for a SSTable
	fileIdxGenerator int32   // 用于生成下一个 SSTable 文件的索引。
	readStats        []int64 // 读操作的统计信息。
	writeStates      []int64 // 写操作的统计信息。

	// memtables associated with this cfStore
	memtable       *Memtable       // 当前使用的内存表。
	binaryMemtable *BinaryMemtable // 二进制格式的内存表。

	// SSTable on disk for this cf
	// ssTables map[string]bool
	ssTables map[string]*SSTableReader // 存储当前列族的数据文件(SSTable) 的读取器。

	// modification lock used for protecting reads
	// from compactions
	rwmu      sync.RWMutex // 读写锁，用于保护读操作免受压缩影响。
	memMu     sync.RWMutex // 内存表的读写锁。
	sstableMu sync.RWMutex // SSTable 的读写锁。

	// flag indicates if a compaction is in process
	isCompacting bool // 指示是否正在进行压缩操作。
	isSuper      bool // 指示是否为超级列族。
}

// NewColumnFamilyStore initializes a new ColumnFamilyStore
func NewColumnFamilyStore(table, columnFamily string) *ColumnFamilyStore {
	c := &ColumnFamilyStore{}
	c.threshold = 4
	c.bufSize = 128 * 1024 * 1024
	c.compactionMemoryThres = 1 << 30
	c.tableName = table
	c.columnFamilyName = columnFamily
	c.fileIdxGenerator = 0
	c.ssTables = make(map[string]*SSTableReader)
	c.isCompacting = false
	c.isSuper = config.GetColumnTypeTableName(table, columnFamily) == "Super"
	c.readStats = make([]int64, 0)
	c.writeStates = make([]int64, 0)
	// Get all data files associated with old Memtables for this table.
	// The names are <CfName>-<index>-Data.db, ...
	// The max is n and increment it to be used as the next index.
	indices := make([]int, 0)
	dataFileDirs := config.DataFileDirs
	for _, dir := range dataFileDirs {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			log.Fatal(err)
		}
		for _, fileInfo := range files {
			filename := fileInfo.Name() // base name <cf>-<index>-Data.db
			cfName := getColumnFamilyFromFileName(filename)
			if cfName == columnFamily {
				index := getIndexFromFileName(filename)
				indices = append(indices, index)
			}
		}
	}
	sort.Ints(indices)
	sz := len(indices)
	value := 0
	if sz > 0 {
		value = indices[sz-1]
	}
	atomic.StoreInt32(&c.fileIdxGenerator, int32(value))
	c.memtable = NewMemtable(table, columnFamily)
	c.binaryMemtable = NewBinaryMemtable(table, columnFamily)
	return c
}

func getColumnFamilyFromFileName(filename string) string {
	// filename is of format: <cf>-<index>-Data.db
	values := strings.Split(filename, "-")
	return values[0]
}

func getIdxFromFileName(filename string) int {
	// filename if of format: <table>-<cf>-<index>-Data.db
	values := strings.Split(filename, "-")
	if len(values) < 3 {
		log.Fatal("Invalid filename")
	}
	res, err := strconv.Atoi(values[2])
	if err != nil {
		log.Fatal(err)
	}
	return res
}

// fileInfoList encapsulates os.FileInfo for comparison needs.
type fileInfoList []os.FileInfo

func (f fileInfoList) Len() int {
	return len(f)
}
func (f fileInfoList) Less(i, j int) bool {
	return f[i].ModTime().UnixNano() > f[j].ModTime().UnixNano()
}
func (f fileInfoList) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

func (c *ColumnFamilyStore) onStart() {
	// scan for data files corresponding to this cf
	ssTables := make([]os.FileInfo, 0)
	// 获取所有与当前表相关的数据文件目录
	dataFileDirs := config.GetAllDataFileLocationsForTable(c.tableName)
	// 存储文件名与其完整路径
	filenames := make(map[string]string) // map to full name with dir
	for _, dir := range dataFileDirs {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			log.Fatal(err)
		}
		for _, fileInfo := range files {
			filename := fileInfo.Name() // name from FileInfo is always base name
			// 删除当前列族的空文件和临时文件
			if strings.Contains(filename, c.columnFamilyName) && (fileInfo.Size() == 0 || strings.Contains(filename, SSTableTmpFile)) {
				if err := os.Remove(path.Join(dir, filename)); err != nil {
					log.Print(err)
				}
				continue
			}
			// 从数据文件名中提取 cf 名：<cf>-<index>-Data.db
			cfName := getColumnFamilyFromFileName(filename)
			// 如果是当前列族的数据文件，就保存起来
			if cfName == c.columnFamilyName && strings.Contains(filename, "-Data.db") {
				ssTables = append(ssTables, fileInfo)
				// full path: var/storage/data/tablename/<cf>-<index>-Data.db
				filenames[filename] = path.Join(dir, filename)
			}
		}
	}
	sort.Sort(fileInfoList(ssTables)) // sort by modification time from old to new

	// 为列族下每个 sstable 数据文件创建一个 Reader 并保存
	for _, file := range ssTables {
		filename := filenames[file.Name()] // filename of the type: var/storage/data/tablename/<cf>-<index>-Data.db
		c.ssTables[filename] = openSSTableReader(filename)
	}
	log.Println("Submitting a major compaction task")
	// submit initial check-for-compaction request
	go c.doCompaction()

	// schedule hinted handoff
	if c.tableName == config.SysTableName && c.columnFamilyName == config.HintsCF {
		GetHintedHandOffManagerInstance().submit(c)
	}
	// TODO should also submit periodic flush
}

// 将一组文件按大小进行分组，使每个分组中的文件大小接近。
// 通过分组，可以更有效地对文件进行后续的压缩操作，避免在压缩时将大小差异较大的文件混在一起，造成效率低下或不合理的资源消耗。
//
// 分组规则如下：
// - 文件大小相近（当前文件大小在 [平均大小 / 2, 3 * 平均大小 / 2] 范围内）时，文件会被加入当前分组。
// - 如果文件小于 50MB，且当前分组的平均文件大小也小于 50MB，也会被加入当前分组。
// - 如果不符合以上条件，则创建新的分组。
func (c *ColumnFamilyStore) stageOrderedCompaction(files []string) map[int][]string {
	// stage the compactions, compact similar size files.
	// this function figures out the files close enough by
	// size and if they are greater than the threshold then
	// compact
	// sort the files based on the generation ID
	sort.Sort(ByFileName(files)) // 文件名格式 <cf>-<index>-Data.db ，这里根据 index 进行排序。
	buckets := make(map[int][]string)
	maxBuckets := 1000
	averages := make([]int64, maxBuckets)
	min := int64(50 * 1024 * 1024)
	i := 0
	for _, file := range files {
		// 打开文件
		f, err := os.Open(file)
		if err != nil {
			log.Fatal(err)
		}
		// 文件大小
		fileInfo, err := f.Stat()
		if err != nil {
			log.Fatal(err)
		}
		size := fileInfo.Size()

		// 判断当前文件是否可以加入到当前分组 i 中：
		//	- 如果当前文件大小在 [average/2, average * 3/2] 范围内，则认为它属于这个分组。
		//  - 如果文件大小小于 min，并且该分组的平均大小 average 也小于 min，则认为它也可以加入该分组。
		if (size > averages[i]/2 && size < 3*averages[i]/2) || (size < min && averages[i] < min) {
			averages[i] = (averages[i] + size) / 2 // 更新 average
			if _, ok := buckets[i]; !ok {
				buckets[i] = make([]string, 0)
			}
			buckets[i] = append(buckets[i], file)
		} else {
			// 如果不符合条件，则创建新分组，此时，增加分组索引 i，并将该文件加入到新的分组中。
			// 如果分组数已经超过了 maxBuckets，则退出循环（防止分组数量超过限制）。
			if i >= maxBuckets {
				break
			}
			i++
			buckets[i] = make([]string, 0)
			buckets[i] = append(buckets[i], file)
			averages[i] = size
		}
	}

	return buckets
}

// ByFileName ...
type ByFileName []string

// Len ...
func (p ByFileName) Len() int {
	return len(p)
}

// Less ...
func (p ByFileName) Less(i, j int) bool {
	return getIndexFromFileName(p[i]) < getIndexFromFileName(p[j])
}

// Swap ...
func (p ByFileName) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func getIndexFromFileName(filename string) int {
	// filename is of form <cf>-<index>-Data.db
	tokens := strings.Split(filename, "-")
	res, err := strconv.Atoi(tokens[len(tokens)-2])
	if err != nil {
		log.Fatal(err)
	}
	return res
}

// 计算传入的多个文件的总大小。
func getExpectedCompactedFileSize(files []string) int64 {
	// calculate total size of compacted files
	expectedFileSize := int64(0)
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatal(err)
		}
		fileInfo, err := f.Stat()
		if err != nil {
			log.Fatal(err)
		}
		size := fileInfo.Size()
		expectedFileSize += size
	}
	return expectedFileSize
}

// 找出最大的文件，并返回该文件的路径。
func getMaxSizeFile(files []string) string {
	maxSize := int64(0)
	maxFile := ""
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatal(err)
		}
		fileInfo, err := f.Stat()
		size := fileInfo.Size()
		if size > maxSize {
			maxSize = size
			maxFile = f.Name()
		}
	}
	return maxFile
}

func removeFromList(files []string, file string) {
	var i int
	var f string
	for i, f = range files {
		if f == file {
			break
		}
	}
	files = append(files[:i], files[i+1:]...)
}

// FPQ is a priority queue of FileStruct
// 优先级队列，按照 FileStruct.row.key 排序，越小越靠前。
type FPQ []*FileStruct

// Len ...
func (pq FPQ) Len() int {
	return len(pq)
}

// Less ...
// Less 方法定义如何比较两个 FileStruct 对象的大小(优先级)。
func (pq FPQ) Less(i, j int) bool {
	switch config.HashingStrategy {
	case config.Ophf:
		// 按字典顺序进行比较
		return pq[i].row.key < pq[j].row.key
	default:
		// 将 row.key 按照 : 分割，只比较 : 前面的部分
		lhs := strings.Split(pq[i].row.key, ":")[0]
		rhs := strings.Split(pq[j].row.key, ":")[0]
		return lhs < rhs
	}
}

// Swap ...
func (pq FPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// Push ...
func (pq *FPQ) Push(x interface{}) {
	item := x.(*FileStruct)
	*pq = append(*pq, item)
}

// Pop ...
func (pq *FPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[:n-1]
	return item
}

func (c *ColumnFamilyStore) initPriorityQueue(files []string, ranges []*dht.Range, minBufferSize int) *FPQ {
	pq := &FPQ{}
	if len(files) > 1 || (ranges != nil && len(files) > 0) {
		// 计算每个文件的缓冲区大小，compactionMemoryThres 表示进行压缩时分配的内存总量，分配给每个文件；
		bufferSize := c.compactionMemoryThres / len(files)
		if bufferSize > minBufferSize {
			bufferSize = minBufferSize
		}
		// 遍历每个文件，将符合条件的文件加入优先队列
		for _, file := range files {
			// 获取 sstable reader
			reader, _ := openedFiles.get(file)
			// 获取 row 迭代器
			fs := reader.getFileStruct()
			fs.advance(true)
			if fs.isExhausted() {
				continue
			}
			// 把 row 迭代器保存到 pq 中
			heap.Push(pq, fs)
		}
	}
	return pq
}

func readKV(file *os.File, buf []byte) (int, string, bool) {
	// read key length
	b4 := make([]byte, 4)
	n, err := file.Read(b4)
	if err != nil {
		log.Print(err)
		return 0, "", false
	}
	if n < 4 {
		return 0, "", false
	}
	buf = append(buf, b4...)
	keySize := int(binary.BigEndian.Uint32(b4))
	// read key bytes as key string
	bs := make([]byte, keySize)
	file.Read(bs)
	key := string(bs)
	buf = append(buf, bs...)
	// read value length
	file.Read(b4)
	buf = append(buf, b4...)
	valueSize := int(binary.BigEndian.Uint32(b4))
	// read value bytes
	bv := make([]byte, valueSize)
	file.Read(bv)
	buf = append(buf, bv...)
	return 4 + 4 + keySize + valueSize, key, true
}

func (c *ColumnFamilyStore) getTmpSSTablePath() string {
	fname := c.getTmpFileName()
	return config.GetDataFileLocationForTable(c.tableName, 0) + string(os.PathSeparator) + fname
}

func (c *ColumnFamilyStore) getTmpFileName() string {
	atomic.AddInt32(&c.fileIdxGenerator, 1)
	res := fmt.Sprintf("%v-%v-%v-Data.db", c.columnFamilyName, SSTableTmpFile, c.fileIdxGenerator)
	return res
}

func getApproximateKeyCount(files []string) int {
	count := 0
	for _, dataFileName := range files {
		sstable, _ := openedFiles.get(dataFileName)
		indexKeyCount := len(sstable.getIndexPositions())
		count += (indexKeyCount + 1) * SSTIndexInterval
	}
	return count
}

// merge all columnFamilies into a single instance, with only
// the newest versions of columns preserved.
//
// 多个 ColumnFamily 对象合并成一个单一的 ColumnFamily ，在合并过程中，保留了每个列族中的最新版本的列。
func resolve(columnFamilies []*ColumnFamily) *ColumnFamily {
	if len(columnFamilies) == 0 {
		return nil
	}
	// start from nothing so that we don't include
	// potential deleted columns from the first
	// instance
	cf0 := columnFamilies[0]
	cf := cf0.cloneMeShallow()
	// merge
	for _, cf2 := range columnFamilies {
		if cf.ColumnFamilyName != cf2.ColumnFamilyName {
			log.Fatal("name should be equal")
		}
		// 把 cf2 中的 column 添加到 cf 中，如果有相同 column 根据优先级(timestamp)决定保留谁。
		cf.addColumns(cf2)
		// 更新删除信息
		cf.deleteCF(cf2)
	}
	return cf
}

func (c *ColumnFamilyStore) merge(columnFamilies []*ColumnFamily) {
	cf := resolve(columnFamilies)
	columnFamilies = []*ColumnFamily{cf}
}

func resolveAndRemoveDeleted(columnFamilies []*ColumnFamily) *ColumnFamily {
	// 合并多个列族
	cf := resolve(columnFamilies)
	// 清理已被删除的列
	return removeDeletedGC(cf)
}

func removeDeletedGC(cf *ColumnFamily) *ColumnFamily {
	return removeDeleted(cf, getDefaultGCBefore())
}

// 根据垃圾回收时间戳 (gcBefore) 从 ColumnFamily 中删除已标记为删除的列。
func removeDeleted(cf *ColumnFamily, gcBefore int) *ColumnFamily {
	if cf == nil {
		return nil
	}
	// in case of a timestamp tie.
	for cname, column := range cf.Columns {
		_, ok := column.(SuperColumn)
		if ok {
			// 获取最小的删除时间戳：列族/超列/子列
			minTimestamp := column.getMarkedForDeleteAt()
			if minTimestamp > cf.getMarkedForDeleteAt() {
				minTimestamp = cf.getMarkedForDeleteAt()
			}

			// 先删除，后面清理完毕后会添加回来
			cf.remove(cname)
			// 克隆超列，只添加有效子列
			sc := column.(SuperColumn).cloneMeShallow()
			for _, subColumn := range column.GetSubColumns() {
				// 如果子列最近更新时间早于 minTimestamp 则直接丢弃。
				if subColumn.timestamp() > minTimestamp {
					// 如果子列被标记为删除并且删除时间早于 gcBefore ，则该子列会被丢弃，否则保留。
					if !subColumn.isMarkedForDelete() || subColumn.getLocalDeletionTime() > gcBefore {
						sc.addColumn(subColumn)
					}
				}
			}
			// 将清理完毕的超列添加回 cf ，其中只包含有效的子列
			if len(sc.getSubColumns()) > 0 || sc.getLocalDeletionTime() > gcBefore {
				cf.addColumn(sc)
			}
		} else if (column.isMarkedForDelete() && column.getLocalDeletionTime() <= gcBefore) || column.timestamp() <= cf.getMarkedForDeleteAt() {
			// 对于普通列，如果它们已经标记为删除，并且删除时间早于 gcBefore，则该列会被从 ColumnFamily 中移除。
			cf.remove(cname)
		}
	}

	// 如果列族中所有列都被删除，并且列族的本地删除时间早于 gcBefore ，删除该列族
	if cf.getColumnCount() == 0 && cf.getLocalDeletionTime() <= gcBefore {
		return nil
	}
	return cf
}

// This function does the actual compaction for files.
// It maintains a priority queue of the first key
// from each file and then removes the top of the queue
// and adds it to the SSTable and repeats this process
// while reading the next from each file until its done
// with all the files. The SSTable to which the keys are
// written represents the new compacted file. Before writing
// if there are keys that occur in multiple files and are
// the same then a resolution is done to get the latest data.
func (c *ColumnFamilyStore) doFileCompaction(files []string, minBufferSize int) int {
	// 估算合并后的文件大小
	expectedCompactedFileSize := getExpectedCompactedFileSize(files)
	// 获取合并后的文件存放位置
	compactionFileLocation := config.GetDataFileLocationForTable(c.tableName, expectedCompactedFileSize)
	// 如果没有足够空间存储压缩后的文件，选择删除一个最大的待合并文件，并重新尝试进行压缩。
	if compactionFileLocation == "" {
		maxFile := getMaxSizeFile(files)
		removeFromList(files, maxFile)
		return c.doFileCompaction(files, minBufferSize)
	}

	// 记录合并过程中的统计信息：耗时、读取字节数、写入字节数、读取和写入的键数。
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	totalBytesRead := int64(0)
	totalBytesWritten := int64(0)
	totalKeysRead := int64(0)
	totalKeysWritten := int64(0)

	// pq 中包含每个 sstable 的 row iterator ；该队列根据行键（row key）来排序，最小的键排在队首，优先读取。
	pq := c.initPriorityQueue(files, nil, minBufferSize)
	if pq.Len() == 0 {
		log.Print("nothing to compact")
		return 0
	}

	// 临时文件名
	mergedFileName := c.getTmpFileName()
	mergedFilePath := compactionFileLocation + string(os.PathSeparator) + mergedFileName
	var writer *SSTableWriter
	var ssTable *SSTableReader

	bufOut := make([]byte, 0)
	expectedBloomFilterSize := getApproximateKeyCount(files)
	if expectedBloomFilterSize <= 0 {
		expectedBloomFilterSize = SSTIndexInterval
	}
	log.Printf("Expeected bloom filter size: %v\n", expectedBloomFilterSize)
	// create the bloom filter for the compacted file
	// compactedBloomFilter := utils.NewBloomFilter(expectedBloomFilterSize, 15)
	columnFamilies := make([]*ColumnFamily, 0)

	newfile := ""
	lastkey := ""
	lfs := make([]*FileStruct, 0) // 存储具有相同行键的 FileStruct
	for pq.Len() > 0 || len(lfs) > 0 {
		// 取 row key 最小的 row iterator
		var fs *FileStruct
		if pq.Len() > 0 {
			fs = pq.Pop().(*FileStruct)
		}

		// 把具有相同 row key 的 FileStruct 存入 lfs 列表中，一起处理。
		if fs != nil && (lastkey == "" || lastkey == fs.key) {
			lastkey = fs.key
			lfs = append(lfs, fs)
		} else {
			// 至此，要么 fs 为 nil ，要么 lastkey != fs.key ，此时需要处理 lfs ，而 fs 后面要返回到 pq 中。
			sort.Sort(ByName(lfs)) // 基于 sstable 文件名进行排序，好像没啥用
			bufOut = make([]byte, 0)
			if len(lfs) > 1 {
				// 把具有相同 row key 的多个 cfs 合并成一个 cf
				for _, st := range lfs {
					/// 这个 if 有点难懂，看上去不会影响正常逻辑，只是提前合并，我先注释了。
					//// we want to add only 2 and resolve them right there in order to save on memory footprint
					//if len(columnFamilies) > 1 {
					//	c.merge(columnFamilies)
					//}
					//// deserialize into column families
					columnFamilies = append(columnFamilies, st.getColumnFamily())
				}
				merged := resolveAndRemoveDeleted(columnFamilies) // 合并多个列族，清理被删除的列
				if merged != nil {                                // 合并后非空
					CFSerializer.serializeWithIndexes(merged, bufOut) // 将合并后的列族序列化存入 bufOut 中
				}
				columnFamilies = make([]*ColumnFamily, 0) // 重置，便于下一轮合并

			} else {
				// 如果 lfs 中只有一个文件，则直接获取它的列族并序列化。
				CFSerializer.serializeWithIndexes(lfs[0].getColumnFamily(), bufOut)
			}

			// 将合并后的数据存储到新 SSTable 文件中，并更新写入键数。
			if writer == nil {
				writer = NewSSTableWriter(mergedFilePath, expectedBloomFilterSize)
			}
			writer.append(lastkey, bufOut)
			totalKeysWritten++

			// 遍历 lfs 中的每个文件，调用 advance(true) 让文件指针前进到下一个有效的行
			for _, filestruct := range lfs {
				filestruct.advance(true)      // 让文件指针前进到下一个有效的行
				if filestruct.isExhausted() { // 如果已经读完，不再加入 pq
					continue
				}
				heap.Push(pq, filestruct) // 否则，推回优先队列 pq 中，继续读取剩余的数据行
				totalKeysRead++           // 更新读取键数
			}

			// 清空 lfs 列表，准备下一轮
			lfs = make([]*FileStruct, 0)
			lastkey = ""

			// 因为可能是 fs.key != lastkey 触发了 lfs 合并，此时 fs 没有被处理，需要放回 pq 中。
			if fs != nil {
				// add back the fs since we processed the rest of filestructs
				heap.Push(pq, fs)
			}
		}
	}

	// 完成合并，将新 sstable 刷盘
	if writer != nil {
		ssTable = writer.closeAndOpenReader()
		newfile = writer.getFilename()
	}

	c.rwmu.Lock()
	defer c.rwmu.Unlock()

	// 将合并后新的 sstable 保存
	if newfile != "" {
		c.ssTables[newfile] = ssTable
		totalBytesWritten += getFileSizeFromName(newfile)
	}

	// 将已合并的 sstable 删除
	for _, file := range files {
		delete(c.ssTables, file)
	}
	for _, file := range files {
		getSSTableReader(file).delete()
	}

	log.Printf("Compacted to %v. %v/%v bytes for %v/%v keys read/written. Time: %vms.",
		newfile, totalBytesRead, totalBytesWritten, totalKeysRead, totalKeysWritten,
		time.Now().UnixNano()/int64(time.Millisecond)-startTime)
	return len(files)
}

func readInt(r io.Reader) int {
	b4 := make([]byte, 4)
	r.Read(b4)
	res := binary.BigEndian.Uint32(b4)
	return int(res)
}

// ByName ...
type ByName []*FileStruct

// Len ...
func (p ByName) Len() int {
	return len(p)
}

// Less ...
func (p ByName) Less(i, j int) bool {
	return p[i].getFileName() < p[j].getFileName()
}

// Swap ...
func (p ByName) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// 执行多个 SSTable 文件的压缩，以提高读取效率和减少存储空间
func (c *ColumnFamilyStore) doCompaction() int {
	files := make([]string, 0)
	for file, _ := range c.ssTables {
		files = append(files, file)
	}

	// 把文件分桶，每桶包含大小接近的一些文件
	buckets := c.stageOrderedCompaction(files)
	// 逐个桶进行压缩
	filesCompacted := 0
	for _, fileList := range buckets {
		// 桶中的文件数小于阈值，不需压缩
		if len(fileList) < config.MinCompactionThres {
			continue
		}
		// 按照序号对文件名排序
		sort.Sort(ByFileName(fileList))

		// 一次最多压缩 mark 个文件
		mark := len(fileList)
		if mark > config.MaxCompactionThres {
			mark = config.MaxCompactionThres
		}

		// 把需要压缩的文件名提取出来
		count := 0
		files = make([]string, 0)
		for _, file := range fileList {
			files = append(files, file)
			count++
			if count == mark {
				break
			}
		}

		// 执行压缩
		filesCompacted += c.doFileCompaction(files, c.bufSize)
	}

	return filesCompacted
}

func getCurrentTimeInMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// 从列族存储中根据给定的查询过滤器（QueryFilter）获取列族数据，并进行垃圾回收，最终返回一个没有已经删除列（垃圾回收后的）的列族数据
//
// 参数：
//   - filter: 指定查询条件，可以是基于列名范围、列族名、超列名等过滤条件。
//   - gcBefore: 垃圾回收时间戳，表示所有在该时间戳之前被删除的列将被清除（不可回收的列会被移除）。
//
//
// 如果查询的条件包含超列（例如，一个列族下有多个子列），就会先查询超列，确保能够拿到超列中的数据。
// 这里如果存在超列，使用一个 NameQueryFilter 来过滤和获取超列数据。
//

func (c *ColumnFamilyStore) getColumnFamilyGC(filter QueryFilter, gcBefore int) *ColumnFamily {
	// get a list of columns starting from a given column, in a specified order
	// only the latest version of a column is returned
	start := getCurrentTimeInMillis()
	// if we are querying subcolumns of a supercolumn, fetch the
	// supercolumn with NameQueryFilter, then filter in-memory

	// 处理超列查询
	if filter.getPath().SuperColumnName != nil {
		// 创建一个过滤器，用于过滤超列
		nameFilter := NewNamesQueryFilter(
			filter.getKey(),
			NewQueryPathCF(c.columnFamilyName),
			filter.getPath().SuperColumnName,
		)
		// 从列族中获取超列数据
		cf := c.getColumnFamily(nameFilter)
		if cf == nil || cf.getColumnCount() == 0 {
			return cf
		}
		// 获取超列数据，进行过滤
		sc := cf.GetSortedColumns()[0].(SuperColumn)
		scFiltered := filter.filterSuperColumn(sc, gcBefore)
		// 克隆列族，并加入过滤后的超列
		cfFiltered := cf.cloneMeShallow()
		cfFiltered.addColumn(scFiltered)
		c.readStats = append(c.readStats, getCurrentTimeInMillis()-start)
	}

	/// 1. 先从 c.memtable 中查询

	// we are querying top-level, do a merging fetch with indices
	c.rwmu.RLock()
	defer c.rwmu.RUnlock()
	iterators := make([]ColumnIterator, 0)
	spew.Printf("\tc.memtable: %#+v\n\n", c.memtable)

	// 获取内存表的列族数据
	iter := filter.getMemColumnIterator(c.memtable)
	spew.Printf("\titer: %#+v\n\n", iter)
	// 存储 row 的所有列，用于返回
	returnCF := iter.getColumnFamily()
	spew.Printf("\treturnCF: %#+v\n\n", returnCF)
	// return returnCF
	iterators = append(iterators, iter)

	/// 2. 从还没被写入磁盘的 Memtables 中读取

	// add the memtable being flushed
	memtables := getUnflushedMemtables(filter.getPath().ColumnFamilyName)
	for _, memtable := range memtables {
		iter = filter.getMemColumnIterator(memtable)
		returnCF.deleteCF(iter.getColumnFamily())
		iterators = append(iterators, iter)
	}

	/// 3. 从磁盘上的 SSTable 上读取

	// add the SSTables on disk
	sstables := make([]*SSTableReader, 0)
	for _, sstable := range c.ssTables {
		sstables = append(sstables, sstable)
		iter = filter.getSSTableColumnIterator(sstable) // 用于遍历 filter.key 在 SSTable 中的列，内部会把所有 col 从数据文件读取到内存中，然后迭代。
		if iter.hasNext() {                             // initializes iter.CF
			returnCF.deleteCF(iter.getColumnFamily()) // 更新 cf 删除时间
		}
		iterators = append(iterators, iter)
	}

	// 获取了内存表、未刷新内存表、SSTable 中的所有数据后，使用 CollatedIterator 来合并数据。
	collated := NewCollatedIterator(iterators)
	// 据查询过滤器进一步处理这些合并的数据，特别是执行垃圾回收（删除已经标记为删除的列）。
	filter.collectCollatedColumns(returnCF, collated, gcBefore)
	// 根据垃圾回收时间戳 (gcBefore) 从 returnCF 中删除已标记为删除的列。
	res := removeDeleted(returnCF, gcBefore)
	// 关闭所有用来查询列族的迭代器。
	for _, ci := range iterators {
		ci.close()
	}
	// 耗时统计
	c.readStats = append(c.readStats, getCurrentTimeInMillis()-start)
	// 返回结果
	return res
}

func getUnflushedMemtables(cfName string) []*Memtable {
	return getMemtablePendingFlushNotNull(cfName)
}

// 获取指定列族的所有待刷新 Memtables 。
func getMemtablePendingFlushNotNull(columnFamilyName string) []*Memtable {
	memtables, ok := memtablesPendingFlush[columnFamilyName]
	if !ok {
		memtablesPendingFlush[columnFamilyName] = make([]*Memtable, 0)
		// might not be the object we just put, if there was a race
		memtables = memtablesPendingFlush[columnFamilyName]
	}
	return memtables
}

// 当前时间减去 10 天对应的时间戳(秒)，表示垃圾回收 10 天前的数据。
func getDefaultGCBefore() int {
	curTime := time.Now().UnixNano() / int64(time.Second)
	return int(curTime - int64(config.GcGraceInSeconds))
}

func (c *ColumnFamilyStore) getColumnFamily(filter QueryFilter) *ColumnFamily {
	return c.getColumnFamilyGC(filter, getDefaultGCBefore())
}

func (c *ColumnFamilyStore) apply(key string, columnFamily *ColumnFamily, cLogCtx *CommitLogContext) {
	start := getCurrentTimeInMillis()
	// 1. 获取当前的 Memtable
	initialMemtable := c.getMemtableThreadSafe()
	// 2. 检查 Memtable 是否超过阈值，若超过把当前 memtable 落盘然后新建一个 memtable
	if initialMemtable.isThresholdViolated() {
		// [重要]
		c.switchMemtableN(initialMemtable, cLogCtx)
	}
	// 3. 锁定 Memtable 操作，确保线程安全
	c.memMu.Lock()
	defer c.memMu.Unlock()
	// 4. 将数据写入 Memtable
	c.memtable.put(key, columnFamily)
	// 5. 记录写操作的耗时
	c.writeStates = append(c.writeStates, getCurrentTimeInMillis()-start)
}

func (c *ColumnFamilyStore) getMemtableThreadSafe() *Memtable {
	c.memMu.RLock()
	defer c.memMu.RUnlock()
	return c.memtable
}

// func (c *ColumnFamilyStore) switchMemtable(key string, columnFamily *ColumnFamily, cLogCtx *CommitLogContext) {
// 	// Used on start up when we are recovering from logs
// 	c.memtable.mu.Lock()
// 	c.memtable = NewMemtable(c.tableName, c.columnFamilyName)
// 	c.memtable.mu.Unlock()
// 	if key != c.memtable.flushKey {
// 		c.memtable.mu.Lock()
// 		c.memtable.put(key, columnFamily, cLogCtx)
// 		c.memtable.mu.Unlock()
// 	}
// }

func (c *ColumnFamilyStore) switchMemtableN(memtable *Memtable, ctx *CommitLogContext) {
	c.memMu.Lock()
	defer c.memMu.Unlock()
	// 已冻结，返回
	if memtable.isFrozen {
		return
	}
	// 冻结
	memtable.freeze()
	// 将当前的 Memtable 添加到待刷新列表
	pendingFlush := getMemtablePendingFlushNotNull(c.columnFamilyName)
	pendingFlush = append(pendingFlush, memtable)
	// 提交刷新
	submitFlush(memtable, ctx)
	// 创建新的 Memtable 来接收新的数据
	c.memtable = NewMemtable(c.tableName, c.columnFamilyName)
}

func submitFlush(memtable *Memtable, cLogCtx *CommitLogContext) {
	// submit memtables to be flushed to disk
	go func() {
		memtable.flush(cLogCtx)
		// 将当前的 Memtable 从待刷新列表中移除
		memtables := getMemtablePendingFlushNotNull(memtable.cfName)
		memtables = remove(memtables, memtable)
	}()
}

func (c *ColumnFamilyStore) getNextFileName() string {
	// increment twice to generate non-consecutive numbers
	atomic.AddInt32(&c.fileIdxGenerator, 1)
	atomic.AddInt32(&c.fileIdxGenerator, 1)
	name := c.tableName + "-" + c.columnFamilyName + "-" +
		strconv.Itoa(int(c.fileIdxGenerator))
	return name
}

func (c *ColumnFamilyStore) forceFlush() {
	// 没有待刷新的 memtable
	if c.memtable.isClean() {
		return
	}
	// 创建 CommitLogContext ，用于追踪某个特定行（Row）在提交日志（Commit Log）中的位置。
	ctx := openCommitLogE().getContext()
	// 将 c.memtable 刷到磁盘
	c.switchMemtableN(c.memtable, ctx)
}

func (c *ColumnFamilyStore) onMemtableFlush(cLogCtx *CommitLogContext) {
	// Called when the memtable is frozen and ready to be flushed
	// to disk. This method informs the commitlog that a particular
	// columnFamily is being flushed to disk.
	if cLogCtx.isValidContext() {
		openCommitLogE().onMemtableFlush(c.tableName, c.columnFamilyName, cLogCtx)
	}
}

// 本函数将该 SSTable 文件 Reader 缓存到 c.ssTables 中，这样可以提高读取效率。
func (c *ColumnFamilyStore) storeLocation(sstable *SSTableReader) {
	// Called after the memtable flushes its inmemory data.
	// This information is cached in the ColumnFamilyStore.
	// This is useful for reads because the ColumnFamilyStore first
	// looks in the inmemory store and then into the disk to find
	// the key. If invoked during recoveryMode the onMemtableFlush()
	// need not be invoked.

	c.sstableMu.Lock()
	c.ssTables[sstable.getFilename()] = sstable
	ssTableCount := len(c.ssTables)
	c.sstableMu.Unlock()

	// it's ok if compaction gets submitted multiple times
	// while one is already in process. worst that happens
	// is, compactor will count the sstable files and decide
	// there are not enough to bother with.
	if ssTableCount >= config.MinCompactionThres {
		log.Print("Submitting " + c.columnFamilyName + " for compaction")
		go c.doCompaction()
	}
}

func (c *ColumnFamilyStore) forceCompaction(ranges []*dht.Range, target *network.EndPoint, skip int64, fileList []string) bool {
	// this method forces a compaction of the sstable on disk
	// TODO
	return true
}

func storeBloomFilter(filename string, bf *utils.BloomFilter) {
	SSTbfs[filename] = bf
}
