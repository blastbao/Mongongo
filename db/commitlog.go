// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"encoding/binary"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DistAlchemist/Mongongo/config"
)

// CommitLog tracks every write operation into the system.
// The aim of the commit log is to be able to successfully
// recover data that was not stored to disk via the memtable.
// Every commit log maintains a header represented by the
// abstraction CommitLogHeader. The header contains a bit
// array and an array of int64 and both the arrays are of
// size: # column families. Whenever a ColumnFamily is
// written to, for the first time its bit flag is set to
// one in the CommitLogHeader. When it is flushed to disk
// by the Memtable its corresponding bit in the header is
// set to zero. This helps track which CommitLog can be thrown
// away as a result of Memtable flushes. However if a ColumnFamily
// is flushed and again written to disk then its entry in the
// array of int64 is updated with the offset in the CommitLog
// file where it was written. This helps speed up recovery since
// we can seek to these offsets and start processing the commit
// log. Every Commit Log is rolled over everytime it reaches its
// threshold in size. Over time there could be a number of
// commit logs that would be generated. However whenever we flush
// a column family disk and update its bit flag we take this bit
// array and bitwise & it with the headers of the other commit logs
// that are older.
type CommitLog struct {
	bufSize              int              // 缓冲区大小
	table                string           // 关联表名
	logFile              string           // 日志文件名
	clHeader             *CommitLogHeader // 日志头，包含列族标记位和列族偏移量信息
	commitHeaderStartPos int64            // 日志头的起始位置
	forcedRollOver       bool             // 强制滚动
	logWriter            *os.File         // 文件指针
}

var (
	clInstance  = map[string]*CommitLog{}
	clSInstance *CommitLog // stands for Single Instance
	clHeaders   = map[string]*CommitLogHeader{}
	clmu        sync.Mutex
)

// CommitLogContext represents the context of commit log
//
// CommitLogContext 用于追踪某个特定行（Row）在提交日志（Commit Log）中的位置。
// 每次向提交日志中添加一行数据时（例如，在 add 方法中），都会生成一个 CommitLogContext 实例。
// 通过 CommitLogContext，系统能够知道某个数据写入到日志中的确切位置，如果后续需要恢复数据或回滚操作，能够依据这些位置信息定位数据。
type CommitLogContext struct {
	file     string // 日志文件名，如 CommitLog-1632123649000.log
	position int64  // 在日志文件中的位置
}

// NewCommitLogContext creates a new commitLogContext
func NewCommitLogContext(file string, position int64) *CommitLogContext {
	c := &CommitLogContext{}
	c.file = file
	c.position = position
	return c
}

func (c *CommitLogContext) isValidContext() bool {
	return c.position != -1
}

// 为下一个日志文件设置文件名，c.logFile = /path/to/logs/CommitLog-1616175872123.log
func (c *CommitLog) setNextFileName() {
	c.logFile = config.LogFileDir +
		string(os.PathSeparator) +
		"CommitLog-" +
		strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10) +
		".log"
}

func createCLWriter(file string) *os.File {
	f, err := os.Create(file)
	if err != nil {
		log.Print(err)
	}
	return f
}

func (c *CommitLog) writeCommitLogHeader() {
	// writes a header with all bits set to zero
	// table := OpenTable(c.table)
	// cfSize := table.getNumberOfColumnFamilies() // number of cf
	cfSize := getColumnFamilyCount()
	c.commitHeaderStartPos = 0
	// write the commit log header
	c.clHeader = NewCommitLogHeader(cfSize)
	c.writeCLH(c.clHeader.toByteArray(), false)
}

func (c *CommitLog) writeCommitLogHeaderB(bytes []byte, reset bool) {
	// record the current position
	currentPos, err := c.logWriter.Seek(c.commitHeaderStartPos, 0)
	if err != nil {
		log.Fatal(err)
	}
	currentPos += c.commitHeaderStartPos
	// write the commit log header
	_, err = c.logWriter.Write(bytes)
	if err != nil {
		log.Print(err)
	}
	if reset {
		// seek back to the old position
		c.logWriter.Seek(currentPos, 0)
	}
}

func (c *CommitLog) writeOldCommitLogHeader(oldFile string, header *CommitLogHeader) {
	// 打开 log
	logWriter := createCLWriter(oldFile)
	// 把 header 按照 len + data 方式写入 log
	writeCommitLogHeader(logWriter, header.toByteArray())
	// 关闭 log
	logWriter.Close()
}

// 将 bytes 写入到 commit log 的 header 部分，如果 reset 为 true 在写入后将文件偏移重置。
func (c *CommitLog) writeCLH(bytes []byte, reset bool) {
	// 获取 logWriter 文件当前写入位置
	currentPos, err := c.logWriter.Seek(c.commitHeaderStartPos, 0)
	if err != nil {
		log.Print(err)
	}
	// write the commit log header
	c.logWriter.Write(bytes)
	if reset {
		c.logWriter.Seek(currentPos, 0)
	}
}

func (c *CommitLog) getContext() *CommitLogContext {
	ctx := NewCommitLogContext(c.logFile, getCurrentPos(c.logWriter))
	return ctx
}

// NewCommitLog creates a new commit log
func NewCommitLog(table string, recoveryMode bool) *CommitLog {
	c := &CommitLog{}
	c.table = table
	c.forcedRollOver = false
	if !recoveryMode {
		c.setNextFileName()                     // 为下一个日志文件设置文件名
		c.logWriter = createCLWriter(c.logFile) // 创建文件
		c.writeCommitLogHeader()                //
	}
	return c
}

// NewCommitLogE creates a new commit log
func NewCommitLogE(recoveryMode bool) *CommitLog {
	c := &CommitLog{}
	// c.table = table
	c.forcedRollOver = false
	if !recoveryMode {
		c.setNextFileName()
		c.logWriter = createCLWriter(c.logFile)
		c.writeCommitLogHeader()
	}
	return c
}

func openCommitLog(table string) *CommitLog {
	commitLog, ok := clInstance[table]
	if !ok {
		clmu.Lock()
		defer clmu.Unlock()
		commitLog = NewCommitLog(table, false)
		clInstance[table] = commitLog
	}
	return commitLog
}

func openCommitLogE() *CommitLog {
	clmu.Lock()
	defer clmu.Unlock()
	if clSInstance == nil {
		clSInstance = NewCommitLogE(false)
	}
	return clSInstance
}

// 检查某个列族是否第一次被写入，如果是，更新提交日志的头信息，并将更新后的头信息写入日志文件。
func (c *CommitLog) maybeUpdateHeader(row *Row) {
	// update the header of the commit log if a
	// new column family is encountered for the
	// first time
	table := OpenTable(row.Table)
	for cfName := range row.getColumnFamilies() {
		id := table.getColumnFamilyID(cfName)
		if c.clHeader.isDirty(id) == false {
			c.clHeader.turnOn(id, getCurrentPos(c.logWriter))
			c.seekAndWriteCommitLogHeader(c.clHeader.toByteArray())
		}
	}
}

func (c *CommitLog) seekAndWriteCommitLogHeader(bytes []byte) {
	// writes header at the beginning of the file, then seeks
	// back to current position
	currentPos := getCurrentPos(c.logWriter)
	c.logWriter.Seek(0, 0)
	writeCommitLogHeader(c.logWriter, bytes)
	c.logWriter.Seek(currentPos, 0)
}

func writeCommitLogHeader(logWriter *os.File, bytes []byte) {
	writeInt64(logWriter, int64(len(bytes))) // 长度
	writeBytes(logWriter, bytes)             // 数据
}

// 检查当前日志文件的大小是否超过了指定的阈值，如果是，则滚动日志文件（即创建一个新的日志文件）。
func (c *CommitLog) maybeRollLog() bool {
	// 检查日志文件大小是否超过阈值
	if getFileSize(c.logWriter) >= config.LogRotationThres {
		// 设置下一个日志文件的名字
		c.setNextFileName()
		// 获取当前日志文件的名字
		oldLogFile := c.logWriter.Name()
		// 关闭当前日志文件
		c.logWriter.Close()
		// 创建并打开新的日志文件
		c.logWriter = createCLWriter(c.logFile)
		// squirrel away the old commit log header
		// ???
		clHeaders[oldLogFile] = NewCommitLogHeaderC(c.clHeader) // 保存当前提交日志的头信息
		c.clHeader.clear()                                      // 清空当前日志头信息
		// 写入新的日志头信息
		writeCommitLogHeader(c.logWriter, c.clHeader.toByteArray())
		return true
	}
	return false
}

// add the specified row to the commit log. This method will
// reset the file offset to what it is before the start of
// the operation in case of any problems. This way we can
// assume that the subsequent commit log entry will override
// the garbage left over by the previous write.
//
// 向提交日志中添加一个新的行
func (c *CommitLog) add(row *Row) *CommitLogContext {
	// 将传入的 row 对象序列化成字节数组
	buf := make([]byte, 0)
	rowSerialize(row, buf)
	// 获取 log 当前写入位置，创建 CommitLogContext
	pos := getCurrentPos(c.logWriter)
	logCtx := NewCommitLogContext(c.logFile, pos)
	// 检查当前 row 的某个列族是否是第一次写入，如果是，会更新头
	c.maybeUpdateHeader(row)
	// 写入 row 数据长度
	writeInt64(c.logWriter, int64(len(buf)))
	// 写入 row 数据
	writeBytes(c.logWriter, buf)
	// 检查当前日志文件是否已经超过了设定的大小阈值。如果超过了阈值，就会触发日志文件的滚动（即创建一个新的日志文件，继续写入）。
	c.maybeRollLog()
	// 返回 CommitLogContext
	return logCtx
}

// writeString will first write string length(int32)
// and then write string in bytes
func writeString(file *os.File, s string) int {
	// 写入字符串长度
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(len(s)))
	file.Write(b4)
	// 写入字符串的字节数据
	file.Write([]byte(s))
	// 返回写入的总字节数
	return 4 + len(s)
}
func writeStringB(file []byte, s string) int {
	// write string length
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(len(s)))
	file = append(file, b4...)
	// write string bytes
	file = append(file, []byte(s)...)
	// return total bytes written
	return 4 + len(s)
}

func writeInt(file *os.File, num int) int {
	return writeInt32(file, int32(num))
}

func writeIntB(buf []byte, num int) int {
	return writeInt32B(buf, int32(num))
}

func writeInt32(file *os.File, num int32) int {
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(num))
	file.Write(b4)
	return 4
}

func writeInt32B(buf []byte, num int32) int {
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(num))
	buf = append(buf, b4...)
	return 4
}

func writeInt64(file *os.File, num int64) int {
	b8 := make([]byte, 8)
	binary.BigEndian.PutUint64(b8, uint64(num))
	file.Write(b8)
	return 8
}

func writeInt64B(buf []byte, num int64) int {
	b8 := make([]byte, 8)
	binary.BigEndian.PutUint64(b8, uint64(num))
	buf = append(buf, b8...)
	return 8
}

func writeBool(file *os.File, b bool) int {
	if b == true {
		file.Write([]byte{1})
	} else {
		file.Write([]byte{0})
	}
	return 1
}

func writeBoolB(file []byte, b bool) int {
	if b == true {
		file = append(file, byte(1))
	} else {
		file = append(file, byte(0))
	}
	return 1
}

func writeBytes(file *os.File, b []byte) int {
	// write byte length
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(len(b)))
	file.Write(b4)
	// write bytes
	file.Write(b)
	// return total bytes written
	return 4 + len(b)
}

// +-------------------+-------------------------------+
// | 4字节长度信息       | 数据内容                       |
// +-------------------+-------------------------------+
func writeBytesB(buf []byte, b []byte) int {
	// write byte length
	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(len(b)))
	buf = append(buf, b4...)
	// write bytes
	buf = append(buf, b...)
	// return total bytes written
	return 4 + len(b)
}

// func (c *CommitLog) checkThresholdAndRollLog(fileSize int64) {
// 	if fileSize >= config.LogRotationThres || c.forcedRollOver {
// 		// rolls the current log file over to a new one
// 		c.setNextFileName()
// 		oldLogFile := c.logWriter.Name()
// 		c.logWriter.Close()
// 		// change logWriter to new log file
// 		c.logWriter = c.createWriter(c.logFile)
// 		// save old log header
// 		clHeaders[oldLogFile] = c.clHeader.copy()
// 		// zero out positions in old file log header
// 		c.clHeader.zeroPositions()
// 		c.writeCommitLogHeaderB(c.clHeader.toByteArray(), false)
// 		// Get the list of files in commit log dir if it is greater than a
// 		// certain number. Force flush all the column families that way we
// 		// ensure that a slowly populated column family is not screwing up
// 		// by accumulating the commit log. TODO
// 	}
// }

// func (c *CommitLog) updateHeader(row *Row) {
// 	// update the header of the commit log if
// 	// a new column family is encounter for the
// 	// first time
// 	table := openTable(c.table)
// 	for cName := range row.columnFamilies {
// 		id := table.tableMetadata.cfIDs[cName]
// 		if c.clHeader.header[id] == 0 || (c.clHeader.header[id] == 1 &&
// 			c.clHeader.position[id] == 0) {
// 			// really ugly workaround for getting file current position
// 			// but I cannot find other way :(
// 			currentPos, err := c.logWriter.Seek(0, 0)
// 			if err != nil {
// 				log.Fatal(err)
// 			}
// 			c.logWriter.Seek(currentPos, 0)
// 			c.clHeader.turnOn(id, currentPos)
// 			c.writeCommitLogHeaderB(c.clHeader.toByteArray(), true)
// 		}
// 	}
// }

func (c *CommitLog) onMemtableFlush(tableName, cf string, cLogCtx *CommitLogContext) {
	// Called on memtable flush to add to the commit log a token
	// indicating that this column family has been flushed.
	// The bit flag associated with this column family is set
	// in the header and this is used to decide if the log
	// file can be deleted.
	table := OpenTable(tableName)
	id := table.tableMetadata.cfIDs[cf]
	c.discard(cLogCtx, id)
}

// ByTime provide struct to sort file by timestamp
type ByTime []string

// Len implements the length of the slice
func (a ByTime) Len() int {
	return len(a)
}

// Less implements less comparator
func (a ByTime) Less(i, j int) bool {
	return getCreationTime(a[i]) < getCreationTime(a[j])
}

// Swap implements swap method
func (a ByTime) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func getCreationTime(name string) int64 {
	arr := strings.FieldsFunc(name, func(r rune) bool {
		return r == '-' || r == '.'
	})
	num, err := strconv.ParseInt(arr[len(arr)-2], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return num
}

// updateDeleteTime log segments whose contents have
// been turned into SSTables
func (c *CommitLog) discard(cLogCtx *CommitLogContext, id int) {
	// Check if old commit logs can be deleted.
	// 查找当前提交日志文件（cLogCtx.file）的日志头
	header, ok := clHeaders[cLogCtx.file]
	if !ok {
		// 如果当前正在处理的文件是 c.logFile ，即当前提交日志文件，则使用 c.clHeader 作为日志头，并将其缓存到 clHeaders 中。
		if c.logFile == cLogCtx.file {
			// we are dealing with the current commit log
			header = c.clHeader
			clHeaders[cLogCtx.file] = c.clHeader
		} else {
			return
		}
	}

	//
	// commitLogHeader.turnOff(id)

	// 获取所有历史日志文件的列表并将其存储在 oldFiles 中，按时间排序。
	oldFiles := make([]string, 0)
	for key := range clHeaders {
		oldFiles = append(oldFiles, key)
	}
	sort.Sort(ByTime(oldFiles))

	// Loop through all the commit log files in the history.
	// Process the files that are older than the one in the
	// context. For each of these files the header needs to
	// modified by performing a bitwise & of the header with
	// the header of the file in the context. If we encounter
	// file in the context in our list of old commit log files
	// then we update the header and write it back to the commit
	// log.
	for _, oldFile := range oldFiles {
		if oldFile == cLogCtx.file {
			// Need to turn on again. Because we always keep
			// the bit turned on and the position indicates
			// from where the commit log needs to be read.
			// When a flush occurs we turn off perform &
			// operation and then turn on with the new position.
			header.turnOn(id, cLogCtx.position)
			if oldFile == c.logFile {
				c.seekAndWriteCommitLogHeader(header.toByteArray())
			} else {
				c.writeOldCommitLogHeader(cLogCtx.file, header)
			}
			break
		}
		header.turnOff(id)
		if header.isSafeToDelete() {
			log.Printf("Deleting commit log: %v\n", oldFile)
			err := os.Remove(oldFile)
			if err != nil {
				log.Fatal(err)
			}
			delete(clHeaders, oldFile)
		} else {
			c.writeOldCommitLogHeader(oldFile, header)
		}
	}
}
