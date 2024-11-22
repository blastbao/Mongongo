// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"log"
	"os"
)

// FileStruct ...
type FileStruct struct {
	key       string        // 当前行键
	row       *IteratingRow // 遍历当前 key 的所有 column
	exhausted bool          // 文件是否读取完毕
	file      *os.File      // 文件句柄
	sstable   *SSTableReader
}

// NewFileStruct ...
func NewFileStruct(s *SSTableReader) *FileStruct {
	f := &FileStruct{}
	f.exhausted = false
	var err error
	f.file, err = os.Open(s.dataFileName) // 打开 SSTable 文件
	if err != nil {
		log.Fatal(err)
	}
	f.sstable = s // 设置 SSTableReader
	return f
}

// 读取行
func (f *FileStruct) advance(materialize bool) {
	// 文件已读取完毕
	if f.exhausted {
		log.Fatal("index out of bounds!")
	}

	// 文件已读取完毕
	if getCurrentPos(f.file) == getFileSize(f.file) {
		f.file.Close() // 关闭文件
		f.exhausted = true
		return
	}

	// 读取新行，创建行的列迭代器
	f.row = NewIteratingRow(f.file, f.sstable)
	if materialize {
		// 遍历当前行的所有列
		for f.row.hasNext() {
			column := f.row.next()
			f.row.getEmptyColumnFamily().addColumn(column)
		}
	} else {
		// 跳过列
		f.row.skipRemaining()
	}
}

func (f *FileStruct) getFileName() string {
	return f.file.Name()
}

func (f *FileStruct) getColumnFamily() *ColumnFamily {
	return f.row.getEmptyColumnFamily()
}

func (f *FileStruct) isExhausted() bool {
	return f.exhausted
}

func getFileSizeFromName(name string) int64 {
	file, err := os.Open(name)
	if err != nil {
		log.Fatal(err)
	}
	return getFileSize(file)
}

func getFileSize(file *os.File) int64 {
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal(err)
	}
	return fileInfo.Size()
}
