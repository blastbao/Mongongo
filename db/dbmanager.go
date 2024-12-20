// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package db

import (
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/davecgh/go-spew/spew"

	"github.com/DistAlchemist/Mongongo/config"
	"github.com/DistAlchemist/Mongongo/dht"
	"github.com/DistAlchemist/Mongongo/network"
	"github.com/DistAlchemist/Mongongo/utils"
)

var (
	mu             sync.Mutex
	minstance      *Manager
	sysMetadata    *StorageMetadata
	sysLocationCF  = "LocationInfo"
	sysLocationKey = "L" // only one row in location cf
	sysToken       = "Token"
	sysGeneration  = "Generation"
)

// Manager manages database
type Manager struct {
}

// GetManagerInstance return an instance of DBManager
func GetManagerInstance() *Manager {
	mu.Lock()
	defer mu.Unlock()
	if minstance == nil {
		minstance = &Manager{}
		minstance.init()
	}
	return minstance
}

func (d *Manager) init() {
	// 从配置中获取表元数据，包含 <table, column family, meta>
	tableToColumnFamily := config.Init()
	// 根据表元数据初始化 meta 信息，包含列族、列族ID、列族类型等；
	storeMetadata(tableToColumnFamily)
	// 为每个表的每个列族创建 ColumnFamilyStore 实例并启动，其负责列族的数据管理。
	for table := range tableToColumnFamily {
		tbl := OpenTable(table)
		tbl.onStart()
	}
	//
	recoveryMgr := GetRecoveryManager()
	recoveryMgr.doRecovery()
}

func storeMetadata(tableToColumnFamily map[string]map[string]config.CFMetaData) {
	// 分配列族 ID
	cfid := 0
	for table, cfs := range tableToColumnFamily {
		// 为 table 初始化 meta 信息，其中包含：列族名 => 列族 ID，列族名 => 列族类型，列族 ID => 列族名 。
		meta := getTableMetadataInstance(table)
		for cf := range cfs {
			meta.Add(cf, cfid, config.GetColumnTypeTableName(table, cf))
			cfid++
		}
	}
}

// // create metadata tables. table stores tableName and columnFamilyName
// // each column family has an ID
// func storeMetadata(tableToColumnFamily map[string]map[string]config.CFMetaData) error {
// 	var cnt int32
// 	cnt = 0
// 	for _, columnFamilies := range tableToColumnFamily {
// 		tmetadata := GetTableMetadataInstance()
// 		if tmetadata.isEmpty() {
// 			for columnFamily := range columnFamilies {
// 				tmetadata.add(columnFamily, int(atomic.AddInt32(&cnt, 1)),
// 					config.GetColumnType(columnFamily))
// 			}
// 			tmetadata.add()
// 		}
// 	}
// 	return nil
// }

// Start reads the system table and retrives the metadata
// associated with this storage instance. The metadata is
// stored in a Column Family called LocationInfo which has
// two columns: "Token" and "Generation". This is the token
// that gets gossiped around and the generation info is used
// for FD. We also store whether we're in bootstrap mode in
// a third column.
func (d *Manager) Start() *StorageMetadata {
	// storageMetadata := &StorageMetadata{}
	// return storageMetadata
	return sysInitMetadata()
}

func sysInitMetadata() *StorageMetadata {
	mu.Lock()
	defer mu.Unlock()
	if sysMetadata != nil {
		return sysMetadata
	}
	// read the sytem table to retrieve the storage ID
	// and the generation
	table := OpenTable(config.SysTableName)
	filter := NewIdentityQueryFilter(sysLocationKey, NewQueryPathCF(sysLocationCF))
	cf := table.getColumnFamilyStore(sysLocationCF).getColumnFamily(filter)
	p := dht.RandomPartInstance // hard code here
	if cf == nil {
		token := p.GetDefaultToken()
		log.Print("saved token not found. using ...")
		generation := 1
		rm := NewRowMutation(config.SysTableName, sysLocationKey)
		cf = createColumnFamily(config.SysTableName, sysLocationCF)
		cf.addColumn(NewColumnKV(sysToken, token))
		cf.addColumn(NewColumnKV(sysGeneration, fmt.Sprint(generation)))
		rm.AddCF(cf)
		rm.ApplyE()
		sysMetadata = &StorageMetadata{token, generation}
		return sysMetadata
	}
	// reach here means that we crashed and came back up
	// so we need to bump generation number
	tokenColumn := cf.GetColumn(sysToken)
	token := string(tokenColumn.getValue())
	log.Print("saved token found")

	generation := cf.GetColumn(sysGeneration)
	gen, err := strconv.Atoi(string(generation.getValue()))
	gen++
	if err != nil {
		log.Fatal(err)
	}
	rm := NewRowMutation(config.SysTableName, sysLocationKey)
	cf = createColumnFamily(config.SysTableName, sysLocationCF)
	generation2 := NewColumn(sysGeneration, fmt.Sprint(gen), generation.timestamp()+1, false)
	cf.addColumn(generation2)
	rm.AddCF(cf)
	rm.ApplyE()
	sysMetadata = &StorageMetadata{token, gen}
	return sysMetadata
}

// StorageMetadata stores id and generation
type StorageMetadata struct {
	StorageID  string
	Generation int
}

// GetGeneration return generation for this storage metadata
func (s *StorageMetadata) GetGeneration() int {
	return s.Generation
}

// RowMutationArgs for rm arguments
type RowMutationArgs struct {
	HeaderKey   string
	HeaderValue network.EndPoint
	From        network.EndPoint
	RM          RowMutation
}

// RowMutationReply for rm reply structure
type RowMutationReply struct {
	Result string
	Status bool
}

// DoRowMutation ...
func DoRowMutation(args *RowMutationArgs, reply *RowMutationReply) error {
	utils.LoggerInstance().Printf("enter db.DoRowMutation\n")
	log.Printf("enter db.DoRowMutation\n")
	spew.Printf("args: %+v\n", args)
	rm := args.RM
	if args.HeaderKey == HINT {
		hint := args.HeaderValue
		log.Printf("adding hint for %v\n", hint)
		// add necessary hints to this mutation
		hintedMutation := NewRowMutation(config.SysTableName, rm.TableName)
		hintedMutation.AddHints(rm.RowKey, hint.HostName)
		hintedMutation.ApplyE()
	}
	rm.ApplyE()
	reply.Status = true
	return nil
}

// // Insert dispatches rowmutation to other replicas
// func Insert(rm RowMutation) string {
// 	// endpointMap := GetInstance().getNStorageEndPointMap(rm.rowKey)
// 	// oversimplified: should get endpoint list to write
// 	c, err := rpc.DialHTTP("tcp", "localhost"+":"+"11111")
// 	defer c.Close()
// 	if err != nil {
// 		log.Fatal("dialing:", err)
// 	}
// 	args := RowMutationArgs{}
// 	reply := RowMutationReply{}
// 	args.RM = rm
// 	err = c.Call("StorageService.DoRowMutation", &args, &reply)
// 	if err != nil {
// 		log.Fatal("calling:", err)
// 	}
// 	fmt.Printf("DoRowMutation.Result: %+v\n", reply.Result)
// 	return reply.Result
// }
