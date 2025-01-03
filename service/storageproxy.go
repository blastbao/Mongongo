// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package service

import (
	"encoding/gob"
	"fmt"
	"log"
	"net/rpc"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/DistAlchemist/Mongongo/config"
	"github.com/DistAlchemist/Mongongo/db"
	"github.com/DistAlchemist/Mongongo/network"
	"github.com/DistAlchemist/Mongongo/utils"
)

// RowMutationArgs for rm arguments
type RowMutationArgs struct {
	RM db.RowMutation
}

// RowMutationReply for rm reply structure
type RowMutationReply struct {
	Result string
}

// Insert will apply this row mutation to
// all replicas. (TODO) It will take care of the
// possibility of a replica being down and
// hint the data across to some other replica.
func Insert(rm db.RowMutation) {
	// 根据 RowKey 获取其归属的副本列表
	endpointMap := GetInstance().getNStorageEndPointMap(rm.RowKey)
	gob.Register(db.SuperColumnFactory{})
	gob.Register(db.SuperColumn{})
	// 并发向每个 endpoint 异步发送 RowMutation 请求
	for endpoint := range endpointMap {
		go func(end network.EndPoint) {
			// 建立连接
			c, err := rpc.DialHTTP("tcp", end.HostName+":"+end.Port)
			defer c.Close()
			if err != nil {
				log.Fatal("dialing:", err)
			}
			// 构造请求/响应
			args := RowMutationArgs{rm}
			reply := RowMutationReply{}
			// 调用 rpc
			if err = c.Call("StorageService.DoRowMutation", &args, &reply); err != nil {
				log.Fatal("calling:", err)
			}
			fmt.Printf("DoRowMutation.Result for %v:%v: %+v\n", end.HostName, end.Port, reply.Result)
		}(endpoint)
	}
	return
}

func getUnhintedNodes(endpointMap map[network.EndPoint]network.EndPoint) []network.EndPoint {
	liveEndPoints := make([]network.EndPoint, 0)
	for k, v := range endpointMap {
		if k == v {
			liveEndPoints = append(liveEndPoints, k)
		}
	}
	return liveEndPoints
}

func insertBlocking(rm db.RowMutation, consistencyLevel int) {
	// TODO
	// endpointMap := GetInstance().getHintedStorageEndpointMap(rm.RowKey)
	// messageMap := createWriteMessage(rm, endpointMap)
	// blockFor := consistencyLevel
	// primaryNodes := getUnhintedNodes(endpointMap)
	// if len(primaryNodes) < blockFor {
	// 	log.Fatal("should gurantee blockFor = W live nodes")
	// }
	// reply := db.RowMutationReply{}
	// for endpoint, message := range messageMap {
	// 	log.Printf("insert writing key %v to %v\n", rm.RowKey, endpoint)
	// 	to := endpoint
	// 	client, err := rpc.DialHTTP("tcp", to.HostName+":"+config.StoragePort)
	// 	if err != nil {
	// 		log.Fatal("dialing: ", err)
	// 	}
	// 	client.Call("StorageService.DoRowMutation", &message, &reply)
	// 	log.Printf("row mutation status for %v: %v\n", to, reply)
	// }
}

// 将 RowMutation 操作应用到所有副本节点上，在某些副本节点不可用时使用 hint 机制将数据发送到其他副本节点。
//
// 主要步骤：
//   - 获取需要复制数据的 N 个节点。
//   - 构建写入请求。
//   - 数据发送到各个节点。
func insert(rm db.RowMutation) {
	log.Printf("enter insert ...")
	// use this method to have this RowMutation applied
	// across all replicas. This method will take care
	// of the possiblity of a replica being down and
	// hint the data across to some other replica.

	// 1. get the N nodes from storage service where the
	// data needs to be replicated
	// 2. construct a message for write
	// 3. send them asynchronously to the replicas
	// startTime := utils.CurrentTimeMillis()
	// this is the ZERO consistency level, so user doesn't
	// care if we don't really have N destinations available.

	// 获取需要存储数据的 N 个节点。
	// getHintedStorageEndpointMap 方法根据行键（rm.RowKey）来确定应该将数据写入哪些副本。
	// 如果某个副本不可用，可能会使用提示（hinting）机制，将数据“提示”到其他可用副本。
	endpointMap := GetInstance().getHintedStorageEndpointMap(rm.RowKey)
	messageMap := createWriteMessage(rm, endpointMap)
	reply := db.RowMutationReply{}
	for endpoint, message := range messageMap {
		utils.LoggerInstance().Printf("enter storageproxy.insert\n")
		log.Printf("insert writing key %v to %v\n", rm.RowKey, endpoint)
		to := endpoint
		// connect
		client, err := rpc.DialHTTP("tcp", to.HostName+":"+config.StoragePort)
		if err != nil {
			log.Fatal("dialing: ", err)
		}
		// call
		err = client.Call("StorageService.DoRowMutation", &message, &reply)
		if err != nil {
			log.Print(err)
		}
		log.Printf("row mutation status for %v: %v\n", to, reply)
	}

}

// WriteMessage ...
// type WriteMessage struct {
// 	HeaderKey   string
// 	HeaderValue network.EndPoint
// 	From        network.EndPoint
// 	RM          db.RowMutation
// }

func createWriteMessage(rm db.RowMutation, endpointMap map[network.EndPoint]network.EndPoint) map[network.EndPoint]db.RowMutationArgs {
	messageMap := make(map[network.EndPoint]db.RowMutationArgs)
	message := db.RowMutationArgs{}
	message.RM = rm
	message.From = *GetInstance().tcpAddr
	for target, hint := range endpointMap {
		if target != hint {
			hintedMessage := db.RowMutationArgs{}
			hintedMessage.HeaderKey = db.HINT
			hintedMessage.HeaderValue = hint
			hintedMessage.From = *GetInstance().tcpAddr
			hintedMessage.RM = rm
			log.Printf("sending the hint of %v to %v \n", hint.HostName, target.HostName)
			messageMap[target] = hintedMessage
		} else {
			messageMap[target] = message
		}
	}
	return messageMap
}

func readProtocol(commands []db.ReadCommand, consistencyLevel int) []*db.Row {
	// performs the actual reading of a row out of the StorageService,
	// fetching a specific set of column names from a given column family
	rows := make([]*db.Row, 0)
	if consistencyLevel == 1 {
		localCommands := make([]db.ReadCommand, 0)
		remoteCommands := make([]db.ReadCommand, 0)
		for _, command := range commands {
			endpoints := GetInstance().getReadStorageEndPoints(command.GetKey())
			_, foundlocal := endpoints[*GetInstance().tcpAddr]
			if foundlocal && GetInstance().isBootstrapMode == false {
				localCommands = append(localCommands, command)
			} else {
				remoteCommands = append(remoteCommands, command)
			}
		}
		if len(localCommands) > 0 {
			rows = append(rows, weakReadLocal(localCommands)...)
		} else {
			rows = append(rows, weakReadRemote(remoteCommands)...)
		}
	} else {
		if consistencyLevel != 2 { // Quorum
			rows = strongRead(commands)
		}
	}
	return rows
}

func remove(list []network.EndPoint, elem network.EndPoint) {
	idx := 0
	var e interface{}
	for idx, e = range list {
		if e == elem {
			break
		}
	}
	list = append(list[:idx], list[idx+1:]...)
}

// weakReadLocal 函数的目的是执行一个弱一致性的本地读取操作，执行流程：
//   - 获取存活的副本：根据读取命令中的行键，获取与之关联的存活的副本节点列表，排除本地节点。
//   - 从本地存储读取数据：首先尝试从本地存储读取数据，如果数据存在（非 nil），则直接返回该数据。
//   - 从其他副本读取数据：如果本地没有数据，则会尝试从其他副本读取数据，直到找到有效的数据。
//   - 一致性检查：如果从其他副本读取到数据，并且开启了配置中的一致性检查，函数会触发一致性检查和修复。
//   - 返回结果：最终，函数会返回读取到的有效数据行。
func weakReadLocal(commands []db.ReadCommand) []*db.Row {
	// this function executes the read protocol locally
	// and should be used only if consistency is not a
	// concern. read the data from the local disk and
	// return if the row is NOT NULL. if the data is NULL
	// do the read from one of the other replicas (in the
	// same data center if possible) till we get the data.
	// in the event we get the data we perform consistency
	// checks and figure out if any repairs need to be done
	// to the replicas
	rows := make([]*db.Row, 0)
	for _, command := range commands {
		endpoints := GetInstance().getLiveReadStorageEndPoints(command.GetKey())
		// remove the local storage endpoint from the list
		remove(endpoints, *GetInstance().tcpAddr)
		spew.Printf("\tweakreadlocal reading %#+v\n\n", command)
		table := db.OpenTable(command.GetTable())
		spew.Printf("\ttable: %#+v\n\n", table)
		row := command.GetRow(table)
		spew.Printf("\trow: %#+v\n\n", row)
		if row != nil {
			rows = append(rows, row)
		}

		// do the consistency checks in the background and return
		// the not NILL row
		if len(endpoints) > 0 && config.DoConsistencyCheck {
			GetInstance().doConsistencyCheck(row, endpoints, command)
		}
	}

	return rows
}

func weakReadRemote(commands []db.ReadCommand) []*db.Row {
	// read the data from one replica. if there is no reply,
	// read the data from another. in the event we get the
	// data we perform consistency checks and figure out if
	// any repairs need to be done to the replicas.
	log.Printf("weakrealremote reading %v\n", commands)
	rows := make([]*db.Row, 0)
	divCalls := make([]*rpc.Call, 0)
	replys := make([]*db.RowReadReply, 0)
	endpoints := make([]network.EndPoint, 0)
	for _, command := range commands {
		endpoint := GetInstance().findSuitableEndPoint(command.GetKey())
		endpoints = append(endpoints, endpoint)
		message := db.RowReadArgs{}
		message.From = *GetInstance().tcpAddr
		message.RCommand = command
		message.HeaderKey = db.DoREPAIR
		reply := db.RowReadReply{}
		to := endpoint
		client, err := rpc.DialHTTP("tcp", to.HostName+":"+config.StoragePort)
		if err != nil {
			log.Fatal("dialing: ", err)
		}
		divCall := client.Go("StorageService.DoRowRead", &message, &reply, nil)
		replys = append(replys, &reply)
		divCalls = append(divCalls, divCall)
	}
	for idx, divCall := range divCalls {
		select {
		case _ = <-divCall.Done:
			if replys[idx].R != nil {
				rows = append(rows, replys[idx].R)
			}
		case <-time.After(time.Duration(config.RPCTimeoutInMillis) * time.Millisecond):
			log.Printf("timeout calling %v for command %v\n", endpoints[idx], commands[idx])
		}
	}
	return rows
}

func strongRead(commands []db.ReadCommand) []*db.Row {
	// this function executes the read protocol
	// 1. get the N nodes from storage service where
	//    the data needs to be replicated
	// 2. construct a message for read/write
	// 3. set one of the messages to get the data and
	//    the rest to get the digest
	// 4. send message to all the nodes above
	// 5. wait for response from at least X nodes
	//    where X <= N and the data node
	// 6. if the digest matches return the data
	// 7. else carry out read repair by getting data from
	//    all the nodes
	// 8. return success
	// TODO
	return make([]*db.Row, 0)
}
