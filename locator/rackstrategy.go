// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package locator

import (
	"log"
	"sort"

	"github.com/DistAlchemist/Mongongo/config"
	"github.com/DistAlchemist/Mongongo/gms"
	"github.com/DistAlchemist/Mongongo/network"
)

// IStrategy is the interface for rack strategy
type IStrategy interface {
	GetStorageEndPoints(token string) []network.EndPoint
	GetTokenEndPointMap() map[string]network.EndPoint
	GetToken(endPoint network.EndPoint) string
	GetReadStorageEndPoints(token string) map[network.EndPoint]bool
	GetWriteStorageEndPoints(token string) map[network.EndPoint]bool
	// GetHintedStorageEndPoints(token *big.Int) map[network.EndPoint]network.EndPoint
}

// RackStrategy implements IStrategy, adds its own methods
type RackStrategy struct {
	I IStrategy
}

// GetHintedStorageEndPoints returns a hinted map.
// The key is the endpoint on which the data is being placed.
// The value is the endpoint which is in the top N.
// Currently it is the map of top N to live nodes.
func (r *RackStrategy) GetHintedStorageEndPoints(token string) map[network.EndPoint]network.EndPoint {
	return r.getHintedMapForEndpoints(r.I.GetWriteStorageEndPoints(token))
	// topN := r.I.GetStorageEndPoints(token) // N is # of replicas, see config.ReplicationFactor
	// m := make(map[network.EndPoint]network.EndPoint)
	// liveList := make([]network.EndPoint, 0)
	// for _, endPoint := range topN {
	// 	if gms.GetFailureDetector().IsAlive(endPoint) {
	// 		m[endPoint] = endPoint
	// 		liveList = append(liveList, endPoint)
	// 	} else {
	// 		nxt, ok := r.getNextAvailableEndPoint(endPoint, topN, liveList)
	// 		if !ok {
	// 			m[nxt] = endPoint // map: alive -> origin
	// 			liveList = append(liveList, nxt)
	// 		} else {
	// 			log.Printf("Unable to find a live endpoint, we might run out of live endpoints! dangerous!\n")
	// 		}
	// 	}
	// }
	// return m
}

func (r *RackStrategy) getHintedMapForEndpoints(topN map[network.EndPoint]bool) map[network.EndPoint]network.EndPoint {
	// 存储当前存活的所有端点。
	liveList := make([]network.EndPoint, 0)
	m := make(map[network.EndPoint]network.EndPoint)
	for node := range topN {
		if gms.GetFailureDetector().IsAlive(node) {
			// 如果 node 活跃，将其保存到 liveList 中
			m[node] = node
			liveList = append(liveList, node)
		} else {
			// 如果 node 失效，选择一个 backup node
			tList := make([]network.EndPoint, 0)
			for k := range topN {
				tList = append(tList, k)
			}
			endPoint, ok := r.getNextAvailableEndPoint(node, tList, liveList)
			if ok {
				m[endPoint] = node // map hinted node => origin node
				liveList = append(liveList, endPoint)
			} else {
				log.Printf("unable to find a live endpoint we might be out of live nodes\n")
			}

		}
	}
	return m
}

// 于在网络中查找下一个可用的端点。
func (r *RackStrategy) getNextAvailableEndPoint(
	startPoint network.EndPoint,
	topN []network.EndPoint,
	liveNodes []network.EndPoint,
) (network.EndPoint, bool) {
	// 获取每个 token 与对应端点 (EndPoint) 的映射关系
	tokenToEndPointMap := r.I.GetTokenEndPointMap()
	// 对 tokens 进行排序
	tokens := make([]string, 0, len(tokenToEndPointMap))
	for k := range tokenToEndPointMap {
		tokens = append(tokens, k)
	}
	sort.Strings(tokens)
	// 获取 startPoint 的 token 在 tokens 中的下标 idx
	token := r.I.GetToken(startPoint)
	idx := sort.SearchStrings(tokens, token)
	totalNodes := len(tokens)
	if idx == totalNodes {
		idx = 0
	}
	// 从 idx 开始，选择一个可用的 ep 并返回
	startIdx := (idx + 1) % totalNodes
	var endPoint network.EndPoint // 保存找到的可用节点
	flag := false                 // 尚未找到可用节点
	for i, count := startIdx, 1; count < totalNodes; count, i = count+1, (i+1)%totalNodes {
		tmp := tokenToEndPointMap[tokens[i]]
		if contains(topN, tmp) {
			continue
		}
		if contains(liveNodes, tmp) {
			continue
		}
		if !gms.GetFailureDetector().IsAlive(tmp) {
			continue
		}
		endPoint, flag = tmp, true // 找到可用节点
	}
	return endPoint, flag
}

// This function change endpoints' port to storage port
func retrofitPorts(list []network.EndPoint) {
	for _, tmp := range list {
		tmp.Port = config.StoragePort
	}
}

// // GetStorageEndPoints return endpoints that correspond to a given token.
// func (r *RackStrategy) GetStorageEndPoints(token *big.Int) map[network.EndPoint]network.EndPoint {
// 	return make(map[network.EndPoint]network.EndPoint)
// }

// GetReadStorageEndPoints ...
func (r *RackStrategy) GetReadStorageEndPoints(token string) map[network.EndPoint]bool {
	// return map[network.EndPoint]bool{}
	return r.I.GetReadStorageEndPoints(token)
}
