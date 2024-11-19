// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package gms

import (
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/DistAlchemist/Mongongo/config"
	"github.com/DistAlchemist/Mongongo/network"
)

var (
	// GIntervalInMillis is the time period for
	// gossiper to gossip
	GIntervalInMillis = 1000
	gossiper          *Gossiper
)

// Gossiper is responsible for Gossiping information for
// the local endpoint. It maintains the list of live and
// dead endpoints. It will periodically (every 1 sec.)
// chooses a random node and initiates a round of Gossip
// with it.
// A round of Gossip involves 3 rounds of messaging:
// If A wants to initiate a round of Gossip with B:
//  1. A -> B using GossipDigestSynMessage.
//  2. B -> A using GossipDigestAckMessage.
//  3. A -> B using GossipDigestAck2Message.
//
// When this module heads from one of the above messages,
// it will update the FailureDetector with the liveness
// information.
type Gossiper struct {
	MaxGossipPacketSize  int
	GossipStage          string
	JoinVerbHandler      string
	GossipDigestSynVerb  string
	GossipDigestAckVerb  string
	GossipDigestAck2Verb string
	intervalInMillis     int

	localEndPoint        *network.EndPoint
	aVeryLongTime        int64
	preIdx               int                       // index used previously
	rrIdx                int                       // round-robin index through live endpoint set
	liveEndpoints        map[network.EndPoint]bool // 活动节点
	unreachableEndpoints map[network.EndPoint]bool // 不可达节点
	seeds                map[network.EndPoint]bool // 种子节点
	endPointStateMap     map[network.EndPoint]*EndPointState
	subscribers          []IEndPointStateChangeSubscriber
	rnd                  *rand.Rand
	mu                   sync.Mutex
}

// NewGossiper creates a new Gossiper
func NewGossiper() *Gossiper {
	g := &Gossiper{}
	g.MaxGossipPacketSize = 1428
	g.GossipStage = "GS" // abbr for Gossip Stage
	g.JoinVerbHandler = "JVH"
	g.GossipDigestSynVerb = "GSV"
	g.GossipDigestAckVerb = "GAV"
	g.GossipDigestAck2Verb = "GA2V"
	g.intervalInMillis = 1000
	g.aVeryLongTime = 259200 * 1000
	g.preIdx = 0
	g.rrIdx = 0
	g.liveEndpoints = make(map[network.EndPoint]bool)
	g.unreachableEndpoints = make(map[network.EndPoint]bool)
	g.seeds = make(map[network.EndPoint]bool)
	g.endPointStateMap = make(map[network.EndPoint]*EndPointState)
	g.subscribers = make([]IEndPointStateChangeSubscriber, 0)
	s := rand.NewSource(time.Now().UnixNano() / int64(time.Millisecond))
	g.rnd = rand.New(s)
	GetFailureDetector().RegisterEventListener(g)
	return g
}

// GetGossiper creates a new Gossiper if not exists
func GetGossiper() *Gossiper {
	if gossiper == nil {
		gossiper = NewGossiper()
	}
	return gossiper
}

// Start will start gossiper on control port
func (g *Gossiper) Start(generation int) {
	log.Printf("starting gossiper...\n")
	g.localEndPoint = network.NewEndPoint(config.ControlPort)
	// get the seeds from the config and initialize them.
	seedHosts := config.Seeds
	for seedHost := range seedHosts {
		seed := network.NewEndPointH(seedHost, config.ControlPort)
		if *seed == *g.localEndPoint {
			// already this host
			continue
		}
		g.seeds[*seed] = true // add seed host
	}

	// initialize the heartbeat state for this localEndPoint
	localState, ok := g.endPointStateMap[*g.localEndPoint]
	if !ok {
		// localState doesn't exist
		hbState := NewHeartBeatState(generation, 0)
		localState = NewEndPointState(hbState)
		localState.SetAlive(true)
		localState.SetGossiper(true)
		g.endPointStateMap[*g.localEndPoint] = localState
	}
	g.startControlServer()
	go g.RunTimerTask()
}

func (g *Gossiper) startControlServer() {
	serv := rpc.NewServer()
	serv.Register(g)
	// ===== workaround ==========
	oldMux := http.DefaultServeMux
	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	// ===========================
	serv.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	// ===== workaround ==========
	http.DefaultServeMux = oldMux
	// ===========================

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	addr := hostname + ":" + config.ControlPort
	l, e := net.Listen("tcp", addr)
	log.Printf("ControlServer listening to %v\n", addr)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	go http.Serve(l, mux)
}

// RunTimerTask starts the periodic task for a gossiper
// 定期运行以执行 Gossip 过程
func (g *Gossiper) RunTimerTask() {
	for {
		g.runTask()
		time.Sleep(time.Millisecond * time.Duration(g.intervalInMillis))
	}
}

// 创建 Gossip 摘要，并向活动和不可达成员以及种子节点发送 Gossip 消息。
func (g *Gossiper) runTask() {
	log.Printf("gossiper is running the timer task...\n")
	g.mu.Lock()
	defer g.mu.Unlock()

	// 更新本端点心跳
	g.endPointStateMap[*g.localEndPoint].GetHeartBeatState().UpdateHeartBeat()

	// 生成一个包含本地端点和活跃端点的随机化 gossip 摘要
	gDigests := make([]*GossipDigest, 0)
	g.makeRandomGossipDigest(gDigests)

	// 将 gossip 摘要转换成一个 GossipDigestSynMessage 消息：
	//  - 发送给一个随机的活跃端点
	//	- 发送给一个随机的不可达端点（概率）
	//  - 发送给一个随机的种子端点（条件&概率）
	//	- 移除那些已经长时间不可达的端点
	if len(gDigests) > 0 {
		message := g.makeGossipDigestSynMessage(gDigests)
		// gossip to some random live member
		bVal := g.doGossipToLiveMember(message)
		// gossip to some unreachable member with some
		// probability to check if he is back up
		g.doGossipToUnreachableMember(message)
		// gossip to the seed
		if bVal == false {
			g.doGossipToSeed(message)
		}
		log.Printf("Performing status check ...")
		g.doStatusCheck()
	}
}

func getCurrentTimeInMillis() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// 检查和更新网络中端点的状态，移除那些已经长时间不可达的端点。
func (g *Gossiper) doStatusCheck() {
	for endpoint := range g.endPointStateMap {
		if endpoint == *g.localEndPoint {
			continue
		}
		GetFailureDetector().interpret(endpoint)
		epState := g.endPointStateMap[endpoint]
		if epState == nil {
			continue
		}
		duration := getCurrentTimeInMillis() - epState.updateTimestamp
		if epState.isAlive == false && duration > g.aVeryLongTime {
			g.evictFromMembership(endpoint)
		}
	}
}

func (g *Gossiper) evictFromMembership(endpoint network.EndPoint) {
	// removes the endpoint from unreachable endpoint set
	delete(g.unreachableEndpoints, endpoint)
}

func (g *Gossiper) doGossipToSeed(message *GossipDigestSynArgs) {
	// gossip to a seed for facilitating partition healing
	size := len(g.seeds)
	if size == 0 {
		return
	}

	// 如果本地端点是唯一的种子端点（即 size == 1 && ok），那么直接返回，不进行 gossip 操作。
	_, ok := g.seeds[*g.localEndPoint]
	if size == 1 && ok {
		return
	}
	log.Printf("gossip to seeds: %v\n", g.seeds)

	// 当没有活跃端点时，直接向种子端点发送 gossip 消息，否则基于概率决定是否向种子端点发送。
	if len(g.liveEndpoints) == 0 {
		log.Printf("# of live endpoints is 0, gossip to seed\n")
		g.sendGossip(message, g.seeds)
	} else {
		// 概率值 prob 是种子端点数量与活跃端点和不可达端点数量之和的比例，用来确定是否将消息发送到种子端点。
		prob := float64(len(g.seeds)) / float64(len(g.liveEndpoints)+len(g.unreachableEndpoints))
		randDbl := g.rnd.Float64()
		log.Printf("gossip with probability: %v <= %v?\n", randDbl, prob)
		if randDbl <= prob {
			g.sendGossip(message, g.seeds)
		}
	}
}

// 向不可达节点发送 gossip 消息，检查这些节点是否已经恢复或重连到网络。
func (g *Gossiper) doGossipToUnreachableMember(message *GossipDigestSynArgs) {
	// sends a gossip message to an unreachable member
	liveEndPoints := len(g.liveEndpoints)
	unreachableEndPoints := len(g.unreachableEndpoints)
	if unreachableEndPoints == 0 {
		return
	}
	// 概率值 prob 基于不可达端点的数量与活跃端点数量的比值。
	prob := float64(unreachableEndPoints) / (float64(liveEndPoints + 1))
	randDbl := g.rnd.Float64()
	if randDbl < prob {
		g.sendGossip(message, g.unreachableEndpoints)
	}
}

func (g *Gossiper) doGossipToLiveMember(message *GossipDigestSynArgs) bool {
	size := len(g.liveEndpoints)
	if size == 0 {
		return false
	}
	return g.sendGossip(message, g.liveEndpoints)
}

// 将一个 GossipDigestSynArgs 消息发送到一个随机选择的活跃网络端点。
func (g *Gossiper) sendGossip(message *GossipDigestSynArgs, epSet map[network.EndPoint]bool) bool {
	size := len(g.liveEndpoints)
	liveEndPoints := make([]network.EndPoint, size)
	for ep := range epSet {
		liveEndPoints = append(liveEndPoints, ep)
	}

	// 从 epSet 中随机选择一个端点
	var index int
	if size == 1 {
		index = 0
	} else {
		index = g.rnd.Intn(size) // generate a random number in [0,size)
	}
	to := liveEndPoints[index]
	log.Printf("Sending a GossipDigestSynMessage to %v ...\n", to)

	// 异步调用 RPC 发送 Gossip 消息
	reply := &GossipDigestSynReply{}
	client, err := rpc.DialHTTP("udp", to.HostName+":"+config.ControlPort)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	client.Go("Gossiper.OnGossipDigestSyn", message, reply, nil)

	// 检查是否为种子端点，如果是种子端点返回 true，否则返回 false
	_, ok := g.seeds[to]
	return ok
}

// 生成一个包含多个端点（包括本地端点和活跃端点）信息的 gossip 摘要列表。
//   - 对于每个端点（包括本地端点和活跃端点），通过查询状态生成相应的 generation 和 maxVersion，并添加到 gDigests 列表中。
//   - 因为 map 的遍历是随机的，这里相当于对活跃端点进行了 shuffle ，这样可以使节点间交换的信息更加多样化，增强了 gossip 协议的效果。
func (g *Gossiper) makeRandomGossipDigest(gDigests []*GossipDigest) {
	// the gossip digest is built based on randomization rather than
	// just looping through the collection of live endpoints.
	// 获取本端状态
	epState := g.endPointStateMap[*g.localEndPoint]
	generation := int(epState.GetHeartBeatState().generation)
	maxVersion := getMaxEndPointStateVersion(epState)
	// 创建本端点的 gossip 摘要，添加到 gDigests 中
	gDigests = append(gDigests, NewGossipDigest(*g.localEndPoint, generation, maxVersion))
	// map is unsorted, so we omit the shuffle here
	// 遍历活跃的端点并生成 gossip 摘要，添加到 gDigests 中
	for liveEndPoint := range g.liveEndpoints {
		epState, ok := g.endPointStateMap[liveEndPoint]
		if ok {
			generation = int(epState.GetHeartBeatState().generation)
			maxVersion = getMaxEndPointStateVersion(epState)
			gDigests = append(gDigests, NewGossipDigest(liveEndPoint, generation, maxVersion))
		} else {
			gDigests = append(gDigests, NewGossipDigest(liveEndPoint, 0, 0))
		}
	}
}

// 获取一个端点状态（EndPointState）的最大版本号。
//   - 首先，它从心跳状态（HeartBeatState）中获取版本号，并添加到版本号列表中。
//   - 然后，遍历 applicationState 提取每个 app state 的版本号并添加到列表。
//   - 最后，所有版本号被排序，并返回最大的版本号。
func getMaxEndPointStateVersion(epState *EndPointState) int {
	versions := make([]int, 0)
	versions = append(versions, int(epState.GetHeartBeatState().GetVersion()))
	appStateMap := epState.applicationState
	for key := range appStateMap {
		stateVersion := appStateMap[key].version
		versions = append(versions, int(stateVersion))
	}
	sort.Ints(versions)
	return versions[len(versions)-1]
}

// GetEndPointStateForEndPoint returns state for given endpoint.
func (g *Gossiper) GetEndPointStateForEndPoint(ep network.EndPoint) *EndPointState {
	return g.endPointStateMap[ep]
}

// Register register end point state change subscriber
func (g *Gossiper) Register(subscriber IEndPointStateChangeSubscriber) {
	g.subscribers = append(g.subscribers, subscriber)
}

// Convict implements IFailureDetectionEventListener interface
// it is invoked by the Failure Detector when it convicts an end point
func (g *Gossiper) Convict(endpoint network.EndPoint) {
	// TODO
}

// Suspect implements IFailureDetectionEventListener interface
// it is invoked by the Failure Detector when it suspects an end point
func (g *Gossiper) Suspect(endpoint network.EndPoint) {
	epState := g.endPointStateMap[endpoint]
	if epState.isAlive {
		log.Printf("EndPoint %v is not dead\n", endpoint)
		g.isAlive(endpoint, epState, false)
		// notify an endpoint is dead to interested parties
		deltaState := NewEndPointState(epState.GetHeartBeatState())
		g.doNotifications(endpoint, deltaState)
	}
}

func (g *Gossiper) doNotifications(addr network.EndPoint, epState *EndPointState) {
	for _, subscriber := range g.subscribers {
		subscriber.OnChange(addr, epState)
	}
}

// isAlive 用于更新一个端点的存活状态，并根据状态更新 Gossiper 内部的存活和不可达端点列表。
func (g *Gossiper) isAlive(addr network.EndPoint, epState *EndPointState, value bool) {
	epState.SetAlive(value)
	if value {
		g.liveEndpoints[addr] = true
		delete(g.unreachableEndpoints, addr)
	} else {
		delete(g.liveEndpoints, addr)
		g.unreachableEndpoints[addr] = true
	}
	if epState.isAGossiper {
		return
	}
	epState.SetGossiper(true)
}

// AddApplicationState ...
func (g *Gossiper) AddApplicationState(key string, appState *ApplicationState) {
	epState := g.endPointStateMap[*g.localEndPoint]
	if epState != nil {
		epState.AddApplicationState(key, appState)
	}
}

func (g *Gossiper) notifyFailureDetector(gDigests []*GossipDigest) {
	fd := GetFailureDetector()
	for _, gDigest := range gDigests {
		localEndPointState := g.endPointStateMap[gDigest.endPoint]
		// if the local endpoint state exists then report
		// to the failure detector only if the versions workout
		if localEndPointState == nil {
			continue
		}
		localGeneration := g.endPointStateMap[gDigest.endPoint].GetHeartBeatState().generation
		remoteGeneration := gDigest.generation
		if remoteGeneration > localGeneration {
			fd.report(gDigest.endPoint)
			continue
		}
		if remoteGeneration == localGeneration {
			localVersion := getMaxEndPointStateVersion(localEndPointState)
			remoteVersion := gDigest.maxVersion
			if remoteVersion > localVersion {
				fd.report(gDigest.endPoint)
			}
		}

	}
}

func (g *Gossiper) applyStateLocally(epStateMap map[network.EndPoint]*EndPointState) {
	for ep := range epStateMap {
		if ep == *g.localEndPoint {
			continue
		}
		localEpStatePtr := g.endPointStateMap[ep]
		remoteState := epStateMap[ep]
		// if state does not exist, just add it.
		// if it does, then add it only if the version
		// of the remote copy is greater than the local copy
		if localEpStatePtr != nil {
			localGeneration := localEpStatePtr.GetHeartBeatState().generation
			remoteGeneration := remoteState.GetHeartBeatState().generation
			if remoteGeneration > localGeneration {
				g.handleNewJoin(ep, remoteState)
			} else if remoteGeneration == localGeneration {
				// manage the membership state
				localMaxVersion := getMaxEndPointStateVersion(localEpStatePtr)
				remoteMaxVersion := getMaxEndPointStateVersion(remoteState)
				if remoteMaxVersion > localMaxVersion {
					g.markAlive(ep, localEpStatePtr)
					g.applyHeartBeatStateLocally(ep, localEpStatePtr, remoteState)
					// apply ApplicationState
					g.applyApplicationStateLocally(ep, localEpStatePtr, remoteState)
				}
			}
		} else {
			g.handleNewJoin(ep, remoteState)
		}
	}
}

func (g *Gossiper) applyApplicationStateLocally(addr network.EndPoint, localStatePtr *EndPointState, remoteStatePtr *EndPointState) {
	localAppStateMap := localStatePtr.applicationState
	remoteAppStateMap := remoteStatePtr.applicationState
	for remoteKey, remoteAppState := range remoteAppStateMap {
		localAppState := localAppStateMap[remoteKey]
		// if state doesn't exist locally for this key then
		// just apply it
		if localAppState == nil {
			localStatePtr.AddApplicationState(remoteKey, remoteAppState)
			// notify interested parties of endpoint state change
			deltaState := NewEndPointState(localStatePtr.GetHeartBeatState())
			deltaState.AddApplicationState(remoteKey, remoteAppState)
			g.doNotifications(addr, deltaState)
			continue
		}
		remoteGeneration := remoteStatePtr.GetHeartBeatState().generation
		localGeneration := localStatePtr.GetHeartBeatState().generation
		// if the remoteGeneration is greater than localGeneration
		// then apply state blindly
		if remoteGeneration > localGeneration {
			localStatePtr.AddApplicationState(remoteKey, remoteAppState)
			// notify interested parties of endpoint state change
			deltaState := NewEndPointState(localStatePtr.GetHeartBeatState())
			deltaState.AddApplicationState(remoteKey, remoteAppState)
			g.doNotifications(addr, deltaState)
			continue
		}
		// if the generation are the same then apply state
		// if the remote version is greater than local version
		if remoteGeneration == localGeneration {
			remoteVersion := remoteAppState.GetStateVersion()
			localVersion := localAppState.GetStateVersion()
			if remoteVersion > localVersion {
				localStatePtr.AddApplicationState(remoteKey, remoteAppState)
				// notify interested parties of endpoint state change
				deltaState := NewEndPointState(localStatePtr.GetHeartBeatState())
				deltaState.AddApplicationState(remoteKey, remoteAppState)
				g.doNotifications(addr, deltaState)
			}
		}
	}
}

func (g *Gossiper) applyHeartBeatStateLocally(addr network.EndPoint, localState *EndPointState,
	remoteState *EndPointState) {
	localHbState := localState.GetHeartBeatState()
	remoteHbState := remoteState.GetHeartBeatState()
	if remoteHbState.generation > localHbState.generation {
		g.markAlive(addr, localState)
		localState.SetHeartBeatState(remoteHbState)
	}
	if localHbState.generation == remoteHbState.generation {
		if remoteHbState.GetVersion() > localHbState.GetVersion() {
			oldVersion := localHbState.GetVersion()
			localState.SetHeartBeatState(remoteHbState)
			log.Printf("Updating heartbeat state version to %v from %v for %v ...\n",
				localState.GetHeartBeatState().GetVersion(),
				oldVersion, addr)
		}
	}

}

func (g *Gossiper) markAlive(addr network.EndPoint, localState *EndPointState) {
	log.Printf("marking as alive %v\n", addr)
	if localState.isAlive == false {
		g.isAlive(addr, localState, true)
		log.Printf("Endpoint %v is now UP\n", addr)
	}
}

func (g *Gossiper) handleNewJoin(ep network.EndPoint, epState *EndPointState) {
	log.Printf("Node %v has now joined\n", ep)
	// mark this endpoint as "live"
	g.endPointStateMap[ep] = epState
	g.isAlive(ep, epState, true)
	// notofy interested parties about state change
	g.doNotifications(ep, epState)
}

func (g *Gossiper) notifyFailureDetectorM(remoteEpStateMap map[network.EndPoint]*EndPointState) {
	fd := GetFailureDetector()
	for endpoint := range remoteEpStateMap {
		remoteEndPointState := remoteEpStateMap[endpoint]
		localEndPointState := g.endPointStateMap[endpoint]
		// if the local endpoint state exists then report to the
		// FD only if the versions workout
		if localEndPointState != nil {
			localGeneration := localEndPointState.GetHeartBeatState().generation
			remoteGeneration := remoteEndPointState.GetHeartBeatState().generation
			if remoteGeneration > localGeneration {
				fd.report(endpoint)
				continue
			}
			if remoteGeneration == localGeneration {
				localVersion := getMaxEndPointStateVersion(localEndPointState)
				remoteVersion := remoteEndPointState.GetHeartBeatState().GetVersion()
				if remoteVersion > int32(localVersion) {
					fd.report(endpoint)
				}
			}
		}
	}
}

// ByDigest ...
type ByDigest []*GossipDigest

// Len ...
func (p ByDigest) Len() int {
	return len(p)
}

// Less ...
func (p ByDigest) Less(i, j int) bool {
	if p[i].generation != p[j].generation {
		return p[i].generation < p[j].generation
	}
	return p[i].maxVersion < p[j].maxVersion
}

// Swap ...
func (p ByDigest) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (g *Gossiper) doSort(gDigestList []*GossipDigest) {
	// First construct a map whose key is the endpoint in the
	// GossipDigest and the value is the GossipDigest itself.
	// Then build a list of version differences i.e. difference
	// between the version in the GossipDigest and the version
	// in the local state for a given EndPoint. Sort this list.
	// Now loop through the sorted list and retrieve the GossipDigest
	// corresponding to the endpoint from the map that was initially
	// constructed.
	// 1. construct a map of endpoint to GossipDigest
	epToDigest := make(map[network.EndPoint]*GossipDigest)
	for _, gDigest := range gDigestList {
		epToDigest[gDigest.endPoint] = gDigest
	}
	// 2. build version differences. These digests have their
	// own maxVersion set to the difference of the version of
	// the local EndPointState and the version found in the
	// GossipDigest.
	diffDigest := make([]*GossipDigest, 0)
	for _, gDigest := range gDigestList {
		ep := gDigest.endPoint
		epState := g.GetEndPointStateForEndPoint(ep)
		version := 0
		if epState != nil {
			version = getMaxEndPointStateVersion(epState)
		}
		diffVersion := version - gDigest.maxVersion
		if diffVersion < 0 {
			diffVersion *= -1
		}
		diffDigest = append(diffDigest, NewGossipDigest(ep, gDigest.generation, diffVersion))
	}
	gDigestList = make([]*GossipDigest, 0)
	sort.Sort(ByDigest(diffDigest))
	size := len(diffDigest)
	// 3. report the digest in descending order. This takes
	// care of the endpoints that are far behind w.r.t this
	// local endpoint
	for i := size - 1; i >= 0; i-- {
		gDigestList = append(gDigestList, epToDigest[diffDigest[i].endPoint])
	}
}

func (g *Gossiper) examineGossiper(gDigestList []*GossipDigest,
	deltaGossipDigestList []*GossipDigest, deltaEpStateMap map[network.EndPoint]*EndPointState) {
	// this method is used to figure the state that
	// the Gossiper has but Gossipee doesn't. the
	// delta digests and the delta state are built up.
	for _, gDigest := range gDigestList {
		remoteGeneration := gDigest.generation
		maxRemoteVersion := gDigest.maxVersion
		// get state associated with the end point in digest
		epStatePtr := g.endPointStateMap[gDigest.endPoint]
		// here we need to fire a GossipDigestAckMessage.
		// if we have some data associated with this
		// endpoint locally then we follow the "if"
		// path of the logic. If we have absolutely
		// nothing for this endpoint we need to request
		// all the data for this endpoint
		if epStatePtr != nil {
			localGeneration := epStatePtr.GetHeartBeatState().generation
			// get the max version of all keys in
			// the state associated with this endpoint
			maxLocalVersion := getMaxEndPointStateVersion(epStatePtr)
			if remoteGeneration == localGeneration && maxRemoteVersion == maxLocalVersion {
				continue
			}
			if remoteGeneration > localGeneration {
				// we request everything from the gossiper
				g.requestAll(gDigest, deltaGossipDigestList, remoteGeneration)
			}
			if remoteGeneration < localGeneration {
				// send all data with generation = local generation and version > 0
				g.sendAll(gDigest, deltaEpStateMap, 0)
			}
			if remoteGeneration == localGeneration {
				// if the max remote version is greater then we request the
				// remote endpoint send us all the data for this endpoint with
				// version greater than the max version number we have locally
				// for this endpoint.
				// if the max remote version less, then we send all the data
				// we have locally for this endpoint with verson greater than
				// the max remote version.
				if maxRemoteVersion > maxLocalVersion {
					deltaGossipDigestList = append(deltaGossipDigestList,
						NewGossipDigest(gDigest.endPoint, remoteGeneration, maxLocalVersion))
				}
				if maxRemoteVersion < maxLocalVersion {
					// send all data with generation = local generation and
					// version > maxRemoteVersion
					g.sendAll(gDigest, deltaEpStateMap, maxRemoteVersion)
				}
			}
		} else {
			// we are here since we have no data for this endpoint locally
			// so request everything.
			g.requestAll(gDigest, deltaGossipDigestList, remoteGeneration)
		}
	}
}

func (g *Gossiper) requestAll(gDigest *GossipDigest, deltaGossipDigestList []*GossipDigest, remoteGeneration int) {
	// request all the state for the endpoint in the gDigest
	// we are here since we have no data for this endpoint
	// locally so request everything
	deltaGossipDigestList = append(deltaGossipDigestList,
		NewGossipDigest(gDigest.endPoint, remoteGeneration, 0))
}

func (g *Gossiper) getStateForVersionBiggerThan(forEndpoint network.EndPoint, version int) *EndPointState {
	epState := g.endPointStateMap[forEndpoint]
	var res *EndPointState
	if epState == nil {
		return res
	}
	// here we try to include the Heart Beat state only
	// if it is greater than the version passed in. it
	// might happen that the heart beat version maybe
	// less than version passed in and some application
	// state has a version that is greater than the version
	// passed in. in this case we also send the old heart
	// beat and throw it away on the receiver if it is redundant
	localHbVersion := epState.GetHeartBeatState().GetVersion()
	if localHbVersion > int32(version) {
		res = NewEndPointState(epState.GetHeartBeatState())
	}
	appStateMap := epState.applicationState
	// accumulate all application states whose versions
	// are greater than "version" variable
	for key, appState := range appStateMap {
		if appState.GetStateVersion() > version {
			if res == nil {
				res = NewEndPointState(epState.GetHeartBeatState())
			}
			res.AddApplicationState(key, appState)
		}
	}
	return res
}

func (g *Gossiper) sendAll(gDigest *GossipDigest, deltaEpStateMap map[network.EndPoint]*EndPointState, maxRemoteVersion int) {
	// send all the data with version greater than maxRemoteVersion
	localEpStatePtr := g.getStateForVersionBiggerThan(gDigest.endPoint, maxRemoteVersion)
	if localEpStatePtr != nil {
		deltaEpStateMap[gDigest.endPoint] = localEpStatePtr
	}
}

// GossipDigestSynArgs ...
type GossipDigestSynArgs struct {
	From      network.EndPoint
	ClusterID string
	GDigest   []*GossipDigest
}

// GossipDigestSynReply ...
type GossipDigestSynReply struct{}

func (g *Gossiper) makeGossipDigestSynMessage(gDigest []*GossipDigest) *GossipDigestSynArgs {
	p := &GossipDigestSynArgs{}
	p.ClusterID = config.ClusterName
	p.GDigest = gDigest
	p.From = *g.localEndPoint
	return p
}

// GossipDigestAckArgs ...
type GossipDigestAckArgs struct {
	From       network.EndPoint
	ClusterID  string
	GDigest    []*GossipDigest
	epStateMap map[network.EndPoint]*EndPointState
}

// GossipDigestAckReply ...
type GossipDigestAckReply struct{}

func (g *Gossiper) makeGossipDigestAckMessage(gDigestList []*GossipDigest,
	epStateMap map[network.EndPoint]*EndPointState) *GossipDigestAckArgs {
	p := &GossipDigestAckArgs{}
	p.From = *g.localEndPoint
	p.ClusterID = config.ClusterName
	p.GDigest = gDigestList
	p.epStateMap = epStateMap
	return p
}

// OnGossipDigestSyn is an rpc
func (g *Gossiper) OnGossipDigestSyn(args *GossipDigestSynArgs, reply *GossipDigestSynReply) error {
	from := args.From
	log.Printf("received a GossipDigestSyn from %v\n", from)
	if args.ClusterID != config.ClusterName {
		// the message is from a different cluster
		return nil
	}
	gDigestList := args.GDigest
	g.notifyFailureDetector(gDigestList)
	g.doSort(gDigestList)
	deltaGossipDigestList := make([]*GossipDigest, 0)
	deltaEpStateMap := make(map[network.EndPoint]*EndPointState)
	g.examineGossiper(gDigestList, deltaGossipDigestList, deltaEpStateMap)
	message := g.makeGossipDigestAckMessage(deltaGossipDigestList, deltaEpStateMap)
	// send message
	to := from
	log.Printf("Sending a GossipDigestAckMessage to %v ...\n", to)
	ackReply := &GossipDigestAckReply{}
	client, err := rpc.DialHTTP("udp", to.HostName+":"+config.ControlPort)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	client.Go("Gossiper.OnGossipDigestAck", message, ackReply, nil)
	return nil
}

// GossipDigestAck2Args ...
type GossipDigestAck2Args struct {
	From       network.EndPoint
	ClusterID  string
	epStateMap map[network.EndPoint]*EndPointState
}

// GossipDigestAck2Reply ...
type GossipDigestAck2Reply struct{}

func (g *Gossiper) makeGossipDigestAck2Message(epStateMap map[network.EndPoint]*EndPointState) *GossipDigestAck2Args {
	p := &GossipDigestAck2Args{}
	p.From = *g.localEndPoint
	p.ClusterID = config.ClusterName
	p.epStateMap = epStateMap
	return p
}

// OnGossipDigestAck is an rpc
func (g *Gossiper) OnGossipDigestAck(args *GossipDigestAckArgs, reply *GossipDigestAckReply) error {
	from := args.From
	log.Printf("received a GossipDigestAckMessage from %v\n", from)
	gDigestList := args.GDigest
	epStateMap := args.epStateMap
	if len(epStateMap) > 0 {
		// notify the Failure Detector
		g.notifyFailureDetectorM(epStateMap)
		g.applyStateLocally(epStateMap)
	}
	// get the state required to send to this gossipee -
	// construct GossipDigestAck2Message
	deltaEpStateMap := make(map[network.EndPoint]*EndPointState)
	for _, gDigest := range gDigestList {
		addr := gDigest.endPoint
		localEpStatePtr := g.getStateForVersionBiggerThan(addr, gDigest.maxVersion)
		if localEpStatePtr != nil {
			deltaEpStateMap[addr] = localEpStatePtr
		}
	}
	message := g.makeGossipDigestAck2Message(deltaEpStateMap)
	// send message
	to := from
	log.Printf("Sending a GossipDigestAck2Message to %v ...\n", to)
	ackReply := &GossipDigestAckReply{}
	client, err := rpc.DialHTTP("udp", to.HostName+":"+config.ControlPort)
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	client.Go("Gossiper.OnGossipDigestAck2", message, ackReply, nil)
	return nil
}

// OnGossipDigestAck2 is an rpc
func (g *Gossiper) OnGossipDigestAck2(args *GossipDigestAck2Args, reply *GossipDigestAck2Reply) error {
	from := args.From
	log.Printf("received a GossipDigestAck2Message from %v\n", from)
	epStateMap := args.epStateMap
	if len(epStateMap) > 0 {
		// notify the Failure Detector
		g.notifyFailureDetectorM(epStateMap)
		g.applyStateLocally(epStateMap)
	}
	return nil
}
