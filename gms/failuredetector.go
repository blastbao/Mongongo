// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package gms

import (
	"log"
	"math"
	"os"
	"time"

	"github.com/DistAlchemist/Mongongo/config"
	"github.com/DistAlchemist/Mongongo/network"
	"github.com/DistAlchemist/Mongongo/utils"
)

var failureDetector IFailureDetector

// FailureDetector 通过监测某个端点的心跳（即请求间隔时间）来判断该端点是否存活，并基于一定的阈值来判断该端点是否故障。

// FailureDetector implements IFailureDetector
type FailureDetector struct {
	sampleSize      int
	phiSuspectThres int // phi 可疑阈值，超过该阈值，系统将怀疑该端点可能故障。
	phiConvictThres int // phi 确认阈值，超过该阈值，系统将认为该端点已经故障。
	// Failure Detector has to have been up for at least 1 min.
	uptimeThres int64 // 故障检测器运行的最小时间，单位为毫秒（1分钟）。
	// Time when the module was instantiated.
	creationTime     int64                               // 故障检测器创建时的时间戳
	fdEventListeners []IFailureDetectionEventListener    // 事件监听器
	arrivalSamples   map[network.EndPoint]*ArrivalWindow //
}

// GetFailureDetector will create a new instance
// of FailureDetector if not exists
func GetFailureDetector() IFailureDetector {
	if failureDetector == nil {
		failureDetector = newFailureDetector()
	}
	return failureDetector
}

func newFailureDetector() *FailureDetector {
	f := &FailureDetector{}
	f.creationTime = time.Now().UnixNano() / int64(time.Millisecond)
	f.sampleSize = 1000
	f.phiSuspectThres = 5
	f.phiConvictThres = 8
	f.uptimeThres = 60000 // 1 min.
	f.arrivalSamples = make(map[network.EndPoint]*ArrivalWindow)
	return f
}

// IsAlive check whether the endpoint is up.
// 判断指定的端点是否存活。
func (f *FailureDetector) IsAlive(ep network.EndPoint) bool {
	localHost, err := os.Hostname()
	if err != nil {
		log.Fatalf("error when getting hostname: %v\n", err)
	}
	if localHost == ep.HostName {
		return true
	}
	ep2 := network.EndPoint{HostName: ep.HostName, Port: config.ControlPort}
	epState := GetGossiper().GetEndPointStateForEndPoint(ep2)
	return epState.IsAlive()
}

// 记录心跳。
func (f *FailureDetector) report(ep network.EndPoint) {
	log.Printf("reporting %v\n", ep)
	now := float64(getCurrentTimeInMillis())
	heartbeatWindow, ok := f.arrivalSamples[ep]
	if ok == false {
		heartbeatWindow = NewArrivalWindow(f.sampleSize)
		f.arrivalSamples[ep] = heartbeatWindow
	}
	heartbeatWindow.Add(now)
}

// 根据 phi 值判断端点是否可能故障。
func (f *FailureDetector) interpret(ep network.EndPoint) {
	hbWnd, ok := f.arrivalSamples[ep]
	if ok == false {
		return
	}
	now := getCurrentTimeInMillis()
	// we need this so that we do not suspect a convict
	isConvicted := false
	phi := hbWnd.Phi(now)
	log.Printf("Phi for %v: %v\n", ep, phi)
	if !isConvicted && phi > float64(f.phiSuspectThres) {
		for _, listener := range f.fdEventListeners {
			listener.Suspect(ep)
		}
	}
}

// RegisterEventListener registers event listener for fd
func (f *FailureDetector) RegisterEventListener(listener IFailureDetectionEventListener) {
	f.fdEventListeners = append(f.fdEventListeners, listener)
}

// UnregisterEventListener ...
func (f *FailureDetector) UnregisterEventListener(listener IFailureDetectionEventListener) {
	res := -1
	for idx, key := range f.fdEventListeners {
		if key == listener {
			res = idx
			break
		}
	}
	if res != -1 {
		f.fdEventListeners = append(f.fdEventListeners[:res], f.fdEventListeners[res+1:]...)
	}
}

// ArrivalWindow ...
// 窗口
type ArrivalWindow struct {
	tLast            float64                  // 上一个心跳时间戳
	arrivalIntervals *utils.BoundedStatsDeque // 保存心跳历史
}

// NewArrivalWindow ...
func NewArrivalWindow(size int) *ArrivalWindow {
	p := &ArrivalWindow{}
	p.tLast = 0
	p.arrivalIntervals = utils.NewBoundedStatsDeque(size)
	return p
}

// Add ...
func (p *ArrivalWindow) Add(value float64) {
	var interArrivalTime float64
	if p.tLast > 0 {
		interArrivalTime = value - p.tLast
	} else {
		interArrivalTime = float64(GIntervalInMillis) / 2
	}
	p.tLast = value
	p.arrivalIntervals.Add(interArrivalTime)
}

// Sum ...
func (p *ArrivalWindow) Sum() float64 {
	return p.arrivalIntervals.Sum()
}

// SumOfDeviations ...
func (p *ArrivalWindow) SumOfDeviations() float64 {
	return p.arrivalIntervals.SumOfDeviations()
}

// Mean ...
func (p *ArrivalWindow) Mean() float64 {
	return p.arrivalIntervals.Mean()
}

// Variance ...
func (p *ArrivalWindow) Variance() float64 {
	return p.arrivalIntervals.Variance()
}

// Stdev ...
func (p *ArrivalWindow) Stdev() float64 {
	return p.arrivalIntervals.Stdev()
}

// Clear ...
func (p *ArrivalWindow) Clear() {
	p.arrivalIntervals.Clear()
}

// P ...
//
// P(t) 计算的是一个指数衰减的概率，表示一个端点在特定时间后仍然存活的概率。
func (p *ArrivalWindow) P(t float64) float64 {
	mean := p.Mean()
	exponent := -1 * t / mean
	return 1 - (1 - math.Pow(math.E, exponent))
}

// Phi ...
//
// Phi 通常用于故障检测中判断端点是否可能发生故障。
// Phi 通过对心跳间隔进行分析，得出一个基于时间和间隔分布的统计值。
// Phi 的计算依赖于 P(t) 的值，通过 math.Log10(prob) 转换为 phi 值。
// phi 是一个重要的指标，表示端点的心跳模式是否异常。
// 通常情况下，如果 phi 值较大，表示端点可能出现故障。
func (p *ArrivalWindow) Phi(tnow int64) float64 {
	size := p.arrivalIntervals.Size()
	res := float64(0)
	if size > 0 {
		t := float64(tnow) - p.tLast
		prob := p.P(t)
		res = (-1) * math.Log10(prob)
	}
	return res
}
