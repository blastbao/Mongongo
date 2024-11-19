// Copyright (c) 2020 DistAlchemist
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

package utils

import (
	"math"

	"gopkg.in/karalabe/cookiejar.v1/collections/deque"
)

// BoundedStatsDeque ...
type BoundedStatsDeque struct {
	size int          // 队列容量，一旦队列大小达到 size，再添加新的元素时，会移除最旧的元素。
	d    *deque.Deque // 双端队列，允许在两端快速地进行插入和删除操作。
}

// NewBoundedStatsDeque ...
func NewBoundedStatsDeque(size int) *BoundedStatsDeque {
	b := &BoundedStatsDeque{}
	b.size = size
	b.d = deque.New()
	return b
}

// Size ...
func (p *BoundedStatsDeque) Size() int {
	return p.d.Size()
}

// Clear ...
func (p *BoundedStatsDeque) Clear() {
	p.d.Reset()
}

// Add ...
func (p *BoundedStatsDeque) Add(o float64) {
	if p.size == p.d.Size() { // 如果队列已满，移除最左边的元素（最旧的元素），以保证队列的大小不会超过 size
		p.d.PopLeft()
	}
	p.d.PushRight(o) // 将新的元素添加到队列的右侧
}

// Sum ...
// 计算队列中所有元素的总和
func (p *BoundedStatsDeque) Sum() float64 {
	sum := float64(0)
	r := deque.New()
	for p.d.Empty() == false {
		interval := p.d.PopLeft()
		sum += interval.(float64)
		r.PushRight(interval)
	}
	p.d = r
	return sum
}

// SumOfDeviations ...
// 计算队列中元素与均值的偏差平方和
func (p *BoundedStatsDeque) SumOfDeviations() float64 {
	res := float64(0)
	mean := p.Mean()
	r := deque.New()
	for p.d.Empty() == false {
		interval := p.d.PopLeft()
		v := interval.(float64) - mean
		res += v * v
		r.PushRight(interval)
	}
	p.d = r
	return res
}

// Mean ...
// 计算队列中所有元素的平均值。
func (p *BoundedStatsDeque) Mean() float64 {
	return p.Sum() / float64(p.Size())
}

// Variance ...
// 计算队列中所有元素的方差。
// 方差是偏差平方和的平均值，通过调用 SumOfDeviations() 方法计算偏差之和，并除以队列的大小来得到方差。
func (p *BoundedStatsDeque) Variance() float64 {
	return p.SumOfDeviations() / float64(p.Size())
}

// Stdev ...
// 计算队列中所有元素的标准差。
// 标准差是方差的平方根，通过调用 Variance() 方法计算方差，并返回其平方根。
func (p *BoundedStatsDeque) Stdev() float64 {
	return math.Sqrt(p.Variance())
}
