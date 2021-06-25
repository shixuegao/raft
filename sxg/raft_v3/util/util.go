package util

import (
	"math/rand"
	"time"
)

func AssertRecoverErr() error {
	r := recover()
	switch v := r.(type) {
	case error:
		return v
	default:
		return nil
	}
}

type RandTimer struct {
	basic    int
	offset   int
	timeUnit time.Duration
	rander   *rand.Rand
	timer    *time.Timer
}

func (rt *RandTimer) Reset() {
	rInt := rt.basic + rt.rander.Intn(rt.offset)
	timeout := time.Duration(rInt) * rt.timeUnit
	rt.timer.Reset(timeout)
}

func (rt *RandTimer) Stop() {
	rt.timer.Stop()
}

func (rt *RandTimer) RawC() <-chan time.Time {
	return rt.timer.C
}

func NewRandTimer(basic, offset int, timeUnit time.Duration) *RandTimer {
	rt := RandTimer{
		basic:    basic,
		offset:   offset,
		timeUnit: timeUnit,
		rander:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	rInt := rt.basic + rt.rander.Intn(rt.offset)
	timeout := time.Duration(rInt) * rt.timeUnit
	rt.timer = time.NewTimer(timeout)
	return &rt
}

type UintSlice []uint64

func (p UintSlice) Len() int           { return len(p) }
func (p UintSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p UintSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
