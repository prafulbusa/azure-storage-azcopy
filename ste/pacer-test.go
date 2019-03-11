// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ste

import (
	"context"
	"github.com/Azure/azure-storage-azcopy/common"
	"sync/atomic"
	"time"
)

type pacerTest struct {
	atomicTokenBucket          int64
	atomicTargetBytesPerSecond int64
	expectedBytesPerRequest    int64
	done                       chan struct{}
}

func NewPacerTest(ctx context.Context, bytesPerSecond int64, expectedBytesPerRequest int64) (p *pacerTest) {
	p = &pacerTest{atomicTokenBucket: 0,
		atomicTargetBytesPerSecond: bytesPerSecond,
		expectedBytesPerRequest:    expectedBytesPerRequest,
		done: make(chan struct{}),
	}

	// the pacer runs in a separate goroutine for as long as the ctx lasts
	go p.pacerBody(ctx)

	return p
}

const minBytesPerPulse = 12 * 1024 // TODO review. In an earlier prototype this was derived algorithmically (to various values depending on runtime params)

func (p *pacerTest) Done() {
	close(p.done)
}

func (p *pacerTest) pacerBody(ctx context.Context) {
	lastTime := time.Now()
	for {

		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		default:
		}

		currentTarget := atomic.LoadInt64(&p.atomicTargetBytesPerSecond)
		bytesToRelease := int64(0)
		for bytesToRelease < minBytesPerPulse { // busy wait until we have a decent amount to release
			time.Sleep(1 * time.Second) // TODO: review. This is the old comment from an earlier prototype: must do this here, otherwise we don't seem to be able to feed the counts to the bucket fast enough (too much busy-waiting?  Too much "contention" on _tokenBucket? Not sure why?)
			elapsedSeconds := time.Since(lastTime).Seconds()
			bytesToRelease = int64(float64(currentTarget) * elapsedSeconds)
		}
		newTokenCount := atomic.AddInt64(&p.atomicTokenBucket, bytesToRelease)

		// If the backlog of unsent bytes is too great, then undo our last addition.
		// Why don't we want a big backlog? Because its a sign that actual send rate is well below
		// the paced rate, and when that happens we can subsequently get unwanted big spikes in the send
		// rate, as it suddenly catches up and consumes the backlog. To prevent those spikes, we make
		// sure here that there is no significant backlog
		// The max size of the backlog that we allow is somewhat arbitrary, but is chose to be small enough
		// to keep us responsive to drops in actual throughput, and yet large enough for the sending threads
		// to flow smoothly (i.e. not have to wait more than is necessary for pacing).
		maxAllowedUnsentBytes := currentTarget // limit us to 1 second of throughput by default
		if maxAllowedUnsentBytes < p.expectedBytesPerRequest {
			maxAllowedUnsentBytes = p.expectedBytesPerRequest
		}
		if newTokenCount > maxAllowedUnsentBytes {
			common.AtomicMorphInt64(&p.atomicTokenBucket, func(currentVal int64) (newVal int64, _ interface{}) {
				newVal = currentVal
				if currentVal > maxAllowedUnsentBytes {
					newVal = maxAllowedUnsentBytes
				}
				return
			})
		}

		lastTime = time.Now()
	}
}

// this function is called by goroutines to request right to send a certain amount of bytes
func (p *pacerTest) RequestRightToSend(ctx context.Context, bytesToSend int64) error {
	for atomic.AddInt64(&p.atomicTokenBucket, -bytesToSend) < 0 {
		// by taking our desired count we've moved below zero, which means our allocation is not available
		// right now, so put back what we asked for, and wait
		atomic.AddInt64(&p.atomicTokenBucket, bytesToSend)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second): // TODO: review duration
		}
	}
	return nil
}

/*
func (p *pacerTest) updateTargetRate(increase bool) {
	lastCheckedTimestamp := atomic.LoadInt64(&p.lastUpdatedTimestamp)
	//lastCheckedTime := time.Unix(0,lastCheckedTimestamp)
	if time.Now().Sub(time.Unix(0, lastCheckedTimestamp)) < (time.Second * 3) {
		return
	}
	if atomic.CompareAndSwapInt64(&p.lastUpdatedTimestamp, lastCheckedTimestamp, time.Now().UnixNano()) {
		atomic.StoreInt64(&p.availableBytesPerPeriod, int64(common.Iffloat64(increase, 1.1, 0.9)*float64(p.availableBytesPerPeriod)))
	}
}*/
