// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cdc

import (
	"context"
	"github.com/golang/glog"
	"runtime"
	"sync"

	"github.com/bborbe/run"
)

// Reader interface for the Streamer
//go:generate counterfeiter -o ../mocks/reader.go --fake-name Reader . Reader
type Reader interface {
	// Read changes and send them to the given channel
	Read(ctx context.Context, gtid *GTID, ch chan<- []byte) error
}

// Sender interface for the Streamer
//go:generate counterfeiter -o ../mocks/sender.go --fake-name Sender . Sender
type Sender interface {
	Send(ctx context.Context, ch <-chan []byte) error
}

// Streamer coordinates read and send of CDC records
type Streamer interface {
	Run(ctx context.Context) error
}

func NewStreamer(
	gtid *GTID,
	reader Reader,
	sender Sender,
) Streamer {
	return &streamer{
		gtid:   gtid,
		reader: reader,
		sender: sender,
	}
}

type streamer struct {
	gtid   *GTID
	reader Reader
	sender Sender
}

// Run read and send of CDC records
func (s *streamer) Run(ctx context.Context) error {
	glog.V(1).Infof("stream started")
	defer glog.V(1).Infof("stream finished")
	ch := make(chan []byte, runtime.NumCPU())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		wg.Wait()
		close(ch)
	}()
	return run.CancelOnFirstFinish(
		ctx,
		func(ctx context.Context) error {
			defer wg.Done()
			return s.reader.Read(ctx, s.gtid, ch)
		},
		func(ctx context.Context) error {
			defer wg.Done()
			return s.sender.Send(ctx, ch)
		},
	)
}
