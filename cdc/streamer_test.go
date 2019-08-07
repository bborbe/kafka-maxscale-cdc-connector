// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cdc_test

import (
	"context"
	"time"

	"github.com/bborbe/kafka-maxscale-cdc-connector/cdc"
	"github.com/bborbe/kafka-maxscale-cdc-connector/mocks"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Streamer", func() {
	var reader *mocks.Reader
	var sender *mocks.Sender
	var streamer cdc.Streamer
	BeforeEach(func() {
		reader = &mocks.Reader{}
		reader.ReadStub = func(i context.Context, gtid *cdc.GTID, bytes chan<- []byte) error {
			bytes <- []byte("hello world")
			return nil
		}
		sender = &mocks.Sender{}
		sender.SendStub = func(i context.Context, bytes <-chan []byte) error {
			for range bytes {
			}
			return nil
		}
	})
	Context("with gtid", func() {
		BeforeEach(func() {
			gtiid, err := cdc.ParseGTID("1-3-37")
			Expect(err).To(BeNil())
			streamer = cdc.NewStreamer(
				gtiid,
				reader,
				sender,
			)
		})
		It("returns nil", func() {
			err := streamer.Run(context.Background())
			Expect(err).To(BeNil())
		})
		It("calls the reader with the given gtid", func() {
			err := streamer.Run(context.Background())
			Expect(err).To(BeNil())
			Expect(reader.ReadCallCount()).To(Equal(1))
			ctx, gtid, ch := reader.ReadArgsForCall(0)
			Expect(ctx).NotTo(BeNil())
			Expect(gtid).To(Equal(gtid))
			Expect(ch).NotTo(BeNil())
		})
	})
	Context("without gtid", func() {
		BeforeEach(func() {
			streamer = cdc.NewStreamer(
				nil,
				reader,
				sender,
			)
		})
		It("returns nil", func() {
			err := streamer.Run(context.Background())
			Expect(err).To(BeNil())
		})
		It("calls the sender", func() {
			err := streamer.Run(context.Background())
			Expect(err).To(BeNil())
			time.Sleep(100 * time.Millisecond)
			Expect(sender.SendCallCount()).To(Equal(1))
			ctx, ch := sender.SendArgsForCall(0)
			Expect(ctx).NotTo(BeNil())
			Expect(ch).NotTo(BeNil())
		})
	})

})
