// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cdc

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"github.com/pkg/errors"
)

// KafkaSender takes a channel of []byte and send them to the given topic
type KafkaSender interface {
	Send(ctx context.Context, ch <-chan []byte) error
}

func NewKafkaSender(
	producer sarama.SyncProducer,
	kafkaTopic string,
	gtidStore GTIDStore,
	gtidExtractor GTIDExtractor,
) KafkaSender {
	return &kafkaSender{
		producer:      producer,
		kafkaTopic:    kafkaTopic,
		gtidStore:     gtidStore,
		gtidExtractor: gtidExtractor,
	}
}

type kafkaSender struct {
	producer      sarama.SyncProducer
	kafkaTopic    string
	gtidStore     GTIDStore
	gtidExtractor GTIDExtractor
}

// Send the given messages to a topic in Kafka
func (k *kafkaSender) Send(ctx context.Context, ch <-chan []byte) error {
	glog.V(3).Infof("wait for lines")
	for {
		select {
		case <-ctx.Done():
			return nil
		case data, ok := <-ch:
			if !ok {
				return nil
			}
			gtid, err := k.gtidExtractor.Parse(data)
			if err != nil {
				return errors.Wrap(err, "extract gtid failed")
			}
			partition, offset, err := k.producer.SendMessage(&sarama.ProducerMessage{
				Topic: k.kafkaTopic,
				Key:   sarama.StringEncoder(gtid.String()),
				Value: sarama.ByteEncoder(data),
			})
			if err != nil {
				return errors.Wrap(err, "send message to kafka failed")
			}
			glog.V(3).Infof("send message successful to %s with partition %d offset %d", k.kafkaTopic, partition, offset)
			if err := k.gtidStore.Write(gtid); err != nil {
				return errors.Wrap(err, "save gtid failed")
			}
		}
	}
}
