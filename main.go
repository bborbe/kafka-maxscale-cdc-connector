// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bborbe/argument"
	"github.com/bborbe/kafka-maxscale-cdc-connector/cdc"
	"github.com/bborbe/run"
	"github.com/getsentry/raven-go"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	defer glog.Flush()
	glog.CopyStandardLogTo("info")
	runtime.GOMAXPROCS(runtime.NumCPU())
	_ = flag.Set("logtostderr", "true")

	app := &application{}
	if err := argument.Parse(app); err != nil {
		glog.Exitf("parse app failed: %v", err)
	}

	if err := app.initSentry(); err != nil {
		glog.Exitf("setting up Sentry failed: %+v", err)
	}

	glog.V(0).Infof("application started")
	if err := app.run(contextWithSig(context.Background())); err != nil {
		raven.CaptureErrorAndWait(err, map[string]string{})
		glog.Exitf("application failed: %+v", err)
	}
	glog.V(0).Infof("application finished")
	os.Exit(0)
}

func contextWithSig(ctx context.Context) context.Context {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()

		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-signalCh:
		case <-ctx.Done():
		}
	}()

	return ctxWithCancel
}

// App for streaming changes from Mariadb to Kafka
type application struct {
	CdcDatabase  string        `required:"true" arg:"cdc-database" env:"CDC_DATABASE" usage:"Database"`
	CdcGTID      string        `required:"false" arg:"cdc-gtid" env:"CDC_GTID" usage:"GTID to start with"`
	CdcHost      string        `required:"true" arg:"cdc-host" env:"CDC_HOST" usage:"Host"`
	CdcPassword  string        `required:"true" arg:"cdc-password" env:"CDC_PASSWORD" usage:"Password" display:"length"`
	CdcPort      int           `required:"true" arg:"cdc-port" env:"CDC_PORT" usage:"Port" default:"4001"`
	CdcTable     string        `required:"true" arg:"cdc-table" env:"CDC_TABLE" usage:"Table"`
	CdcUser      string        `required:"true" arg:"cdc-user" env:"CDC_USER" usage:"User"`
	CdcUUID      string        `required:"false" arg:"cdc-uuid" env:"CDC_UUID" usage:"UUID of CDC"`
	DataDir      string        `required:"true" arg:"datadir" env:"DATADIR" usage:"Directory last GTID is saved in"`
	InitialDelay time.Duration `required:"false" arg:"initial-delay" env:"INITIAL_DELAY" usage:"initial time before processing starts" default:"1m"`
	KafkaBrokers string        `required:"true" arg:"kafka-brokers" env:"KAFKA_BROKERS" usage:"kafka brokers"`
	KafkaTopic   string        `required:"true" arg:"kafka-topic" env:"KAFKA_TOPIC" usage:"Kafka CDC messages are sent to"`
	Port         int           `required:"false" arg:"port" env:"PORT" usage:"port to listen" default:"9033"`
	SentryDSN    string        `required:"false" arg:"sentry-dsn" env:"SENTRY_DSN" usage:"Sentry DSN"`
}

func (a *application) initSentry() error {
	if a.SentryDSN == "" {
		glog.V(0).Info("Sentry will not be used since not all settings for it were passed")
		return nil
	}
	return raven.SetDSN(a.SentryDSN)
}

// Run the app and blocks until error occurred or the context is canceled
func (a *application) run(ctx context.Context) error {
	if a.CdcUUID == "" {
		a.CdcUUID = uuid.New().String()
	}
	return run.CancelOnFirstFinish(
		ctx,
		run.Delayed(a.runStreamer, a.InitialDelay),
		a.runHttpServer,
	)
}

func (a *application) runStreamer(ctx context.Context) error {
	gtid, err := cdc.ParseGTID(a.CdcGTID)
	if err != nil {
		return errors.Wrap(err, "parse gtid failed")
	}
	gtidStore := cdc.NewGTIDStore(
		a.DataDir,
	)
	if gtid == nil {
		gtid, err = gtidStore.Read()
		if err != nil {
			glog.V(1).Infof("read gtid from disk failed")
		}
	}

	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	glog.V(3).Infof("connect to brokers %s", a.KafkaBrokers)

	client, err := sarama.NewClient(strings.Split(a.KafkaBrokers, ","), config)
	if err != nil {
		return errors.Wrap(err, "create client failed")
	}
	defer client.Close()

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return errors.Wrap(err, "create sync producer failed")
	}
	defer producer.Close()

	return cdc.NewStreamer(
		gtid,
		cdc.NewRetryReader(
			&cdc.MaxscaleReader{
				Dialer: cdc.NewTcpDialer(
					fmt.Sprintf("%s:%d", a.CdcHost, a.CdcPort),
				),
				User:     a.CdcUser,
				Password: a.CdcPassword,
				Database: a.CdcDatabase,
				Table:    a.CdcTable,
				Format:   "JSON",
				UUID:     a.CdcUUID,
			},
			cdc.NewGTIDExtractor(
				"JSON",
			),
		),
		cdc.NewKafkaSender(
			producer,
			a.KafkaTopic,
			gtidStore,
			cdc.NewGTIDExtractor(
				"JSON",
			),
		),
	).Run(ctx)
}

func (a *application) runHttpServer(ctx context.Context) error {
	router := mux.NewRouter()
	router.HandleFunc("/healthz", a.check)
	router.HandleFunc("/readiness", a.check)
	router.Handle("/metrics", promhttp.Handler())
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", a.Port),
		Handler: router,
	}
	go func() {
		select {
		case <-ctx.Done():
			if err := server.Shutdown(ctx); err != nil {
				glog.Warningf("shutdown failed: %v", err)
			}
		}
	}()
	return server.ListenAndServe()
}

func (a *application) check(resp http.ResponseWriter, req *http.Request) {
	resp.WriteHeader(http.StatusOK)
}
