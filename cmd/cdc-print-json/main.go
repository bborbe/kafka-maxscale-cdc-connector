// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Sample of read and print cdc messages
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/bborbe/run"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/bborbe/argument"
	"github.com/bborbe/kafka-maxscale-cdc-connector/cdc"
	"github.com/getsentry/raven-go"
	"github.com/golang/glog"
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
	CdcDatabase string `required:"true" arg:"cdc-database" env:"CDC_DATABASE" usage:"Database"`
	CdcHost     string `required:"true" arg:"cdc-host" env:"CDC_HOST" usage:"Host"`
	CdcPassword string `required:"true" arg:"cdc-password" env:"CDC_PASSWORD" usage:"Password" display:"length"`
	CdcPort     int    `required:"true" arg:"cdc-port" env:"CDC_PORT" usage:"Port" default:"4001"`
	CdcTable    string `required:"true" arg:"cdc-table" env:"CDC_TABLE" usage:"Table"`
	CdcUser     string `required:"true" arg:"cdc-user" env:"CDC_USER" usage:"User"`
	CdcUUID     string `required:"false" arg:"cdc-uuid" env:"CDC_UUID" usage:"UUID of CDC"`
	SentryDSN   string `required:"false" arg:"sentry-dsn" env:"SENTRY_DSN" usage:"Sentry DSN"`
}

func (a *application) initSentry() error {
	if a.SentryDSN == "" {
		glog.V(0).Info("Sentry will not be used since not all settings for it were passed")
		return nil
	}
	return raven.SetDSN(a.SentryDSN)
}

func (a *application) run(ctx context.Context) error {
	cdcClient := &cdc.MaxscaleReader{
		Dialer: cdc.NewTcpDialer(
			fmt.Sprintf("%s:%d", a.CdcHost, a.CdcPort),
		),
		User:     a.CdcUser,
		Password: a.CdcPassword,
		Database: a.CdcDatabase,
		Table:    a.CdcTable,
		Format:   "JSON",
		UUID:     a.CdcUUID,
	}

	ch := make(chan []byte, runtime.NumCPU())
	return run.CancelOnFirstError(
		ctx,
		func(ctx context.Context) error {
			defer close(ch)
			return cdcClient.Read(ctx, nil, ch)
		},
		func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case line, ok := <-ch:
					if !ok {
						return nil
					}
					os.Stdout.Write(line)
				}
			}
		},
	)
}
