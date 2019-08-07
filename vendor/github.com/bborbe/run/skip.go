package run

import (
	"context"

	"github.com/getsentry/raven-go"
	"github.com/golang/glog"
)

// SkipErrors runs the given Func and returns always nil.
func SkipErrors(fn Func) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if err := fn(ctx); err != nil {
			glog.Warningf("run failed: %v", err)
		}
		return nil
	}
}

// SkipErrorsAndReport runs the given Func, report errors to sentry and returns always nil.
func SkipErrorsAndReport(fn Func, tags map[string]string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if err := fn(ctx); err != nil {
			glog.Warningf("run failed: %v", err)
			raven.CaptureErrorAndWait(err, tags)
		}
		return nil
	}
}
