// Copyright (c) 2019 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package run

import (
	"context"
	"runtime"
	"sync"
	"time"
)

// CancelOnFirstFinish executes all given functions. After the first function finishes, any remaining functions will be canceled.
func CancelOnFirstFinish(ctx context.Context, funcs ...Func) error {
	if len(funcs) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return <-Run(ctx, funcs...)
}

// CancelOnFirstError executes all given functions. When a function encounters an error all remaining functions will be canceled.
func CancelOnFirstError(ctx context.Context, funcs ...Func) error {
	if len(funcs) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for err := range Run(ctx, funcs...) {
		if err != nil {
			return err
		}
	}
	return nil
}

// All executes all given functions. Errors are wrapped into one aggregate error.
func All(ctx context.Context, funcs ...Func) error {
	if len(funcs) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return NewErrorListByChan(onlyNotNil(Run(ctx, funcs...)))
}

// Sequential run every given function.
func Sequential(ctx context.Context, funcs ...Func) (err error) {
	if len(funcs) == 0 {
		return nil
	}
	for _, fn := range funcs {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err = fn(ctx); err != nil {
				return
			}
		}
	}
	return
}

// Run all functions and send each result to the returned channel.
func Run(ctx context.Context, funcs ...Func) <-chan error {
	if len(funcs) == 0 {
		return nil
	}
	errors := make(chan error, runtime.NumCPU())
	var wg sync.WaitGroup
	for _, run := range funcs {
		wg.Add(1)
		go func(run Func) {
			defer wg.Done()
			errors <- run(ctx)
		}(run)
	}
	go func() {
		wg.Wait()
		close(errors)
	}()
	return errors
}

func onlyNotNil(ch <-chan error) <-chan error {
	errors := make(chan error, runtime.NumCPU())
	go func() {
		defer close(errors)
		for err := range ch {
			if err != nil {
				errors <- err
			}
		}
	}()
	return errors
}

// Delayed wraps the given function that delays the execution.
func Delayed(fn Func, duration time.Duration) Func {
	return func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return nil
		case <-time.NewTimer(duration).C:
			return fn(ctx)
		}
	}
}
