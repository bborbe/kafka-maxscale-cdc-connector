// Copyright (c) 2019 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package argument

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
)

// ValidateRequired fields are set and returns an error if not.
func ValidateRequired(data interface{}) error {
	e := reflect.ValueOf(data).Elem()
	t := e.Type()
	for i := 0; i < e.NumField(); i++ {
		tf := t.Field(i)
		ef := e.Field(i)
		argName, ok := tf.Tag.Lookup("required")
		if !ok || argName != "true" {
			continue
		}
		createError := func() error {
			buf := bytes.NewBufferString("Required field empty, ")
			argName, argOk := tf.Tag.Lookup("arg")
			if argOk {
				fmt.Fprintf(buf, "define parameter %s", argName)
			}
			envName, envOk := tf.Tag.Lookup("env")
			if envOk {
				if argOk {
					fmt.Fprintf(buf, " or ")
				}
				fmt.Fprintf(buf, "define env %s", envName)
			}
			return errors.New(buf.String())
		}
		switch ef.Interface().(type) {
		case string:
			var empty string
			if empty == ef.Interface() {
				return createError()
			}
		case bool:
		case int:
			var empty int
			if empty == ef.Interface() {
				return createError()
			}
		case int64:
			var empty int64
			if empty == ef.Interface() {
				return createError()
			}
		case uint:
			var empty uint
			if empty == ef.Interface() {
				return createError()
			}
		case uint64:
			var empty uint64
			if empty == ef.Interface() {
				return createError()
			}
		case float64:
			var empty float64
			if empty == ef.Interface() {
				return createError()
			}
		case time.Duration:
			var empty time.Duration
			if empty == ef.Interface() {
				return createError()
			}
		default:
			return errors.Errorf("field %s with type %T is unsupported", tf.Name, ef.Interface())
		}
	}
	return nil
}
