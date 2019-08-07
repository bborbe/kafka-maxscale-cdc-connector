// Copyright (c) 2019 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package argument

import (
	"flag"
	"reflect"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// ParseArgs into the given struct.
func ParseArgs(data interface{}, args []string) error {
	values, err := argsToValues(data, args)
	if err != nil {
		return err
	}
	return Fill(data, values)
}

func argsToValues(data interface{}, args []string) (map[string]interface{}, error) {
	e := reflect.ValueOf(data).Elem()
	t := e.Type()
	values := make(map[string]interface{})
	for i := 0; i < e.NumField(); i++ {
		tf := t.Field(i)
		ef := e.Field(i)
		argName, ok := tf.Tag.Lookup("arg")
		if !ok {
			continue
		}
		defaultString := tf.Tag.Get("default")
		usage := tf.Tag.Get("usage")
		switch ef.Interface().(type) {
		case string:
			values[tf.Name] = flag.CommandLine.String(argName, defaultString, usage)
		case bool:
			defaultValue, _ := strconv.ParseBool(defaultString)
			values[tf.Name] = flag.CommandLine.Bool(argName, defaultValue, usage)
		case int:
			defaultValue, _ := strconv.Atoi(defaultString)
			values[tf.Name] = flag.CommandLine.Int(argName, defaultValue, usage)
		case int64:
			defaultValue, _ := strconv.ParseInt(defaultString, 10, 0)
			values[tf.Name] = flag.CommandLine.Int64(argName, defaultValue, usage)
		case uint:
			defaultValue, _ := strconv.ParseUint(defaultString, 10, 0)
			values[tf.Name] = flag.CommandLine.Uint(argName, uint(defaultValue), usage)
		case uint64:
			defaultValue, _ := strconv.ParseUint(defaultString, 10, 0)
			values[tf.Name] = flag.CommandLine.Uint64(argName, defaultValue, usage)
		case float64:
			defaultValue, _ := strconv.ParseFloat(defaultString, 64)
			values[tf.Name] = flag.CommandLine.Float64(argName, defaultValue, usage)
		case time.Duration:
			defaultValue, _ := time.ParseDuration(defaultString)
			values[tf.Name] = flag.CommandLine.Duration(argName, defaultValue, usage)
		default:
			return nil, errors.Errorf("field %s with type %T is unsupported", tf.Name, ef.Interface())
		}
	}
	if err := flag.CommandLine.Parse(args); err != nil {
		return nil, err
	}
	return values, nil
}
