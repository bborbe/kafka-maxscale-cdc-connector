// Copyright (c) 2019 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package run

import "bytes"

// ErrorList contains a list of errors.
type ErrorList []error

// NewErrorList create a ErrorList with the given errors.
func NewErrorList(errors ...error) ErrorList {
	if len(errors) == 0 {
		return nil
	}
	return ErrorList(errors)
}

// NewErrorListByChan create a ErrorList with the given error channel.
func NewErrorListByChan(errors <-chan error) ErrorList {
	var list []error
	for err := range errors {
		list = append(list, err)
	}
	return NewErrorList(list...)
}

// Error combines all error messages into one.
func (e ErrorList) Error() string {
	buf := bytes.NewBufferString("errors: ")
	first := true
	for _, err := range e {
		if first {
			first = false
		} else {
			buf.WriteString(", ")
		}
		buf.WriteString(err.Error())
	}
	return buf.String()
}
