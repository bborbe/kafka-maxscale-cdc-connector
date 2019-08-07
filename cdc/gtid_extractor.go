// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cdc

import (
	"bytes"
	"encoding/json"

	"github.com/pkg/errors"
)

// GTIDExtractor decodes GTID from records
type GTIDExtractor interface {
	Parse(line []byte) (*GTID, error)
}

func NewGTIDExtractor(
	format string,
) GTIDExtractor {
	return &gtidExtractor{format: format}
}

type gtidExtractor struct {
	format string
}

// Parse GTID from given record
func (g *gtidExtractor) Parse(line []byte) (*GTID, error) {
	switch g.format {
	case "JSON":
		var data GTID
		err := json.NewDecoder(bytes.NewBuffer(line)).Decode(&data)
		return &data, errors.Wrap(err, "decode json failed")
	default:
		return nil, errors.Errorf("unsupported format")
	}
}
