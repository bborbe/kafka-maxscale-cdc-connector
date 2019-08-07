// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cdc

import (
	"io/ioutil"
	"path"

	"github.com/pkg/errors"
)

// GTIDStore save GTID to disk
type GTIDStore interface {
	Read() (*GTID, error)
	Write(gtid *GTID) error
}

func NewGTIDStore(
	dataDir string,
) GTIDStore {
	return &gtidStore{
		dataDir: dataDir,
	}
}

type gtidStore struct {
	dataDir string
}

// Read the last GTID from disk
func (g *gtidStore) Read() (*GTID, error) {
	content, err := ioutil.ReadFile(g.path())
	if err != nil {
		return nil, errors.Wrapf(err, "read file %s failed", g.path())
	}
	return ParseGTID(string(content))
}

// Write the given GTID to disk
func (g *gtidStore) Write(gtid *GTID) error {
	return ioutil.WriteFile(g.path(), []byte(gtid.String()), 0600)
}

func (g *gtidStore) path() string {
	return path.Join(g.dataDir, "lastgtid")
}
