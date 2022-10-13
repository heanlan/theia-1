// Copyright 2022 Antrea Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testing

import (
	"fmt"
	"io"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
)

// NewLogger returns a logr.Logger that writes to buffer,
// referring to https://github.com/go-logr/logr/blob/master/example_test.go
func NewLogger(w io.Writer) logr.Logger {
	return funcr.New(func(prefix, args string) {
		if prefix != "" {
			_ = fmt.Sprintf("%s: %s\n", prefix, args)
		} else {
			fmt.Fprintln(w, args)
		}
	}, funcr.Options{})
}
