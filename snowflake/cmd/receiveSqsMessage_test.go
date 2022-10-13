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

package cmd

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	sqsfake "antrea.io/theia/snowflake/pkg/aws/client/sqs/fake"
	"github.com/stretchr/testify/assert"
)

func TestReceiveSQSMessage(t *testing.T) {
	origPrintln := println
	defer func() {
		println = origPrintln
	}()
	var b bytes.Buffer
	println = func(a ...any) (n int, err error) {
		return fmt.Fprintln(&b, a...)
	}
	for _, tc := range []struct {
		name                   string
		queueName              string
		delete                 bool
		expectedStdOut         string
		expectedRemainQueueLen int
	}{
		{
			name:                   "Receive message from empty queue",
			queueName:              "emptyQueue",
			delete:                 true,
			expectedStdOut:         "",
			expectedRemainQueueLen: 0,
		},
		{
			name:                   "Receive message from non-empty queue with deletion",
			queueName:              "nonEmptyQueue",
			delete:                 true,
			expectedStdOut:         "fake-msg-1\n",
			expectedRemainQueueLen: 1,
		},
		{
			name:                   "Receive message from non-empty queue without deletion",
			queueName:              "nonEmptyQueue",
			delete:                 false,
			expectedStdOut:         "fake-msg-1\n",
			expectedRemainQueueLen: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b.Reset()
			fakeClient := sqsfake.FakeSqsClient{}
			fakeClient.InitFakeQueue(tc.queueName)
			receiveSQSMessage(context.TODO(), fakeClient, tc.queueName, tc.delete)
			assert.Equal(t, tc.expectedStdOut, b.String())
			assert.Equal(t, tc.expectedRemainQueueLen, len(sqsfake.FakeQueue))
		})
	}
}
