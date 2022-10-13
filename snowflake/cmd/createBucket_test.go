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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sftesting "antrea.io/theia/snowflake/cmd/testing"
	s3clientfake "antrea.io/theia/snowflake/pkg/aws/client/s3/fake"
)

const bucketExistLog = "S3 bucket already exists"

func TestCreateBucket(t *testing.T) {
	origLogger := logger
	defer func() {
		logger = origLogger
	}()

	var b bytes.Buffer
	logger = sftesting.NewLogger(&b)

	for _, tc := range []struct {
		name              string
		bucketName        string
		expectedBucketNum int
		bucketExist       bool
	}{
		{
			name:              "Create existing bucket",
			bucketName:        "existingBucket",
			expectedBucketNum: 1,
			bucketExist:       true,
		},
		{
			name:              "Create new bucket",
			bucketName:        "newBucket",
			expectedBucketNum: 2,
			bucketExist:       false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b.Reset()
			fakeS3Client := s3clientfake.FakeS3Client{}
			fakeS3Client.InitFakeBuckets()
			require.Equal(t, 1, len(s3clientfake.FakeBuckets))
			err := createBucket(context.TODO(), fakeS3Client, tc.bucketName, "")
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedBucketNum, len(s3clientfake.FakeBuckets))
			_, ok := s3clientfake.FakeBuckets[tc.bucketName]
			assert.True(t, ok)
			if tc.bucketExist {
				assert.Contains(t, b.String(), bucketExistLog)
			} else {
				assert.NotContains(t, b.String(), bucketExistLog)
			}
		})
	}
}
