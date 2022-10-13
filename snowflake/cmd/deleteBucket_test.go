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

	sftesting "antrea.io/theia/snowflake/cmd/testing"
	s3clientfake "antrea.io/theia/snowflake/pkg/aws/client/s3/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const deleteObjectLog = "Deleting objects"

func TestDeleteObjects(t *testing.T) {
	origLogger := logger
	defer func() {
		logger = origLogger
	}()

	var b bytes.Buffer
	logger = sftesting.NewLogger(&b)

	for _, tc := range []struct {
		name             string
		bucketName       string
		deletionExpected bool
	}{
		{
			name:             "Delete objects from non-empty bucket",
			bucketName:       "nonEmptyBucket",
			deletionExpected: true,
		},
		{
			name:             "Delete objects from empty bucket",
			bucketName:       "emptyBucket",
			deletionExpected: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b.Reset()
			fakeS3Client := s3clientfake.FakeS3Client{}
			fakeS3Client.InitFakeNonEmptyBucket()
			err := deleteS3Objects(context.TODO(), fakeS3Client, tc.bucketName)
			assert.NoError(t, err)
			if tc.deletionExpected {
				assert.Contains(t, b.String(), deleteObjectLog)
			} else {
				assert.NotContains(t, b.String(), deleteObjectLog)
			}
		})
	}
}

func TestDeleteBucket(t *testing.T) {
	origLogger := logger
	defer func() {
		logger = origLogger
	}()

	var b bytes.Buffer
	logger = sftesting.NewLogger(&b)

	for _, tc := range []struct {
		name                    string
		bucketName              string
		expectedRemainBucketNum int
		bucketExist             bool
	}{
		{
			name:                    "Delete existing bucket",
			bucketName:              "existingBucket",
			expectedRemainBucketNum: 0,
			bucketExist:             true,
		},
		{
			name:                    "Delete non-existing bucket",
			bucketName:              "nonExistingBucket",
			expectedRemainBucketNum: 1,
			bucketExist:             false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeS3Client := s3clientfake.FakeS3Client{}
			fakeS3Client.InitFakeBuckets()
			require.Equal(t, 1, len(s3clientfake.FakeBuckets))
			err := deleteBucket(context.TODO(), fakeS3Client, tc.bucketName)
			if tc.bucketExist {
				assert.NoError(t, err)
				_, ok := s3clientfake.FakeBuckets[tc.bucketName]
				assert.False(t, ok)
			} else {
				assert.Error(t, err)
			}
			assert.Equal(t, tc.expectedRemainBucketNum, len(s3clientfake.FakeBuckets))
		})
	}
}
