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
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// For unit-test only
type FakeS3Client struct{}

type fakeBucket struct{}

var FakeBuckets map[string]fakeBucket
var fakeNonEmptyBucket map[string]s3types.Object

func (c FakeS3Client) InitFakeBuckets() {
	FakeBuckets = make(map[string]fakeBucket)
	FakeBuckets["existingBucket"] = fakeBucket{}
}

func (c FakeS3Client) InitFakeNonEmptyBucket() {
	fakeNonEmptyBucket = make(map[string]s3types.Object)
	key1 := "key1"
	key2 := "key2"
	fakeNonEmptyBucket[key1] = s3types.Object{Key: &key1}
	fakeNonEmptyBucket[key2] = s3types.Object{Key: &key2}
}

func (c FakeS3Client) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	bucketName := params.Bucket
	output := s3.HeadBucketOutput{}
	if _, ok := FakeBuckets[*bucketName]; ok {
		return &output, nil
	}
	return &output, &s3types.NotFound{}
}

func (c FakeS3Client) CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	output := s3.CreateBucketOutput{}
	FakeBuckets[*params.Bucket] = fakeBucket{}
	return &output, nil
}

func (c FakeS3Client) DeleteBucket(ctx context.Context, params *s3.DeleteBucketInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketOutput, error) {
	output := s3.DeleteBucketOutput{}
	bucketName := *params.Bucket
	_, ok := FakeBuckets[bucketName]
	if !ok {
		return &output, &s3types.NotFound{}
	}
	delete(FakeBuckets, bucketName)
	return &output, nil
}

func (c FakeS3Client) PutBucketLifecycleConfiguration(ctx context.Context, params *s3.PutBucketLifecycleConfigurationInput, optFns ...func(*s3.Options)) (*s3.PutBucketLifecycleConfigurationOutput, error) {
	output := s3.PutBucketLifecycleConfigurationOutput{}
	return &output, nil
}

func (c FakeS3Client) PutBucketNotificationConfiguration(ctx context.Context, params *s3.PutBucketNotificationConfigurationInput, optFns ...func(*s3.Options)) (*s3.PutBucketNotificationConfigurationOutput, error) {
	output := s3.PutBucketNotificationConfigurationOutput{}
	return &output, nil
}

func (c FakeS3Client) GetBucketNotificationConfiguration(ctx context.Context, params *s3.GetBucketNotificationConfigurationInput, optFns ...func(*s3.Options)) (*s3.GetBucketNotificationConfigurationOutput, error) {
	output := s3.GetBucketNotificationConfigurationOutput{}
	return &output, nil
}

func (c FakeS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	output := s3.ListObjectsV2Output{}
	if *params.Bucket == "nonEmptyBucket" {
		contents := []s3types.Object{}
		for _, obj := range fakeNonEmptyBucket {
			contents = append(contents, obj)
		}
		output.Contents = contents
	}
	return &output, nil
}

func (c FakeS3Client) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	output := s3.DeleteObjectsOutput{}
	for _, obj := range params.Delete.Objects {
		key := obj.Key
		delete(fakeNonEmptyBucket, *key)
	}
	return &output, nil
}
