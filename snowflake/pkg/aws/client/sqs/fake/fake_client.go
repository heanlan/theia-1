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

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// For unit-test only
type FakeSqsClient struct{}

var FakeQueue []types.Message

func (c FakeSqsClient) InitFakeQueue(queueName string) {
	FakeQueue = FakeQueue[:0]
	if queueName == "nonEmptyQueue" {
		body1 := "fake-msg-1"
		body2 := "fake-msg-2"
		m1 := types.Message{Body: &body1}
		m2 := types.Message{Body: &body2}
		FakeQueue = append(FakeQueue, m1)
		FakeQueue = append(FakeQueue, m2)
	}
}

func (c FakeSqsClient) GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	queueUrl := "emptyQueueUrl"
	if *params.QueueName == "nonEmptyQueue" {
		queueUrl = "nonEmptyQueueUrl"
	}
	output := sqs.GetQueueUrlOutput{
		QueueUrl: &queueUrl,
	}
	return &output, nil
}

func (c FakeSqsClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	messages := []types.Message{}
	if *params.QueueUrl == "nonEmptyQueueUrl" {
		for i := 0; int32(i) < params.MaxNumberOfMessages; i++ {
			messages = append(messages, FakeQueue[i])
		}
	}
	output := sqs.ReceiveMessageOutput{
		Messages: messages,
	}
	return &output, nil
}

func (c FakeSqsClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if len(FakeQueue) > 0 {
		FakeQueue = FakeQueue[1:]
	}
	output := sqs.DeleteMessageOutput{}
	return &output, nil
}
