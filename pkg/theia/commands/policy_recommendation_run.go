// Copyright 2022 Antrea Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	sparkv1 "antrea.io/theia/third_party/sparkoperator/v1beta2"
)

const (
	flowVisibilityNS        = "flow-visibility"
	k8sQuantitiesReg        = "^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$"
	sparkImage              = "antrea/theia-policy-recommendation:latest"
	sparkImagePullPolicy    = "IfNotPresent"
	sparkAppFile            = "local:///opt/spark/work-dir/policy_recommendation_job.py"
	sparkServiceAccount     = "policy-reco-spark"
	sparkVersion            = "3.1.1"
	statusCheckPollInterval = 5 * time.Second
	statusCheckPollTimeout  = 60 * time.Minute
)

type SparkResourceArgs struct {
	executorInstances   int32
	driverCoreRequest   string
	driverMemory        string
	executorCoreRequest string
	executorMemory      string
}

// policyRecommendationRunCmd represents the policy recommendation run command
var policyRecommendationRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a new policy recommendation Spark job",
	Long: `Run a new policy recommendation Spark job.
Network policies will be recommended based on the flow records sent by Flow Aggregator.
Must finish the deployment of Theia first, please follow the steps in 
https://github.com/antrea-io/theia/blob/main/docs/network-policy-recommendation.md`,
	Example: `Run a policy recommendation spark job with default configuration
$ theia policy-recommendation run
Run an initial policy recommendation spark job with network isolation option anp-deny-applied and limit on last 10k flow records
$ theia policy-recommendation run --type initial --option anp-deny-applied --limit 10000
Run an initial policy recommendation spark job with network isolation option anp-deny-applied and limit on flow records from 2022-01-01 00:00:00 to 2022-01-31 23:59:59.
$ theia policy-recommendation run --type initial --option anp-deny-applied --start-time '2022-01-01 00:00:00' --end-time '2022-01-31 23:59:59'
Run a policy recommendation spark job with default configuration but doesn't recommend toServices ANPs
$ theia policy-recommendation run --to-services=false
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		var recoJobArgs []string
		sparkResourceArgs := SparkResourceArgs{}

		recoType, err := cmd.Flags().GetString("type")
		if err != nil {
			return err
		}
		if recoType != "initial" && recoType != "subsequent" {
			return fmt.Errorf("recommendation type should be 'initial' or 'subsequent'")
		}
		recoJobArgs = append(recoJobArgs, "--type", recoType)

		limit, err := cmd.Flags().GetInt("limit")
		if err != nil {
			return err
		}
		if limit < 0 {
			return fmt.Errorf("limit should be an integer >= 0")
		}
		recoJobArgs = append(recoJobArgs, "--limit", strconv.Itoa(limit))

		option, err := cmd.Flags().GetString("option")
		if err != nil {
			return err
		}
		var optionArg int
		if option == "anp-deny-applied" {
			optionArg = 1
		} else if option == "anp-deny-all" {
			optionArg = 2
		} else if option == "k8s-np" {
			optionArg = 3
		} else {
			return fmt.Errorf(`option of network isolation preference should be 
anp-deny-applied or anp-deny-all or k8s-np`)
		}
		recoJobArgs = append(recoJobArgs, "--option", strconv.Itoa(optionArg))

		startTime, err := cmd.Flags().GetString("start-time")
		if err != nil {
			return err
		}
		var startTimeObj time.Time
		if startTime != "" {
			startTimeObj, err = time.Parse("2006-01-02 15:04:05", startTime)
			if err != nil {
				return fmt.Errorf(`parsing start-time: %v, start-time should be in 
'YYYY-MM-DD hh:mm:ss' format, for example: 2006-01-02 15:04:05`, err)
			}
			recoJobArgs = append(recoJobArgs, "--start_time", startTime)
		}

		endTime, err := cmd.Flags().GetString("end-time")
		if err != nil {
			return err
		}
		if endTime != "" {
			endTimeObj, err := time.Parse("2006-01-02 15:04:05", endTime)
			if err != nil {
				return fmt.Errorf(`parsing end-time: %v, end-time should be in 
'YYYY-MM-DD hh:mm:ss' format, for example: 2006-01-02 15:04:05`, err)
			}
			endAfterStart := endTimeObj.After(startTimeObj)
			if !endAfterStart {
				return fmt.Errorf("end-time should be after start-time")
			}
			recoJobArgs = append(recoJobArgs, "--end_time", endTime)
		}

		nsAllowList, err := cmd.Flags().GetString("ns-allow-list")
		if err != nil {
			return err
		}
		if nsAllowList != "" {
			var parsedNsAllowList []string
			err := json.Unmarshal([]byte(nsAllowList), &parsedNsAllowList)
			if err != nil {
				return fmt.Errorf(`parsing ns-allow-list: %v, ns-allow-list should 
be a list of namespace string, for example: '["kube-system","flow-aggregator","flow-visibility"]'`, err)
			}
			recoJobArgs = append(recoJobArgs, "--ns_allow_list", nsAllowList)
		}

		rmLabels, err := cmd.Flags().GetBool("rm-labels")
		if err != nil {
			return err
		}
		recoJobArgs = append(recoJobArgs, "--rm_labels", strconv.FormatBool(rmLabels))

		toServices, err := cmd.Flags().GetBool("to-services")
		if err != nil {
			return err
		}
		recoJobArgs = append(recoJobArgs, "--to_services", strconv.FormatBool(toServices))

		executorInstances, err := cmd.Flags().GetInt32("executor-instances")
		if err != nil {
			return err
		}
		if executorInstances < 0 {
			return fmt.Errorf("executor-instances should be an integer >= 0")
		}
		sparkResourceArgs.executorInstances = executorInstances

		driverCoreRequest, err := cmd.Flags().GetString("driver-core-request")
		if err != nil {
			return err
		}
		matchResult, err := regexp.MatchString(k8sQuantitiesReg, driverCoreRequest)
		if err != nil || !matchResult {
			return fmt.Errorf("driver-core-request should conform to the Kubernetes convention")
		}
		sparkResourceArgs.driverCoreRequest = driverCoreRequest

		driverMemory, err := cmd.Flags().GetString("driver-memory")
		if err != nil {
			return err
		}
		matchResult, err = regexp.MatchString(k8sQuantitiesReg, driverMemory)
		if err != nil || !matchResult {
			return fmt.Errorf("driver-memory should conform to the Kubernetes convention")
		}
		sparkResourceArgs.driverMemory = driverMemory

		executorCoreRequest, err := cmd.Flags().GetString("executor-core-request")
		if err != nil {
			return err
		}
		matchResult, err = regexp.MatchString(k8sQuantitiesReg, executorCoreRequest)
		if err != nil || !matchResult {
			return fmt.Errorf("executor-core-request should conform to the Kubernetes convention")
		}
		sparkResourceArgs.executorCoreRequest = executorCoreRequest

		executorMemory, err := cmd.Flags().GetString("executor-memory")
		if err != nil {
			return err
		}
		matchResult, err = regexp.MatchString(k8sQuantitiesReg, executorMemory)
		if err != nil || !matchResult {
			return fmt.Errorf("executor-memory should conform to the Kubernetes convention")
		}
		sparkResourceArgs.executorMemory = executorMemory

		kubeconfig, err := ResolveKubeConfig(cmd)
		if err != nil {
			return err
		}
		clientset, err := CreateK8sClient(kubeconfig)
		if err != nil {
			return fmt.Errorf("couldn't create k8s client using given kubeconfig, %v", err)
		}

		waitFlag, err := cmd.Flags().GetBool("wait")
		if err != nil {
			return err
		}

		err = PolicyRecoPreCheck(clientset)
		if err != nil {
			return err
		}

		recommendationID := uuid.New().String()
		recoJobArgs = append(recoJobArgs, "--id", recommendationID)
		recommendationApplication := &sparkv1.SparkApplication{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "sparkoperator.k8s.io/v1beta2",
				Kind:       "SparkApplication",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      "policy-reco-" + recommendationID,
				Namespace: flowVisibilityNS,
			},
			Spec: sparkv1.SparkApplicationSpec{
				Type:                "Python",
				SparkVersion:        sparkVersion,
				Mode:                "cluster",
				Image:               ConstStrToPointer(sparkImage),
				ImagePullPolicy:     ConstStrToPointer(sparkImagePullPolicy),
				MainApplicationFile: ConstStrToPointer(sparkAppFile),
				Arguments:           recoJobArgs,
				Driver: sparkv1.DriverSpec{
					CoreRequest: &driverCoreRequest,
					SparkPodSpec: sparkv1.SparkPodSpec{
						Memory: &driverMemory,
						Labels: map[string]string{
							"version": sparkVersion,
						},
						EnvSecretKeyRefs: map[string]sparkv1.NameKey{
							"CH_USERNAME": {
								Name: "clickhouse-secret",
								Key:  "username",
							},
							"CH_PASSWORD": {
								Name: "clickhouse-secret",
								Key:  "password",
							},
						},
						ServiceAccount: ConstStrToPointer(sparkServiceAccount),
					},
				},
				Executor: sparkv1.ExecutorSpec{
					CoreRequest: &executorCoreRequest,
					SparkPodSpec: sparkv1.SparkPodSpec{
						Memory: &executorMemory,
						Labels: map[string]string{
							"version": sparkVersion,
						},
						EnvSecretKeyRefs: map[string]sparkv1.NameKey{
							"CH_USERNAME": {
								Name: "clickhouse-secret",
								Key:  "username",
							},
							"CH_PASSWORD": {
								Name: "clickhouse-secret",
								Key:  "password",
							},
						},
					},
					Instances: &sparkResourceArgs.executorInstances,
				},
			},
		}
		response := &sparkv1.SparkApplication{}
		err = clientset.CoreV1().RESTClient().
			Post().
			AbsPath("/apis/sparkoperator.k8s.io/v1beta2").
			Namespace(flowVisibilityNS).
			Resource("sparkapplications").
			Body(recommendationApplication).
			Do(context.TODO()).
			Into(response)
		if err != nil {
			return err
		}
		if waitFlag {
			err = wait.Poll(statusCheckPollInterval, statusCheckPollTimeout, func() (bool, error) {
				state, err := getPolicyRecommendationStatus(clientset, recommendationID)
				if err != nil {
					return false, err
				}
				if state == "COMPLETED" {
					return true, nil
				}
				if state == "FAILED" || state == "SUBMISSION_FAILED" || state == "FAILING" || state == "INVALIDATING" {
					return false, fmt.Errorf("policy recommendation job failed, state: %s", state)
				} else {
					return false, nil
				}
			})
			if err != nil {
				return err
			}

			endpoint, err := cmd.Flags().GetString("clickhouse-endpoint")
			if err != nil {
				return err
			}
			if endpoint != "" {
				_, err := url.ParseRequestURI(endpoint)
				if err != nil {
					return fmt.Errorf("failed to decode input endpoint %s into a url, err: %v", endpoint, err)
				}
			}
			useClusterIP, err := cmd.Flags().GetBool("use-cluster-ip")
			if err != nil {
				return err
			}
			filePath, err := cmd.Flags().GetString("file")
			if err != nil {
				return err
			}
			if err := CheckClickHousePod(clientset); err != nil {
				return err
			}
			recoResult, err := getPolicyRecommendationResult(clientset, kubeconfig, endpoint, useClusterIP, filePath, recommendationID)
			if err != nil {
				return err
			} else {
				if recoResult != "" {
					fmt.Print(recoResult)
				}
			}
			return nil
		} else {
			fmt.Printf("Successfully created policy recommendation job with ID %s\n", recommendationID)
		}
		return nil
	},
}

func init() {
	policyRecommendationCmd.AddCommand(policyRecommendationRunCmd)
	policyRecommendationRunCmd.Flags().StringP(
		"type",
		"t",
		"initial",
		"{initial|subsequent} Indicates this recommendation is an initial recommendion or a subsequent recommendation job.",
	)
	policyRecommendationRunCmd.Flags().IntP(
		"limit",
		"l",
		0,
		"The limit on the number of flow records read from the database. 0 means no limit.",
	)
	policyRecommendationRunCmd.Flags().StringP(
		"option",
		"o",
		"anp-deny-applied",
		`Option of network isolation preference in policy recommendation.
Currently we support 3 options:
anp-deny-applied: Recommending allow ANP/ACNP policies, with default deny rules only on applied to Pod labels which have allow rules recommended.
anp-deny-all: Recommending allow ANP/ACNP policies, with default deny rules for whole cluster.
k8s-np: Recommending allow K8s network policies, with no deny rules at all`,
	)
	policyRecommendationRunCmd.Flags().StringP(
		"start-time",
		"s",
		"",
		`The start time of the flow records considered for the policy recommendation.
Format is YYYY-MM-DD hh:mm:ss in UTC timezone. No limit of the start time of flow records by default.`,
	)
	policyRecommendationRunCmd.Flags().StringP(
		"end-time",
		"e",
		"",
		`The end time of the flow records considered for the policy recommendation.
Format is YYYY-MM-DD hh:mm:ss in UTC timezone. No limit of the end time of flow records by default.`,
	)
	policyRecommendationRunCmd.Flags().StringP(
		"ns-allow-list",
		"n",
		"",
		`List of default traffic allow namespaces.
If no namespaces provided, Traffic inside Antrea CNI related namespaces: ['kube-system', 'flow-aggregator',
'flow-visibility'] will be allowed by default.`,
	)
	policyRecommendationRunCmd.Flags().Bool(
		"rm-labels",
		true,
		`Enable this option will remove automatically generated Pod labels including 'pod-template-hash',
'controller-revision-hash', 'pod-template-generation'.`,
	)
	policyRecommendationRunCmd.Flags().Bool(
		"to-services",
		true,
		`Use the toServices feature in ANP and recommendation toServices rules for Pod-to-Service flows,
only works when option is 1 or 2.`,
	)
	policyRecommendationRunCmd.Flags().Int32(
		"executor-instances",
		1,
		"Specify the number of executors for the spark application. Example values include 1, 2, 8, etc.",
	)
	policyRecommendationRunCmd.Flags().String(
		"driver-core-request",
		"200m",
		`Specify the cpu request for the driver Pod. Values conform to the Kubernetes convention.
Example values include 0.1, 500m, 1.5, 5, etc.`,
	)
	policyRecommendationRunCmd.Flags().String(
		"driver-memory",
		"512M",
		`Specify the memory request for the driver Pod. Values conform to the Kubernetes convention.
Example values include 512M, 1G, 8G, etc.`,
	)
	policyRecommendationRunCmd.Flags().String(
		"executor-core-request",
		"200m",
		`Specify the cpu request for the executor Pod. Values conform to the Kubernetes convention.
Example values include 0.1, 500m, 1.5, 5, etc.`,
	)
	policyRecommendationRunCmd.Flags().String(
		"executor-memory",
		"512M",
		`Specify the memory request for the executor Pod. Values conform to the Kubernetes convention.
Example values include 512M, 1G, 8G, etc.`,
	)
	policyRecommendationRunCmd.Flags().Bool(
		"wait",
		false,
		"Enable this option will hold and wait the whole policy recommendation job finished.",
	)
	policyRecommendationRunCmd.Flags().String(
		"clickhouse-endpoint",
		"",
		"The ClickHouse Service endpoint. (Only works when wait is enabled)",
	)
	policyRecommendationRunCmd.Flags().Bool(
		"use-cluster-ip",
		false,
		`Enable this option will use ClusterIP instead of port forwarding when connecting to the ClickHouse Service.
It can only be used when running in cluster. (Only works when wait is enabled)`,
	)
	policyRecommendationRunCmd.Flags().StringP(
		"file",
		"f",
		"",
		"The file path where you want to save the results. (Only works when wait is enabled)",
	)
}
