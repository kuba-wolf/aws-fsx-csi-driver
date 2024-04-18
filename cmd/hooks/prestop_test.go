// Copyright 2024 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hooks

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	driverMocks "sigs.k8s.io/aws-fsx-csi-driver/pkg/driver/mocks"
)

func TestPreStopHook(t *testing.T) {
	testCases := []struct {
		name     string
		nodeName string
		expErr   error
		mockFunc func(string, *driverMocks.MockKubernetesClient, *driverMocks.MockCoreV1Interface, *driverMocks.MockNodeInterface, *driverMocks.MockPodInterface) error
	}{
		{
			name:     "TestPreStopHook: CSI_NODE_NAME not set",
			nodeName: "",
			expErr:   fmt.Errorf("PreStop: CSI_NODE_NAME missing"),
			mockFunc: func(nodeName string, mockClient *driverMocks.MockKubernetesClient, mockCoreV1 *driverMocks.MockCoreV1Interface, mockNode *driverMocks.MockNodeInterface, mockPod *driverMocks.MockPodInterface) error {
				return nil
			},
		},
		{
			name:     "TestPreStopHook: failed to retrieve node information",
			nodeName: "test-node",
			expErr:   fmt.Errorf("fetchNode: failed to retrieve node information: non-existent node"),
			mockFunc: func(nodeName string, mockClient *driverMocks.MockKubernetesClient, mockCoreV1 *driverMocks.MockCoreV1Interface, mockNode *driverMocks.MockNodeInterface, mockPod *driverMocks.MockPodInterface) error {
				mockClient.EXPECT().CoreV1().Return(mockCoreV1).Times(1)
				mockCoreV1.EXPECT().Nodes().Return(mockNode).Times(1)
				mockNode.EXPECT().Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).Return(nil, fmt.Errorf("non-existent node")).Times(1)

				return nil
			},
		},
		{
			name:     "TestPreStopHook: node is not being drained, skipping Pods check - missing TaintNodeUnschedulable",
			nodeName: "test-node",
			expErr:   nil,
			mockFunc: func(nodeName string, mockClient *driverMocks.MockKubernetesClient, mockCoreV1 *driverMocks.MockCoreV1Interface, mockNode *driverMocks.MockNodeInterface, mockPod *driverMocks.MockPodInterface) error {
				mockNodeObj := &v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: v1.NodeSpec{
						Taints: []v1.Taint{},
					},
				}

				mockClient.EXPECT().CoreV1().Return(mockCoreV1).Times(1)
				mockCoreV1.EXPECT().Nodes().Return(mockNode).Times(1)
				mockNode.EXPECT().Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).Return(mockNodeObj, nil).Times(1)

				return nil
			},
		},
		{
			name:     "TestPreStopHook: node is being drained, pods associated with node don't have PVCs",
			nodeName: "test-node",
			expErr:   nil,
			mockFunc: func(nodeName string, mockClient *driverMocks.MockKubernetesClient, mockCoreV1 *driverMocks.MockCoreV1Interface, mockNode *driverMocks.MockNodeInterface, mockPod *driverMocks.MockPodInterface) error {

				fakeNode := &v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: v1.NodeSpec{
						Taints: []v1.Taint{
							{
								Key:    v1.TaintNodeUnschedulable,
								Effect: v1.TaintEffectNoSchedule,
							},
						},
					},
				}

				fakePods := &v1.PodList{
					Items: []v1.Pod{
						{
							Spec: v1.PodSpec{
								NodeName: "test-node",
								Volumes: []v1.Volume{
									{
										Name: "vol1",
										VolumeSource: v1.VolumeSource{
											EmptyDir: &v1.EmptyDirVolumeSource{},
										},
									},
								},
							},
						},
					},
				}

				mockClient.EXPECT().CoreV1().Return(mockCoreV1).AnyTimes()

				mockCoreV1.EXPECT().Nodes().Return(mockNode).AnyTimes()
				mockNode.EXPECT().Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).Return(fakeNode, nil).AnyTimes()

				mockCoreV1.EXPECT().Pods("").Return(mockPod).AnyTimes()
				mockPod.EXPECT().List(gomock.Any(), gomock.Any()).Return(fakePods, nil).AnyTimes()
				mockPod.EXPECT().Watch(gomock.Any(), gomock.Any()).Return(watch.NewFake(), nil).AnyTimes()

				return nil
			},
		},
		{
			name:     "TestPreStopHook: node is being drained, pods on other nodes have PVCs",
			nodeName: "test-node",
			expErr:   nil,
			mockFunc: func(nodeName string, mockClient *driverMocks.MockKubernetesClient, mockCoreV1 *driverMocks.MockCoreV1Interface, mockNode *driverMocks.MockNodeInterface, mockPod *driverMocks.MockPodInterface) error {

				fakeNode := &v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: v1.NodeSpec{
						Taints: []v1.Taint{
							{
								Key:    v1.TaintNodeUnschedulable,
								Effect: v1.TaintEffectNoSchedule,
							},
						},
					},
				}

				fakePods := &v1.PodList{
					Items: []v1.Pod{
						{
							Spec: v1.PodSpec{
								NodeName: "test-node-2",
								Volumes: []v1.Volume{
									{
										Name: "vol1",
										VolumeSource: v1.VolumeSource{
											PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{},
										},
									},
								},
							},
						},
					},
				}

				mockClient.EXPECT().CoreV1().Return(mockCoreV1).AnyTimes()

				mockCoreV1.EXPECT().Nodes().Return(mockNode).AnyTimes()
				mockNode.EXPECT().Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).Return(fakeNode, nil).AnyTimes()

				mockCoreV1.EXPECT().Pods("").Return(mockPod).AnyTimes()
				mockPod.EXPECT().List(gomock.Any(), gomock.Any()).Return(fakePods, nil).AnyTimes()
				mockPod.EXPECT().Watch(gomock.Any(), gomock.Any()).Return(watch.NewFake(), nil).AnyTimes()

				return nil
			},
		},
		{
			name:     "TestPreStopHook: Node is drained before timeout",
			nodeName: "test-node",
			expErr:   nil,
			mockFunc: func(nodeName string, mockClient *driverMocks.MockKubernetesClient, mockCoreV1 *driverMocks.MockCoreV1Interface, mockNode *driverMocks.MockNodeInterface, mockPod *driverMocks.MockPodInterface) error {

				fakeNode := &v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: v1.NodeSpec{
						Taints: []v1.Taint{
							{
								Key:    v1.TaintNodeUnschedulable,
								Effect: v1.TaintEffectNoSchedule,
							},
						},
					},
				}

				fakePods := &v1.PodList{
					Items: []v1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "pod-1",
								Namespace: "testspace",
							},
							Spec: v1.PodSpec{
								NodeName: "test-node",
								Volumes: []v1.Volume{
									{
										Name: "vol1",
										VolumeSource: v1.VolumeSource{
											PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{},
										},
									},
								},
							},
						},
					},
				}

				fakeWatcher := watch.NewFake()
				deleteSignal := make(chan bool, 1)

				mockClient.EXPECT().CoreV1().Return(mockCoreV1).AnyTimes()

				mockCoreV1.EXPECT().Nodes().Return(mockNode).AnyTimes()
				mockNode.EXPECT().Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).Return(fakeNode, nil).AnyTimes()

				mockCoreV1.EXPECT().Pods("").Return(mockPod).AnyTimes()
				gomock.InOrder(
					mockPod.EXPECT().List(gomock.Any(), gomock.Any()).Return(fakePods, nil).AnyTimes(),
					mockPod.EXPECT().Watch(gomock.Any(), gomock.Any()).DoAndReturn(func(signal, watchSignal interface{}) (watch.Interface, error) {
						deleteSignal <- true
						return fakeWatcher, nil
					}).AnyTimes(),
					mockPod.EXPECT().List(gomock.Any(), gomock.Any()).Return(&v1.PodList{Items: []v1.Pod{}}, nil).AnyTimes(),
				)

				go func() {
					<-deleteSignal
					fakeWatcher.Delete(&v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod-1",
							Namespace: "testspace",
						},
					})
				}()
				return nil
			},
		},
		{
			name:     "TestPreStopHook: Karpenter node is being drained, pods associated with node don't have PVCs",
			nodeName: "test-karpenter-node",
			expErr:   nil,
			mockFunc: func(nodeName string, mockClient *driverMocks.MockKubernetesClient, mockCoreV1 *driverMocks.MockCoreV1Interface, mockNode *driverMocks.MockNodeInterface, mockPod *driverMocks.MockPodInterface) error {

				fakeNode := &v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: v1.NodeSpec{
						Taints: []v1.Taint{
							{
								Key:    v1beta1KarpenterTaint,
								Effect: v1.TaintEffectNoSchedule,
							},
						},
					},
				}

				fakePods := &v1.PodList{
					Items: []v1.Pod{
						{
							Spec: v1.PodSpec{
								NodeName: "test-node",
								Volumes: []v1.Volume{
									{
										Name: "vol1",
										VolumeSource: v1.VolumeSource{
											EmptyDir: &v1.EmptyDirVolumeSource{},
										},
									},
								},
							},
						},
					},
				}

				mockClient.EXPECT().CoreV1().Return(mockCoreV1).AnyTimes()

				mockCoreV1.EXPECT().Nodes().Return(mockNode).AnyTimes()
				mockNode.EXPECT().Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).Return(fakeNode, nil).AnyTimes()

				mockCoreV1.EXPECT().Pods("").Return(mockPod).AnyTimes()
				mockPod.EXPECT().List(gomock.Any(), gomock.Any()).Return(fakePods, nil).AnyTimes()
				mockPod.EXPECT().Watch(gomock.Any(), gomock.Any()).Return(watch.NewFake(), nil).AnyTimes()

				return nil
			},
		},
		{
			name:     "TestPreStopHook: Karpenter node is being drained, pods on other nodes have PVCs",
			nodeName: "test-node",
			expErr:   nil,
			mockFunc: func(nodeName string, mockClient *driverMocks.MockKubernetesClient, mockCoreV1 *driverMocks.MockCoreV1Interface, mockNode *driverMocks.MockNodeInterface, mockPod *driverMocks.MockPodInterface) error {

				fakeNode := &v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: v1.NodeSpec{
						Taints: []v1.Taint{
							{
								Key:    v1beta1KarpenterTaint,
								Effect: v1.TaintEffectNoSchedule,
							},
						},
					},
				}

				fakePods := &v1.PodList{
					Items: []v1.Pod{
						{
							Spec: v1.PodSpec{
								NodeName: "test-node-2",
								Volumes: []v1.Volume{
									{
										Name: "vol1",
										VolumeSource: v1.VolumeSource{
											PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{},
										},
									},
								},
							},
						},
					},
				}

				mockClient.EXPECT().CoreV1().Return(mockCoreV1).AnyTimes()

				mockCoreV1.EXPECT().Nodes().Return(mockNode).AnyTimes()
				mockNode.EXPECT().Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).Return(fakeNode, nil).AnyTimes()

				mockCoreV1.EXPECT().Pods("").Return(mockPod).AnyTimes()
				mockPod.EXPECT().List(gomock.Any(), gomock.Any()).Return(fakePods, nil).AnyTimes()
				mockPod.EXPECT().Watch(gomock.Any(), gomock.Any()).Return(watch.NewFake(), nil).AnyTimes()

				return nil
			},
		},
		{
			name:     "TestPreStopHook: Karpenter Node is drained before timeout",
			nodeName: "test-karpenter-node",
			expErr:   nil,
			mockFunc: func(nodeName string, mockClient *driverMocks.MockKubernetesClient, mockCoreV1 *driverMocks.MockCoreV1Interface, mockNode *driverMocks.MockNodeInterface, mockPod *driverMocks.MockPodInterface) error {

				fakeNode := &v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: nodeName,
					},
					Spec: v1.NodeSpec{
						Taints: []v1.Taint{
							{
								Key:    v1beta1KarpenterTaint,
								Effect: v1.TaintEffectNoSchedule,
							},
						},
					},
				}

				fakePods := &v1.PodList{
					Items: []v1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "pod-1",
								Namespace: "testspace",
							},
							Spec: v1.PodSpec{
								NodeName: "test-node",
								Volumes: []v1.Volume{
									{
										Name: "vol1",
										VolumeSource: v1.VolumeSource{
											PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{},
										},
									},
								},
							},
						},
					},
				}

				fakeWatcher := watch.NewFake()
				deleteSignal := make(chan bool, 1)

				mockClient.EXPECT().CoreV1().Return(mockCoreV1).AnyTimes()

				mockCoreV1.EXPECT().Nodes().Return(mockNode).AnyTimes()
				mockNode.EXPECT().Get(gomock.Any(), gomock.Eq(nodeName), gomock.Any()).Return(fakeNode, nil).AnyTimes()

				mockCoreV1.EXPECT().Pods("").Return(mockPod).AnyTimes()
				gomock.InOrder(
					mockPod.EXPECT().List(gomock.Any(), gomock.Any()).Return(fakePods, nil).AnyTimes(),
					mockPod.EXPECT().Watch(gomock.Any(), gomock.Any()).DoAndReturn(func(signal, watchSignal interface{}) (watch.Interface, error) {
						deleteSignal <- true
						return fakeWatcher, nil
					}).AnyTimes(),
					mockPod.EXPECT().List(gomock.Any(), gomock.Any()).Return(&v1.PodList{Items: []v1.Pod{}}, nil).AnyTimes(),
				)

				go func() {
					<-deleteSignal
					fakeWatcher.Delete(&v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "pod-1",
							Namespace: "testspace",
						},
					})
				}()
				return nil
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockCtl := gomock.NewController(t)
			defer mockCtl.Finish()

			mockClient := driverMocks.NewMockKubernetesClient(mockCtl)
			mockCoreV1 := driverMocks.NewMockCoreV1Interface(mockCtl)
			mockNode := driverMocks.NewMockNodeInterface(mockCtl)
			mockPod := driverMocks.NewMockPodInterface(mockCtl)

			if tc.mockFunc != nil {
				err := tc.mockFunc(tc.nodeName, mockClient, mockCoreV1, mockNode, mockPod)
				if err != nil {
					t.Fatalf("TestPreStopHook: mockFunc returned error: %v", err)
				}
			}

			if tc.nodeName != "" {
				t.Setenv("CSI_NODE_NAME", tc.nodeName)
			}

			err := PreStop(mockClient)

			if tc.expErr != nil {
				require.Error(t, err)
				assert.Equal(t, tc.expErr.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
