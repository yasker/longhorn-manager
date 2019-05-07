package controller

import (
	"fmt"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhfake "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

const (
	ManagerPodUp     = "managerPodUp"
	ManagerPodDown   = "managerPodDown"
	KubeNodeDown     = "kubeNodeDown"
	KubeNodePressure = "kubeNodePressure"
)

var MountPropagationBidirectional = v1.MountPropagationBidirectional

type NodeTestCase struct {
	nodes     map[string]*longhorn.Node
	pods      map[string]*v1.Pod
	replicas  []*longhorn.Replica
	kubeNodes map[string]*v1.Node

	expectNodeStatus map[string]types.NodeStatus
}

func newTestNodeController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset, controllerID string) *NodeController {
	replicaInformer := lhInformerFactory.Longhorn().V1alpha1().Replicas()
	engineImageInformer := lhInformerFactory.Longhorn().V1alpha1().EngineImages()
	nodeInformer := lhInformerFactory.Longhorn().V1alpha1().Nodes()
	settingInformer := lhInformerFactory.Longhorn().V1alpha1().Settings()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	kubeNodeInformer := kubeInformerFactory.Core().V1().Nodes()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1beta2().DaemonSets()
	persistentVolumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	persistentVolumeClaimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()

	ds := datastore.NewDataStore(
		engineImageInformer,
		lhClient,
		podInformer, cronJobInformer, daemonSetInformer,
		persistentVolumeInformer, persistentVolumeClaimInformer,
		kubeClient, TestNamespace)

	nc := NewNodeController(ds, scheme.Scheme, nodeInformer, settingInformer, podInformer, replicaInformer, kubeNodeInformer, kubeClient, TestNamespace, controllerID)
	fakeRecorder := record.NewFakeRecorder(100)
	nc.eventRecorder = fakeRecorder
	nc.getDiskInfoHandler = fakeGetDiskInfo

	nc.nStoreSynced = alwaysReady
	nc.pStoreSynced = alwaysReady

	return nc
}

func fakeGetDiskInfo(directory string) (*util.DiskInfo, error) {
	return &util.DiskInfo{
		Fsid:       "fsid",
		Path:       directory,
		Type:       "ext4",
		FreeBlock:  0,
		TotalBlock: 0,
		BlockSize:  0,

		StorageMaximum:   0,
		StorageAvailable: 0,
	}, nil
}

func generateKubeNodes(testType string) map[string]*v1.Node {
	var kubeNode1, kubeNode2 *v1.Node
	switch testType {
	case KubeNodeDown:
		kubeNode1 = newKubernetesNode(TestNode1, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
		kubeNode2 = newKubernetesNode(TestNode2, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
	case KubeNodePressure:
		kubeNode1 = newKubernetesNode(TestNode1, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
		kubeNode2 = newKubernetesNode(TestNode2, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
	default:
		kubeNode1 = newKubernetesNode(TestNode1, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
		kubeNode2 = newKubernetesNode(TestNode2, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionTrue)
	}
	return map[string]*v1.Node{
		TestNode1: kubeNode1,
		TestNode2: kubeNode2,
	}
}

func generateManagerPod(testType string) map[string]*v1.Pod {
	var daemon1, daemon2 *v1.Pod
	switch testType {
	case ManagerPodDown:
		daemon1 = newDaemonPod(v1.PodFailed, TestDaemon1, TestNamespace, TestNode1, TestIP1, nil)
		daemon2 = newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, nil)
	default:
		daemon1 = newDaemonPod(v1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, &MountPropagationBidirectional)
		daemon2 = newDaemonPod(v1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, &MountPropagationBidirectional)
	}
	return map[string]*v1.Pod{
		TestDaemon1: daemon1,
		TestDaemon2: daemon2,
	}
}

func kubeObjStatusSyncTest(testType string) *NodeTestCase {
	tc := &NodeTestCase{}
	tc.kubeNodes = generateKubeNodes(testType)
	node1 := newNode(TestNode1, TestNamespace, true, types.ConditionStatusUnknown, "")
	node2 := newNode(TestNode2, TestNamespace, true, types.ConditionStatusUnknown, "")
	nodes := map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.nodes = nodes
	nodeStatus := map[string]types.NodeStatus{}
	switch testType {
	case ManagerPodUp:
		nodeStatus = map[string]types.NodeStatus{
			TestNode1: {
				Conditions: map[types.NodeConditionType]types.Condition{
					types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
					types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
				},
			},
			TestNode2: {
				Conditions: map[types.NodeConditionType]types.Condition{
					types.NodeConditionTypeReady: newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				},
			},
		}
	case ManagerPodDown:
		nodeStatus = map[string]types.NodeStatus{
			TestNode1: {
				Conditions: map[types.NodeConditionType]types.Condition{
					types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusFalse, types.NodeConditionReasonManagerPodDown),
					types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusFalse, types.NodeConditionReasonNoMountPropagationSupport),
				},
			},
			TestNode2: {
				Conditions: map[types.NodeConditionType]types.Condition{
					types.NodeConditionTypeReady: newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				},
			},
		}
	case KubeNodeDown:
		nodeStatus = map[string]types.NodeStatus{
			TestNode1: {
				Conditions: map[types.NodeConditionType]types.Condition{
					types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusFalse, types.NodeConditionReasonKubernetesNodeNotReady),
					types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
				},
			},
			TestNode2: {
				Conditions: map[types.NodeConditionType]types.Condition{
					types.NodeConditionTypeReady: newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				},
			},
		}
	case KubeNodePressure:
		nodeStatus = map[string]types.NodeStatus{
			TestNode1: {
				Conditions: map[types.NodeConditionType]types.Condition{
					types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusFalse, types.NodeConditionReasonKubernetesNodePressure),
					types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
				},
			},
			TestNode2: {
				Conditions: map[types.NodeConditionType]types.Condition{
					types.NodeConditionTypeReady: newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				},
			},
		}
	}
	tc.pods = generateManagerPod(testType)

	tc.expectNodeStatus = nodeStatus

	return tc
}

func (s *TestSuite) TestSyncNode(c *C) {
	testCases := map[string]*NodeTestCase{}
	testCases["manager pod up"] = kubeObjStatusSyncTest(ManagerPodUp)
	testCases["manager pod down"] = kubeObjStatusSyncTest(ManagerPodDown)
	testCases["kubernetes node down"] = kubeObjStatusSyncTest(KubeNodeDown)
	testCases["kubernetes node pressure"] = kubeObjStatusSyncTest(KubeNodePressure)

	tc := &NodeTestCase{}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	node1 := newNode(TestNode1, TestNamespace, true, types.ConditionStatusTrue, "")
	node1.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
		},
	}
	node2 := newNode(TestNode2, TestNamespace, true, types.ConditionStatusTrue, "")
	node2.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Conditions: map[types.DiskConditionType]types.Condition{
				types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusUnknown, ""),
			},
		},
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	volume := newVolume(TestVolumeName, 2)
	engine := newEngineForVolume(volume)
	replica1 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1)
	replica2 := newReplicaForVolume(volume, engine, TestNode2, TestDiskID1)
	replicas := []*longhorn.Replica{replica1, replica2}
	tc.replicas = replicas

	tc.expectNodeStatus = map[string]types.NodeStatus{
		TestNode1: {
			Conditions: map[types.NodeConditionType]types.Condition{
				types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
			},
			DiskStatus: map[string]types.DiskStatus{
				TestDiskID1: {
					StorageScheduled: TestVolumeSize,
					Conditions: map[types.DiskConditionType]types.Condition{
						types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusFalse, string(types.DiskConditionReasonDiskPressure)),
						types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusTrue, ""),
					},
					ScheduledReplica: map[string]int64{
						replica1.Name: replica1.Spec.VolumeSize,
					},
				},
			},
		},
		TestNode2: {
			Conditions: map[types.NodeConditionType]types.Condition{
				types.NodeConditionTypeReady: newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
			},
			DiskStatus: map[string]types.DiskStatus{
				TestDiskID1: {
					StorageScheduled: 0,
					StorageAvailable: 0,
					Conditions: map[types.DiskConditionType]types.Condition{
						types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusUnknown, ""),
					},
				},
			},
		},
	}
	testCases["only disk on node1 should be updated status"] = tc

	tc = &NodeTestCase{}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	node1 = newNode(TestNode1, TestNamespace, true, types.ConditionStatusTrue, "")
	node1.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Conditions: map[types.DiskConditionType]types.Condition{
				types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusTrue, ""),
			},
		},
		"unavailable-disk": {
			StorageScheduled: 0,
			StorageAvailable: 0,
			Conditions: map[types.DiskConditionType]types.Condition{
				types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusTrue, ""),
			},
		},
	}
	node2 = newNode(TestNode2, TestNamespace, true, types.ConditionStatusTrue, "")
	node2.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
		},
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.expectNodeStatus = map[string]types.NodeStatus{
		TestNode1: {
			Conditions: map[types.NodeConditionType]types.Condition{
				types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
			},
			DiskStatus: map[string]types.DiskStatus{
				TestDiskID1: {
					StorageScheduled: 0,
					StorageAvailable: 0,
					Conditions: map[types.DiskConditionType]types.Condition{
						types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusFalse, string(types.DiskConditionReasonDiskPressure)),
						types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusTrue, ""),
					},
					ScheduledReplica: map[string]int64{},
				},
			},
		},
		TestNode2: {
			Conditions: map[types.NodeConditionType]types.Condition{
				types.NodeConditionTypeReady: newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
			},
			DiskStatus: map[string]types.DiskStatus{
				TestDiskID1: {
					StorageScheduled: 0,
					StorageAvailable: 0,
				},
			},
		},
	}
	testCases["clean disk status when disk removed from the node spec"] = tc

	tc = &NodeTestCase{}
	tc.kubeNodes = generateKubeNodes(ManagerPodUp)
	tc.pods = generateManagerPod(ManagerPodUp)
	node1 = newNode(TestNode1, TestNamespace, true, types.ConditionStatusTrue, "")
	node1.Spec.Disks = map[string]types.DiskSpec{
		"changedId": {
			Path:            TestDefaultDataPath,
			AllowScheduling: true,
			StorageReserved: 0,
		},
	}
	node1.Status.DiskStatus = map[string]types.DiskStatus{
		"changedId": {
			StorageScheduled: 0,
			StorageAvailable: 0,
			StorageMaximum:   TestDiskSize,
			Conditions: map[types.DiskConditionType]types.Condition{
				types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusTrue, ""),
				types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusTrue, ""),
			},
		},
	}
	node2 = newNode(TestNode2, TestNamespace, true, types.ConditionStatusTrue, "")
	node2.Status.DiskStatus = map[string]types.DiskStatus{
		TestDiskID1: {
			StorageScheduled: 0,
			StorageAvailable: 0,
		},
	}
	tc.nodes = map[string]*longhorn.Node{
		TestNode1: node1,
		TestNode2: node2,
	}
	tc.expectNodeStatus = map[string]types.NodeStatus{
		TestNode1: {
			Conditions: map[types.NodeConditionType]types.Condition{
				types.NodeConditionTypeReady:            newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
				types.NodeConditionTypeMountPropagation: newNodeCondition(types.NodeConditionTypeMountPropagation, types.ConditionStatusTrue, ""),
			},
			DiskStatus: map[string]types.DiskStatus{
				"changedId": {
					StorageScheduled: 0,
					StorageAvailable: 0,
					Conditions: map[types.DiskConditionType]types.Condition{
						types.DiskConditionTypeSchedulable: newNodeCondition(types.DiskConditionTypeSchedulable, types.ConditionStatusFalse, string(types.DiskConditionReasonDiskPressure)),
						types.DiskConditionTypeReady:       newNodeCondition(types.DiskConditionTypeReady, types.ConditionStatusFalse, string(types.DiskConditionReasonDiskFilesystemChanged)),
					},
					ScheduledReplica: map[string]int64{},
				},
			},
		},
		TestNode2: {
			Conditions: map[types.NodeConditionType]types.Condition{
				types.NodeConditionTypeReady: newNodeCondition(types.NodeConditionTypeReady, types.ConditionStatusTrue, ""),
			},
			DiskStatus: map[string]types.DiskStatus{
				TestDiskID1: {
					StorageScheduled: 0,
					StorageAvailable: 0,
				},
			},
		},
	}
	testCases["test disable disk when file system changed"] = tc

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)
		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		nIndexer := lhInformerFactory.Longhorn().V1alpha1().Nodes().Informer().GetIndexer()
		pIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

		knIndexer := kubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

		// create kuberentes node
		for _, kubeNode := range tc.kubeNodes {
			n, err := kubeClient.CoreV1().Nodes().Create(kubeNode)
			c.Assert(err, IsNil)
			knIndexer.Add(n)
		}

		nc := newTestNodeController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient, TestNode1)
		// create manager pod
		for _, pod := range tc.pods {
			p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(pod)
			c.Assert(err, IsNil)
			pIndexer.Add(p)
		}
		// create node
		for _, node := range tc.nodes {
			n, err := lhClient.Longhorn().Nodes(TestNamespace).Create(node)
			c.Assert(err, IsNil)
			c.Assert(n, NotNil)
			nIndexer.Add(n)
		}
		// create replicas
		for _, replica := range tc.replicas {
			_, err := lhClient.Longhorn().Replicas(TestNamespace).Create(replica)
			c.Assert(err, IsNil)
		}
		// sync node status
		for nodeName, node := range tc.nodes {
			err := nc.syncNode(getKey(node, c))
			c.Assert(err, IsNil)

			n, err := lhClient.LonghornV1alpha1().Nodes(TestNamespace).Get(node.Name, metav1.GetOptions{})
			c.Assert(err, IsNil)
			for ctype, condition := range n.Status.Conditions {
				/*
					if condition.Status != types.ConditionStatusUnknown {
						c.Assert(condition.LastProbeTime, Not(Equals), "")
					}
				*/
				condition.LastProbeTime = ""
				condition.LastTransitionTime = ""
				condition.Message = ""
				n.Status.Conditions[ctype] = condition
			}
			c.Assert(n.Status.Conditions, DeepEquals, tc.expectNodeStatus[nodeName].Conditions)
			if len(tc.expectNodeStatus[nodeName].DiskStatus) > 0 {
				diskConditions := n.Status.DiskStatus
				for fsid, diskStatus := range diskConditions {
					for ctype, condition := range diskStatus.Conditions {
						if condition.Status != types.ConditionStatusUnknown {
							//c.Assert(condition.LastProbeTime, Not(Equals), "")
							c.Assert(condition.LastTransitionTime, Not(Equals), "")
						}
						condition.LastProbeTime = ""
						condition.LastTransitionTime = ""
						condition.Message = ""
						diskStatus.Conditions[ctype] = condition
					}
					n.Status.DiskStatus[fsid] = diskStatus
				}
				c.Assert(n.Status.DiskStatus, DeepEquals, tc.expectNodeStatus[nodeName].DiskStatus)
			}
		}

	}
}
