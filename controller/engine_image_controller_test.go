package controller

import (
	"fmt"
	appv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

type EngineImageControllerTestCase struct {
	// For DaemonSet related tests
	node *longhorn.Node

	// For ref count check
	volume *longhorn.Volume
	engine *longhorn.Engine

	// For ref count related test
	upgradedEngineImage *longhorn.EngineImage

	// For expired engine image cleanup
	defaultEngineImage string

	currentEngineImage    *longhorn.EngineImage
	currentEngineManager  *longhorn.InstanceManager
	currentReplicaManager *longhorn.InstanceManager
	currentDaemonSet      *appv1.DaemonSet

	expectedEngineImage    *longhorn.EngineImage
	expectedEngineManager  *longhorn.InstanceManager
	expectedReplicaManager *longhorn.InstanceManager
	expectedDaemonSet      *appv1.DaemonSet
}

func newTestEngineImageController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset) *EngineImageController {

	volumeInformer := lhInformerFactory.Longhorn().V1beta1().Volumes()
	engineInformer := lhInformerFactory.Longhorn().V1beta1().Engines()
	replicaInformer := lhInformerFactory.Longhorn().V1beta1().Replicas()
	engineImageInformer := lhInformerFactory.Longhorn().V1beta1().EngineImages()
	nodeInformer := lhInformerFactory.Longhorn().V1beta1().Nodes()
	settingInformer := lhInformerFactory.Longhorn().V1beta1().Settings()
	imInformer := lhInformerFactory.Longhorn().V1beta1().InstanceManagers()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	persistentVolumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	persistentVolumeClaimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1().DaemonSets()
	deploymentInformer := kubeInformerFactory.Apps().V1().Deployments()
	kubeNodeInformer := kubeInformerFactory.Core().V1().Nodes()

	// Skip the Lister check that occurs on creation of an Instance Manager.
	datastore.SkipListerCheck = true

	ds := datastore.NewDataStore(
		volumeInformer, engineInformer, replicaInformer,
		engineImageInformer, nodeInformer, settingInformer, imInformer,
		lhClient,
		podInformer, cronJobInformer, daemonSetInformer, deploymentInformer,
		persistentVolumeInformer, persistentVolumeClaimInformer, kubeNodeInformer,
		kubeClient, TestNamespace)

	ic := NewEngineImageController(
		ds, scheme.Scheme,
		engineImageInformer, volumeInformer, daemonSetInformer, nodeInformer, imInformer,
		kubeClient, TestNamespace, TestNode1, TestServiceAccount)

	fakeRecorder := record.NewFakeRecorder(100)
	ic.eventRecorder = fakeRecorder

	ic.iStoreSynced = alwaysReady
	ic.vStoreSynced = alwaysReady
	ic.dsStoreSynced = alwaysReady
	ic.nStoreSynced = alwaysReady
	ic.imStoreSynced = alwaysReady

	ic.nowHandler = getTestNow
	ic.engineBinaryChecker = fakeEngineBinaryChecker
	ic.engineImageVersionUpdater = fakeEngineImageUpdater
	ic.instanceManagerNameGenerator = fakeInstanceManagerNameGenerator

	return ic
}

func getEngineImageControllerTestTemplate() *EngineImageControllerTestCase {
	tc := &EngineImageControllerTestCase{
		node:                newNode(TestNode1, TestNamespace, true, types.ConditionStatusTrue, ""),
		volume:              newVolume(TestVolumeName, 2),
		engine:              newEngine(TestEngineName, TestEngineImage, TestEngineManagerName, TestIP1, 0, true, types.InstanceStateRunning, types.InstanceStateRunning),
		upgradedEngineImage: newEngineImage(TestUpgradedEngineImage, types.EngineImageStateReady),

		defaultEngineImage: TestEngineImage,

		currentEngineImage:    newEngineImage(TestEngineImage, types.EngineImageStateReady),
		currentEngineManager:  newInstanceManager(TestEngineManagerName, types.InstanceManagerTypeEngine, types.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
		currentReplicaManager: newInstanceManager(TestReplicaManagerName, types.InstanceManagerTypeReplica, types.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false),
		currentDaemonSet:      newEngineImageDaemonSet(),
	}

	tc.volume.Status.CurrentNodeID = TestNode1
	tc.volume.Status.State = types.VolumeStateAttached
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.currentEngineImage.Status.RefCount = 1

	return tc
}

func (tc *EngineImageControllerTestCase) copyCurrentToExpected() {
	if tc.currentEngineImage != nil {
		tc.expectedEngineImage = tc.currentEngineImage.DeepCopy()
	}
	if tc.currentEngineManager != nil {
		tc.expectedEngineManager = tc.currentEngineManager.DeepCopy()
	}
	if tc.currentReplicaManager != nil {
		tc.expectedReplicaManager = tc.currentReplicaManager.DeepCopy()
	}
	if tc.currentDaemonSet != nil {
		tc.expectedDaemonSet = tc.currentDaemonSet.DeepCopy()
	}
}

func generateEngineImageControllerTestCases() map[string]*EngineImageControllerTestCase {
	var tc *EngineImageControllerTestCase
	testCases := map[string]*EngineImageControllerTestCase{}

	// The TestNode2 is a non-existing node and is just used for node down test.
	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineImage.Status.OwnerID = TestNode2
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.OwnerID = TestNode1
	testCases["Engine image ownerID node is down"] = tc

	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineImage.Status.State = types.EngineImageStateDeploying
	tc.currentDaemonSet = nil
	tc.copyCurrentToExpected()
	tc.expectedDaemonSet = newEngineImageDaemonSet()
	tc.expectedDaemonSet.Status.NumberAvailable = 0
	testCases["Engine image DaemonSet creation"] = tc

	// The controller will rely on foreground deletion to clean up the dependants then remove finalizer for the deleting engine image
	// The fake k8s client won't apply foreground deletion to delete the dependants and clean up the object with DeletionTimestamp set and Finalizers unset,
	// hence the expectedEngineImage, expectedEngineManager, and expectedReplicaManager are not nil here.
	//tc = getEngineImageControllerTestTemplate()
	//deleteTime := metav1.Now()
	//tc.copyCurrentToExpected()
	//tc.expectedEngineImage.Finalizers = []string{}
	//tc.expectedEngineManager.DeletionTimestamp = nil
	//tc.expectedReplicaManager.DeletionTimestamp = nil
	//tc.expectedDaemonSet.DeletionTimestamp = nil
	//testCases["Engine image deletion"] = tc

	// The DaemonSet is not ready hence the engine image will become state `deploying`
	tc = getEngineImageControllerTestTemplate()
	tc.currentDaemonSet.Status.NumberAvailable = 0
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.State = types.EngineImageStateDeploying
	testCases["Engine Image DaemonSet pods are suddenly removed"] = tc

	// `ei.Status.refCount` should become 1 and `Status.NoRefSince` should be unset
	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineImage.Status.RefCount = 0
	tc.currentEngineImage.Status.NoRefSince = getTestNow()
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.RefCount = 1
	tc.expectedEngineImage.Status.NoRefSince = ""
	testCases["One volume starts to use the engine image"] = tc

	// The engine is still using the engine manager of current engine image after live upgrade,
	// hence the `currentEngineImage.Status.refCount` should keep 1
	tc = getEngineImageControllerTestTemplate()
	tc.volume.Spec.EngineImage = TestUpgradedEngineImage
	tc.volume.Status.CurrentImage = TestUpgradedEngineImage
	tc.engine.Spec.EngineImage = TestUpgradedEngineImage
	tc.engine.Status.CurrentImage = TestUpgradedEngineImage
	tc.engine.Status.InstanceManagerName = TestEngineManagerName
	tc.copyCurrentToExpected()
	testCases["Volume is using other engine image after live upgrade"] = tc

	// No volume is using the current engine image.
	tc = getEngineImageControllerTestTemplate()
	tc.volume = nil
	tc.engine = nil
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.RefCount = 0
	tc.expectedEngineImage.Status.NoRefSince = getTestNow()
	testCases["The default engine image won't be cleaned up even if there is no volume using it"] = tc

	// Since the finalizer mechanism doesn't work for fake k8s client, the expired engine image in this case
	// will be removed directly then lead to the following engine image update failure.
	// Comment it for now.
	//tc = getEngineImageControllerTestTemplate()
	//tc.volume = nil
	//tc.engine = nil
	//tc.defaultEngineImage = TestUpgradedEngineImage
	//tc.copyCurrentToExpected()
	//tc.expectedEngineImage = nil
	//testCases["Expired engine image cleanup"] = tc

	tc = getEngineImageControllerTestTemplate()
	deprecatedLabels := map[string]string{
		"longhorn": "engine-image",
	}
	tc.currentDaemonSet.Labels = deprecatedLabels
	tc.currentDaemonSet.Spec.Template.Labels = deprecatedLabels
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.State = types.EngineImageStateDeploying
	tc.expectedDaemonSet = nil
	testCases["DaemonSet with deprecated labels is found"] = tc

	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineImage.Status.State = types.EngineImageStateDeploying
	tc.currentEngineManager = nil
	tc.currentReplicaManager = nil
	tc.currentDaemonSet.Status.NumberAvailable = 0
	tc.copyCurrentToExpected()
	testCases["Engine manager or replica manager cannot be created before the DaemonSet is ready"] = tc

	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineImage.Status.State = types.EngineImageStateDeploying
	tc.currentEngineManager = nil
	tc.currentReplicaManager = nil
	tc.copyCurrentToExpected()
	tc.expectedEngineManager = newInstanceManager(TestEngineManagerName, types.InstanceManagerTypeEngine, "", TestNode1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false)
	tc.expectedReplicaManager = newInstanceManager(TestReplicaManagerName, types.InstanceManagerTypeReplica, "", TestNode1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false)
	testCases["Engine manager and replica manager creation"] = tc

	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineManager = nil
	tc.currentReplicaManager = nil
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.State = types.EngineImageStateDeploying
	tc.expectedEngineManager = newInstanceManager(TestEngineManagerName, types.InstanceManagerTypeEngine, "", TestNode1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false)
	tc.expectedReplicaManager = newInstanceManager(TestReplicaManagerName, types.InstanceManagerTypeReplica, "", TestNode1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false)
	testCases["Engine image becomes state deploying when recreating instance managers"] = tc

	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineImage.Status.State = types.EngineImageStateDeploying
	tc.currentEngineManager.Status.CurrentState = types.InstanceManagerStateStopped
	tc.copyCurrentToExpected()
	testCases["Engine image keeps state deploying when some instance managers are not ready"] = tc

	tc = getEngineImageControllerTestTemplate()
	tc.currentEngineImage.Status.State = types.EngineImageStateDeploying
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.State = types.EngineImageStateReady
	testCases["Engine image becomes state ready when the DaemonSet and all instance managers are running"] = tc

	tc = getEngineImageControllerTestTemplate()
	tc.node.Spec.Disks = map[string]types.DiskSpec{}
	tc.copyCurrentToExpected()
	tc.expectedReplicaManager = nil
	testCases["Replica Manager will be removed and the engine image keeps ready state when the node disk is empty"] = tc

	tc = getEngineImageControllerTestTemplate()
	tc.currentReplicaManager = nil
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.State = types.EngineImageStateDeploying
	tc.expectedReplicaManager = newInstanceManager(TestReplicaManagerName, types.InstanceManagerTypeReplica, "", TestNode1, TestNode1, TestIP1, map[string]types.InstanceProcess{}, false)
	testCases["Replica Manager will be created when the first disk is added to the node"] = tc

	tc = getEngineImageControllerTestTemplate()
	incompatibleVersion := types.EngineVersionDetails{
		Version:                 "ei.Spec.Image",
		GitCommit:               "unknown",
		BuildDate:               "unknown",
		CLIAPIVersion:           types.InvalidEngineVersion,
		CLIAPIMinVersion:        types.InvalidEngineVersion,
		ControllerAPIVersion:    types.InvalidEngineVersion,
		ControllerAPIMinVersion: types.InvalidEngineVersion,
		DataFormatVersion:       types.InvalidEngineVersion,
		DataFormatMinVersion:    types.InvalidEngineVersion,
	}
	tc.currentEngineImage.Status.EngineVersionDetails = incompatibleVersion
	tc.copyCurrentToExpected()
	tc.expectedEngineImage.Status.State = types.EngineImageStateIncompatible
	tc.expectedEngineManager = nil
	tc.expectedReplicaManager = nil
	testCases["Incompatible engine image cleanup"] = tc

	return testCases
}

func (s *TestSuite) TestEngineImage(c *C) {
	testCases := generateEngineImageControllerTestCases()
	for name, tc := range testCases {
		var err error
		fmt.Printf("Testing engine image controller: %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		dsIndexer := kubeInformerFactory.Apps().V1().DaemonSets().Informer().GetIndexer()

		nodeIndexer := lhInformerFactory.Longhorn().V1beta1().Nodes().Informer().GetIndexer()
		settingIndexer := lhInformerFactory.Longhorn().V1beta1().Settings().Informer().GetIndexer()
		eiIndexer := lhInformerFactory.Longhorn().V1beta1().EngineImages().Informer().GetIndexer()
		vIndexer := lhInformerFactory.Longhorn().V1beta1().Volumes().Informer().GetIndexer()
		eIndexer := lhInformerFactory.Longhorn().V1beta1().Engines().Informer().GetIndexer()
		imIndexer := lhInformerFactory.Longhorn().V1beta1().InstanceManagers().Informer().GetIndexer()

		ic := newTestEngineImageController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient)

		setting, err := lhClient.LonghornV1beta1().Settings(TestNamespace).Create(newSetting(string(types.SettingNameDefaultEngineImage), tc.defaultEngineImage))
		c.Assert(err, IsNil)
		err = settingIndexer.Add(setting)
		c.Assert(err, IsNil)
		// For DaemonSet creation test
		setting, err = lhClient.LonghornV1beta1().Settings(TestNamespace).Create(newSetting(string(types.SettingNameTaintToleration), ""))
		c.Assert(err, IsNil)
		err = settingIndexer.Add(setting)
		c.Assert(err, IsNil)

		node, err := lhClient.LonghornV1beta1().Nodes(TestNamespace).Create(tc.node)
		c.Assert(err, IsNil)
		err = nodeIndexer.Add(node)
		c.Assert(err, IsNil)

		ei, err := lhClient.LonghornV1beta1().EngineImages(TestNamespace).Create(tc.currentEngineImage)
		c.Assert(err, IsNil)
		err = eiIndexer.Add(ei)
		c.Assert(err, IsNil)
		ei, err = lhClient.LonghornV1beta1().EngineImages(TestNamespace).Create(tc.upgradedEngineImage)
		c.Assert(err, IsNil)
		err = eiIndexer.Add(ei)
		c.Assert(err, IsNil)

		if tc.volume != nil {
			v, err := lhClient.LonghornV1beta1().Volumes(TestNamespace).Create(tc.volume)
			c.Assert(err, IsNil)
			err = vIndexer.Add(v)
			c.Assert(err, IsNil)
		}
		if tc.engine != nil {
			e, err := lhClient.LonghornV1beta1().Engines(TestNamespace).Create(tc.engine)
			c.Assert(err, IsNil)
			err = eIndexer.Add(e)
			c.Assert(err, IsNil)
		}
		if tc.currentDaemonSet != nil {
			ds, err := kubeClient.AppsV1().DaemonSets(TestNamespace).Create(tc.currentDaemonSet)
			c.Assert(err, IsNil)
			err = dsIndexer.Add(ds)
			c.Assert(err, IsNil)
		}
		if tc.currentEngineManager != nil {
			em, err := lhClient.LonghornV1beta1().InstanceManagers(TestNamespace).Create(tc.currentEngineManager)
			c.Assert(err, IsNil)
			err = imIndexer.Add(em)
			c.Assert(err, IsNil)
		}
		if tc.currentReplicaManager != nil {
			rm, err := lhClient.LonghornV1beta1().InstanceManagers(TestNamespace).Create(tc.currentReplicaManager)
			c.Assert(err, IsNil)
			err = imIndexer.Add(rm)
			c.Assert(err, IsNil)
		}

		engineImageControllerKey := fmt.Sprintf("%s/%s", TestNamespace, getTestEngineImageName())
		err = ic.syncEngineImage(engineImageControllerKey)
		c.Assert(err, IsNil)

		ei, err = lhClient.LonghornV1beta1().EngineImages(TestNamespace).Get(getTestEngineImageName(), metav1.GetOptions{})
		if tc.expectedEngineImage == nil {
			c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
		} else {
			c.Assert(err, IsNil)
			c.Assert(ei.Status, DeepEquals, tc.expectedEngineImage.Status)
		}

		ds, err := kubeClient.AppsV1().DaemonSets(TestNamespace).Get(getTestEngineImageDaemonSetName(), metav1.GetOptions{})
		if tc.expectedDaemonSet == nil {
			c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
		} else {
			c.Assert(err, IsNil)
			// For the DaemonSet created by the fake k8s client, the field `Status.DesiredNumberScheduled` won't be set automatically.
			ds.Status.DesiredNumberScheduled = 1
			c.Assert(ds.Status, DeepEquals, tc.expectedDaemonSet.Status)
		}
		em, err := lhClient.LonghornV1beta1().InstanceManagers(TestNamespace).Get(TestEngineManagerName, metav1.GetOptions{})
		if tc.expectedEngineManager == nil {
			c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
		} else {
			c.Assert(err, IsNil)
			c.Assert(em.Status.CurrentState, DeepEquals, tc.expectedEngineManager.Status.CurrentState)
		}
		rm, err := lhClient.LonghornV1beta1().InstanceManagers(TestNamespace).Get(TestReplicaManagerName, metav1.GetOptions{})
		if tc.expectedReplicaManager == nil {
			c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
		} else {
			c.Assert(err, IsNil)
			c.Assert(rm.Status.CurrentState, DeepEquals, tc.expectedReplicaManager.Status.CurrentState)
		}
	}
}
