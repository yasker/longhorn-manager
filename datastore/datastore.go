package datastore

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	appsinformers_v1beta2 "k8s.io/client-go/informers/apps/v1beta2"
	batchinformers_v1beta1 "k8s.io/client-go/informers/batch/v1beta1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	appslisters_v1beta2 "k8s.io/client-go/listers/apps/v1beta2"
	batchlisters_v1beta1 "k8s.io/client-go/listers/batch/v1beta1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"

	lhclientset "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

type DataStore struct {
	namespace string

	lhClient   lhclientset.Interface
	kubeClient clientset.Interface

	pLister        corelisters.PodLister
	pStoreSynced   cache.InformerSynced
	cjLister       batchlisters_v1beta1.CronJobLister
	cjStoreSynced  cache.InformerSynced
	dsLister       appslisters_v1beta2.DaemonSetLister
	dsStoreSynced  cache.InformerSynced
	pvLister       corelisters.PersistentVolumeLister
	pvStoreSynced  cache.InformerSynced
	pvcLister      corelisters.PersistentVolumeClaimLister
	pvcStoreSynced cache.InformerSynced
}

func NewDataStore(
	lhClient lhclientset.Interface,

	podInformer coreinformers.PodInformer,
	cronJobInformer batchinformers_v1beta1.CronJobInformer,
	daemonSetInformer appsinformers_v1beta2.DaemonSetInformer,
	persistentVolumeInformer coreinformers.PersistentVolumeInformer,
	persistentVolumeClaimInformer coreinformers.PersistentVolumeClaimInformer,

	kubeClient clientset.Interface,
	namespace string) *DataStore {

	return &DataStore{
		namespace: namespace,

		lhClient:   lhClient,
		kubeClient: kubeClient,

		pLister:        podInformer.Lister(),
		pStoreSynced:   podInformer.Informer().HasSynced,
		cjLister:       cronJobInformer.Lister(),
		cjStoreSynced:  cronJobInformer.Informer().HasSynced,
		dsLister:       daemonSetInformer.Lister(),
		dsStoreSynced:  daemonSetInformer.Informer().HasSynced,
		pvLister:       persistentVolumeInformer.Lister(),
		pvStoreSynced:  persistentVolumeInformer.Informer().HasSynced,
		pvcLister:      persistentVolumeClaimInformer.Lister(),
		pvcStoreSynced: persistentVolumeClaimInformer.Informer().HasSynced,
	}
}

func (s *DataStore) Sync(stopCh <-chan struct{}) bool {
	return controller.WaitForCacheSync("longhorn datastore", stopCh,
		s.pStoreSynced, s.cjStoreSynced, s.dsStoreSynced,
		s.pvStoreSynced, s.pvcStoreSynced)
}

func ErrorIsNotFound(err error) bool {
	return apierrors.IsNotFound(err)
}

func ErrorIsConflict(err error) bool {
	return apierrors.IsConflict(err)
}
