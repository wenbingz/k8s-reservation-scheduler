package scheduler

import (
	"context"
	"fmt"
	schedulerconfig "github.com/wenbingz/k8s-reservation-scheduler/config/scheduler"
	"github.com/wenbingz/k8s-resource-reservation/api/v1alpha1"
	controller "github.com/wenbingz/k8s-resource-reservation/controllers"
	reservationconfig "github.com/wenbingz/k8s-resource-reservation/pkg/config"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
	"os"
	"path/filepath"
	"strconv"
)

const (
	queueTokenRefillRate = 50
	queueTokenBucketSize = 500
)

var (
	keyFunc        = cache.DeletionHandlingMetaNamespaceKeyFunc
	ReservationCRD = schema.GroupVersionResource{
		Group:    "resource.scheduling.org",
		Version:  "v1alpha1",
		Resource: "reservations",
	}
	podResource = schema.GroupVersionResource{
		Version:  "v1",
		Resource: "pods",
	}
)

type ReservationScheduler struct {
	dynamicCli           dynamic.Interface
	informerFactory      dynamicinformer.DynamicSharedInformerFactory
	crdLister            cache.GenericLister
	podLister            cache.GenericLister
	podInformer          cache.SharedIndexInformer
	queue                workqueue.RateLimitingInterface
	Stopper              chan struct{}
	clientset            *kubernetes.Clientset
	resourceOccupiedInfo map[string]map[string]map[string]bool
}

func unstructuredToPod(obj interface{}) (*v1.Pod, error) {
	if obj == nil {
		return nil, fmt.Errorf("nil argument passed")
	}
	unstructured, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("error when converting interface{} to *Unstructured")
	}

	var pod *v1.Pod
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructured.Object, &pod)
	if err != nil {
		return nil, err
	}
	return pod, nil
}

func unstructuredToReservation(obj interface{}) (*v1alpha1.Reservation, error) {
	if obj == nil {
		return nil, fmt.Errorf("nil argument passed")
	}
	unstructured, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("error when converting interface{} to *Unstructured")
	}

	var reservation *v1alpha1.Reservation
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructured.Object, &reservation)
	if err != nil {
		return nil, err
	}
	return reservation, nil
}
func (rs *ReservationScheduler) enqueue(pod *v1.Pod) {
	rs.queue.AddRateLimited(pod)
}
func (rs *ReservationScheduler) onAdd(obj interface{}) {
	pod, err := unstructuredToPod(obj)
	if err != nil {
		fmt.Println(err)
		return
	}
	if !(pod.Spec.NodeName == "" && pod.Spec.SchedulerName == schedulerconfig.SchedulerName) {
		return
	}
	rs.enqueue(pod)
}

func (rs *ReservationScheduler) onDelete(obj interface{}) {

}

func (rs *ReservationScheduler) onUpdate(old, new interface{}) {

}

func NewReservationScheduler() *ReservationScheduler {
	userHomePath, err := os.UserHomeDir()
	if err != nil {
		fmt.Println("error when get user home dir")
		os.Exit(-1)
	}
	kubeConfigPath := filepath.Join(userHomePath, ".kube", "config")
	kubeConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		fmt.Println("error when constructing kube config")
		os.Exit(-1)
	}
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		fmt.Println("error when constructing client set. ", err)
		return nil
	}
	dynamicCli, err := dynamic.NewForConfig(kubeConfig)
	if err != nil {
		fmt.Println("error when constructing dynamic client")
		fmt.Println(dynamicCli, err)
		return nil
	}
	queue := workqueue.NewNamedRateLimitingQueue(&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(queueTokenRefillRate), queueTokenBucketSize)},
		"reservation-scheduler")
	stopper := make(chan struct{})
	scheduler := &ReservationScheduler{
		dynamicCli: dynamicCli,
		clientset:  clientset,
		queue:      queue,
		Stopper:    stopper,
	}

	scheduler.informerFactory = dynamicinformer.NewDynamicSharedInformerFactory(scheduler.dynamicCli, 0)
	scheduler.podInformer = scheduler.informerFactory.ForResource(podResource).Informer()
	scheduler.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    scheduler.onAdd,
		DeleteFunc: scheduler.onDelete,
		UpdateFunc: scheduler.onUpdate,
	})
	scheduler.podLister = scheduler.informerFactory.ForResource(podResource).Lister()
	scheduler.crdLister = scheduler.informerFactory.ForResource(ReservationCRD).Lister()
	scheduler.resourceOccupiedInfo = make(map[string]map[string]map[string]bool)
	return scheduler
}

func (rs *ReservationScheduler) processNextItem() bool {
	key, quit := rs.queue.Get()
	if quit {
		return false
	}
	defer rs.queue.Done(key)
	ok, err := rs.schedulePod(key.(string))
	if err == nil {
		rs.queue.Forget(key)
		return true
	}
	if !ok {
		rs.queue.AddRateLimited(key)
		return true
	}
	return true
}

func (rs *ReservationScheduler) getPlaceholderPodOccupationStatus(reservation *v1alpha1.Reservation, resourceId string, replicaId string) bool {
	reservationKey := GetReservationKey(reservation)
	if _, ok := rs.resourceOccupiedInfo[reservationKey]; !ok {
		rs.resourceOccupiedInfo[reservationKey] = make(map[string]map[string]bool)
	}
	if _, ok := rs.resourceOccupiedInfo[reservationKey][resourceId]; !ok {
		rs.resourceOccupiedInfo[reservationKey][resourceId] = make(map[string]bool)
	}
	if _, ok := rs.resourceOccupiedInfo[reservationKey][resourceId][replicaId]; !ok {
		return false
	}
	return rs.resourceOccupiedInfo[reservationKey][resourceId][replicaId]
}

func (rs *ReservationScheduler) setPlaceholderPodOccupationStatus(reservation *v1alpha1.Reservation, resourceId string, replicaId string, occupationStatus bool) {
	// to avoid null point exception
	rs.getPlaceholderPodOccupationStatus(reservation, resourceId, replicaId)
	rs.resourceOccupiedInfo[GetReservationKey(reservation)][resourceId][replicaId] = occupationStatus
}
func (rs *ReservationScheduler) initializePodOccupiedInfo() error {
	reservations, err := rs.crdLister.List(labels.Everything())
	if err != nil {
		return err
	}
	for _, reservationUnstructured := range reservations {
		reservation, err := unstructuredToReservation(reservationUnstructured)
		if err != nil {
			fmt.Println("error when try to parse unstructured reservation. ", err)
		}
		// only check completed reservations
		if reservation.Spec.Status.ReservationStatus == v1alpha1.ReservationStatusCompleted {
			reservationKey := reservation.Namespace + "/" + reservation.Name
			if _, ok := rs.resourceOccupiedInfo[reservationKey]; !ok {
				rs.resourceOccupiedInfo[reservationKey] = make(map[string]map[string]bool)
			}
			for _, podStatus := range reservation.Spec.Placeholders {
				resourceId, replicaId, isOccupied := podStatus.ResourceId, podStatus.ReplicaId, podStatus.IsOccupied
				if _, ok := rs.resourceOccupiedInfo[reservationKey][strconv.Itoa(resourceId)]; !ok {
					rs.resourceOccupiedInfo[reservationKey][strconv.Itoa(resourceId)] = make(map[string]bool)
				}
				occupiedStatus := false
				if isOccupied == "occupied" {
					occupiedStatus = true
				}
				rs.resourceOccupiedInfo[reservationKey][strconv.Itoa(resourceId)][strconv.Itoa(replicaId)] = occupiedStatus
			}
		}
	}
	return nil
}

func (rs *ReservationScheduler) findPlaceholderForPod(reservation *v1alpha1.Reservation, resourceId int, replicaId int) (*v1.Pod, error) {
	var placeholderPod *v1.Pod
	if resourceId >= 0 && replicaId >= 0 {
		if rs.getPlaceholderPodOccupationStatus(reservation, string(resourceId), string(replicaId)) {
			return nil, fmt.Errorf("the reservation app %s, resource id %d, replica id %d has already been occupied", GetReservationKey(reservation), resourceId, replicaId)
		}
		unstructuredPods, err := rs.podLister.ByNamespace(reservation.Namespace).List(labels.SelectorFromSet(map[string]string{
			reservationconfig.ReservationAppLabel:   reservation.Name,
			reservationconfig.ReservationResourceId: string(resourceId),
			reservationconfig.ReservationReplicaId:  string(replicaId),
		}))
		if err != nil {
			return nil, err
		}
		if unstructuredPods == nil || len(unstructuredPods) == 0 {
			return nil, fmt.Errorf("fail to get targeted placeholder pod for reservation %s, resourceId: %d, replicaId: %d", reservation.Name, resourceId, replicaId)
		}
		placeholderPod, err = unstructuredToPod(unstructuredPods[0])
		if err != nil {
			return nil, err
		}
		return placeholderPod, nil
	}
	for _, request := range reservation.Spec.ResourceRequests {
		for i := 0; i < request.Replica; i = i + 1 {
			// get first unoccupied placeholder
			if !rs.getPlaceholderPodOccupationStatus(reservation, string(request.ResourceId), string(i)) {
				return rs.findPlaceholderForPod(reservation, request.ResourceId, i)
			}
		}
	}
	return nil, fmt.Errorf("can not find a valid placeholder pod with reservation %s ", GetReservationKey(reservation))
}

func (rs *ReservationScheduler) preemptPlaceholderPod(placeholderPod *v1.Pod, podToSchedule *v1.Pod) error {
	if placeholderPod.Status.Phase != v1.PodRunning {
		return fmt.Errorf("pod %s is not valid to preempt, since its status is %s ", placeholderPod.Namespace+"/"+placeholderPod.Name, placeholderPod.Status.Phase)
	}
	nodeName := placeholderPod.Spec.NodeName
	fmt.Sprintf("trying to assign pod %s to node %s ", podToSchedule.Namespace+"/"+podToSchedule.Namespace, nodeName)
	rs.clientset.CoreV1().Pods(placeholderPod.Namespace).Bind(context.TODO(), &v1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podToSchedule.Name,
			Namespace: podToSchedule.Namespace,
		},
		Target: v1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       nodeName,
		},
	}, metav1.CreateOptions{})
	return nil
}

func (rs *ReservationScheduler) schedulePod(key string) (bool, error) {
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return false, err
	}
	unstructuredPod, err := rs.podLister.ByNamespace(ns).Get(name)
	if err != nil {
		return false, err
	}
	pod, err := unstructuredToPod(unstructuredPod)
	reservationAppName, ok := pod.Labels[reservationconfig.ReservationAppLabel]
	if !ok {
		return false, fmt.Errorf("error when fetching reservation app info from pod %s", key)
	}
	unstructuredReservation, err := rs.crdLister.ByNamespace(ns).Get(reservationAppName)
	reserve, err := unstructuredToReservation(unstructuredReservation)
	if err != nil {
		return false, err
	}
	reservation := reserve.DeepCopy()
	if reservation.Spec.Status.ReservationStatus != v1alpha1.ReservationStatusCompleted {
		return false, fmt.Errorf("pod %s can not be scheduled, since reservation app %s is in status of %s",
			key, reservationAppName, reservation.Spec.Status.ReservationStatus)
	}
	resourceIdStr, ok1 := pod.Labels[reservationconfig.ReservationResourceId]
	replicaIdStr, ok2 := pod.Labels[reservationconfig.ReservationReplicaId]
	resourceId, _ := strconv.Atoi(resourceIdStr)
	replicaId, _ := strconv.Atoi(replicaIdStr)
	var placeholderPod *v1.Pod
	if ok1 && ok2 {
		placeholderPod, err = rs.findPlaceholderForPod(reservation, resourceId, replicaId)
	} else {
		placeholderPod, err = rs.findPlaceholderForPod(reservation, -1, -1)
	}
	if err != nil {
		return false, err
	}
	err = rs.preemptPlaceholderPod(placeholderPod, pod)
	if err != nil {
		return false, err
	}
	_, resourceIdOfPlaceholder, replicaIdOfPlaceholder, ok := controller.SplitReservationPodInfo(placeholderPod)
	rs.setPlaceholderPodOccupationStatus(reservation, string(resourceIdOfPlaceholder), string(replicaIdOfPlaceholder), true)
	placeholderPodStatus := reservation.Spec.Placeholders[placeholderPod.Name]
	placeholderPodStatus.IsOccupied = "occupied"
	reservation.Spec.Placeholders[placeholderPod.Name] = placeholderPodStatus
	unstructuredObj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(reservation)
	_, err = rs.dynamicCli.Resource(ReservationCRD).Namespace(reserve.Namespace).Update(context.TODO(), &unstructured.Unstructured{Object: unstructuredObj}, metav1.UpdateOptions{})
	if err != nil {
		fmt.Println(err)
	}
	return true, nil
}

func (rs *ReservationScheduler) RunController() {
	rs.informerFactory.Start(rs.Stopper)
	rs.informerFactory.WaitForCacheSync(rs.Stopper)
	if !rs.podInformer.HasSynced() {
		fmt.Println("error when try to wait for cache sync")
		return
	}
	err := rs.initializePodOccupiedInfo()
	if err != nil {
		fmt.Println("error when initializing placeholder pod occupation status", err)
		return
	}
	defer utilruntime.HandleCrash()
	for rs.processNextItem() {
	}
}
