package bullfinch

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/cache/memory"
)

type Bullfinch struct {
	handle framework.Handle
	cache  *memory.InMemoryCache
}

var _ framework.PreScorePlugin = &Bullfinch{}

const (
	// Name is the name of the plugin used in Registry and configurations.
	Name                 = "Bullfinch"
	SumAllocatableCpu    = "SumAllocatableCpu"
	SumAllocatableMem    = "SumAllocatableMem"
	SumAllocatableGpuMem = "SumAllocatableGpuMem"
	ScaledResourceName   = "aliyun.com/gpu-mem"
	WorkloadNodeList     = "WorkloadNodeList"
	IdelnessHitNodes     = "xxx.com/idelness-hit-nodes"
	NodeName             = "xxx.com/nodename"
)

func (b *Bullfinch) Name() string {
	return Name
}

func (b *Bullfinch) getWorkloadName(pod *v1.Pod) (string, error) {
	podName := "nil"
	if pod != nil {
		podName = pod.Name
		for _, or := range pod.OwnerReferences {
			if *or.Controller {
				return or.Name, nil
			}
		}
	}
	return "", fmt.Errorf("pod[%v] workerload not found", podName)
}

func (b *Bullfinch) PreScore(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodes []*v1.Node) (status *framework.Status) {
	defer func() {
		if err := recover(); err != nil {
			klog.Errorf("score with error:%v", err)
			status = framework.NewStatus(framework.Success, "")
		}
	}()

	if b.cache == nil {
		b.cache = memory.GetInMemoryCache(10 * time.Minute)
	}

	// 获取pod的workloadNodelist
	workloadNodeTable := map[string]int64{}
	for _, node := range nodes {
		nodeinfo, err := b.handle.SnapshotSharedLister().NodeInfos().Get(node.Name)
		if err != nil {
			klog.Error("from cache get nodeinfo faild")
		} else {
			for _, podinfo := range nodeinfo.Pods {
				if podwlname, err := b.getWorkloadName(podinfo.Pod); err == nil {
					if wlname, err := b.getWorkloadName(pod); err == nil {
						if podwlname == wlname {
							value, found := workloadNodeTable[node.Name]
							if !found {
								value = 0
							}
							workloadNodeTable[node.Name] = value + 1
						}
					}
				}
			}
		}
	}
	if jsonValue, err := json.Marshal(workloadNodeTable); err != nil {
		klog.Errorf("json err:%v", err)
	} else {
		pod.Annotations[WorkloadNodeList] = string(jsonValue)
	}
	// 根据 workloadnodelist 开始获得优先级node
	pQueue := &PriorityQueue{}
	for _, node := range nodes {
		value, ok := workloadNodeTable[node.Name]
		if !ok {
			value = 0
		}
		heap.Push(pQueue, &Item{
			Value: map[string]string{
				"nodeName": node.Name,
			},
			Priority: float64(value),
		})
	}

	var firstValue float64
	hitNodes := map[string]int64{}
	if pQueue.Len() > 0 {
		item := heap.Pop(pQueue).(*Item)
		data := item.Value.(map[string]string)
		firstValue = item.Priority
		hitNodes[data["nodeName"]] = int64(item.Priority)

		for pQueue.Len() > 0 {
			item = heap.Pop(pQueue).(*Item)
			data = item.Value.(map[string]string)
			if item.Priority > firstValue {
				break
			}
			hitNodes[data["nodeName"]] = int64(item.Priority)
		}
	}

	if len(hitNodes) > 0 {
		if jsonValue, err := json.Marshal(hitNodes); err != nil {
			klog.Errorf("json err:%v", err)
		} else {
			pod.Annotations[IdelnessHitNodes] = string(jsonValue)
		}
	}

	plugins := b.cache.GetPlugins()
	for _, node := range nodes {
		if node.Labels != nil {
			if plugins != nil && len(plugins) > 0 {
				for _, plugin := range plugins {
					if plugin.Switch == true {
						for key, value := range node.Labels {
							v, ok := plugin.NodeSelector[key]
							if ok && value == v {
								StateKey := GetStateKeyByNodeAndPlugin(plugin.Name, node.Name)
								state.Write(StateKey, NewBullfinchStateData())
								break
							}
						}
					}
				}
			}
		}
	}

	nodeResourceTable := make(map[string]int64)
	for _, node := range nodes {
		if value, ok := nodeResourceTable["cpu"]; ok {
			nodeResourceTable["cpu"] = value + node.Status.Allocatable.Cpu().MilliValue()
		} else {
			nodeResourceTable["cpu"] = node.Status.Allocatable.Cpu().MilliValue()
		}

	}

	var sumAllocatableCpu int64
	var sumAllocatableMem int64
	var sumAllocatableGpuMem int64
	for _, node := range nodes {
		sumAllocatableCpu += node.Status.Allocatable.Cpu().MilliValue()
		sumAllocatableMem += node.Status.Allocatable.Memory().Value()
		if quantity, found := node.Status.Allocatable[ScaledResourceName]; found {
			sumAllocatableGpuMem += quantity.Value()
		}
	}

	pod.Annotations[SumAllocatableCpu] = strconv.FormatInt(sumAllocatableCpu, 10)
	pod.Annotations[SumAllocatableMem] = strconv.FormatInt(sumAllocatableMem, 10)
	pod.Annotations[SumAllocatableGpuMem] = strconv.FormatInt(sumAllocatableGpuMem, 10)

	//klog.Infof("bullfinch Annotations: %s %s %s",pod.Annotations[SumAllocatableCpu], pod.Annotations[SumAllocatableMem],pod.Annotations[SumAllocatableGpuMem] )

	return framework.NewStatus(framework.Success, "")
}

func GetStateKeyByNodeAndPlugin(plugin string, nodeName string) framework.StateKey {
	return framework.StateKey(fmt.Sprintf("%v-%v", plugin, nodeName))
}

type BullfinchStateData struct {
}

func NewBullfinchStateData() framework.StateData {
	return &BullfinchStateData{}
}

func (d *BullfinchStateData) Clone() framework.StateData {
	return d
}

// New 初始化一个新插件并返回
func New(_ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &Bullfinch{handle: h}, nil
}
