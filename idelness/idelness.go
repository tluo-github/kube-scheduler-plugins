package idleness

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/bluele/gcache"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/cache/memory"
	"sigs.k8s.io/scheduler-plugins/pkg/bullfinch"
)

type Idleness struct {
	handle   framework.Handle
	cache    *memory.InMemoryCache
	appCache gcache.Cache
}

var _ framework.ScorePlugin = &Idleness{}

type IdlenessPolicy string

const (
	// Name is the name of the plugin used in Registry and configurations.
	Name                     = "Idleness"
	ScaledResourceName       = "aliyun.com/gpu-mem"
	BinpackEnabled           = "xxx.com/binpack-enabled"
	IdlenessPolicyAnnotation = "xxx.com/idleness-policy"

	ResourceTypeCpu = "cpu"
	ResourceTypeMem = "memory"
	ResourceTypeGpu = "gpu"
	True            = "true"

	Binpack  IdlenessPolicy = "binpack"    // 极端聚集
	Spread   IdlenessPolicy = "spread"     // 极端打散
	Balanced IdlenessPolicy = "binpack2.0" // binpack 2.0 均衡
)

func (r *Idleness) Name() string {
	return Name
}

func (r *Idleness) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (score int64, status *framework.Status) {
	defer func() {
		if err := recover(); err != nil {
			klog.Errorf("score with error:%v", err)
			score = 0
		}
	}()

	nodeInfo, err := r.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}
	if r.cache == nil {
		r.cache = memory.GetInMemoryCache(10 * time.Minute)
	}

	_, err = state.Read(bullfinch.GetStateKeyByNodeAndPlugin(Name, nodeName))
	if err != nil {
		if err.Error() == framework.NotFound {
			klog.V(5).Infof("node[%v] Ideless bullfinch state is false", nodeName)
			return 0, nil
		}
	}
	klog.V(5).Infof("node[%v] Idleness bullfinch state is true", nodeName)

	if !r.cache.GetSwitchByPluginName(Name) {
		klog.V(5).Infof("Idleness switch is false")
		return 0, nil
	}

	return r.score(pod, nodeInfo)
}

func (r *Idleness) score(pod *v1.Pod, nodeInfo *framework.NodeInfo) (score int64, status *framework.Status) {
	// 优选得分：(Node对应闲置资源-Pod主导资源)/Node对应Allocated资源 * 100
	// Node对应闲置资源=Node对应Allocated资源-Requested
	resoureType, leadResource := calculatePodLeadResource(pod, nodeInfo)
	score = 100
	// 让调度策略失效 指向旁路
	if value, found := pod.Annotations[bullfinch.NodeName]; found && len(value) > 0 {
		if strings.TrimSpace(value) == nodeInfo.Node().Name {
			return score, status
		}
	}

	// 跳过WorkloadNodeList
	if value, found := pod.Annotations[bullfinch.WorkloadNodeList]; found && len(value) > 0 {
		workloadNodeTable := map[string]int64{}
		if err := json.Unmarshal([]byte(value), &workloadNodeTable); err != nil {
			klog.Errorf("json err:%v", err)
		} else {
			nodeName := nodeInfo.Node().Name
			value, found := workloadNodeTable[nodeName]
			if !found {
				value = 0
			}
			count := value + 1
			if count > 0 {
				score = int64(float64(100) / float64(count))
			}
			if count > 1 {
				klog.V(5).Infof("node[%v] the node already has the same replica count[%v]", nodeInfo.Node().Name, count-1)
			}
		}
	}

	switch resoureType {
	case ResourceTypeCpu:
		allocatableCpu := nodeInfo.Allocatable.MilliCPU
		nodeRequestedCpu := nodeInfo.Requested.MilliCPU
		futureRequestedCpu := nodeRequestedCpu + leadResource
		if allocatableCpu > futureRequestedCpu {
			scoreFloat := float64(allocatableCpu-nodeRequestedCpu-leadResource) * float64(100) / float64(allocatableCpu)
			score = (score + int64(scoreFloat)) / 2
		} else {
			score = 0
		}
		break

	case ResourceTypeMem:
		allocatableMem := nodeInfo.Allocatable.Memory
		nodeRequestedMem := nodeInfo.Requested.Memory
		futureRequestMem := nodeRequestedMem + leadResource
		if allocatableMem > futureRequestMem {
			scoreFloat := float64(allocatableMem-nodeRequestedMem-leadResource) * float64(100) / float64(allocatableMem)
			score = (score + int64(scoreFloat)) / 2
		} else {
			score = 0
		}
		break

	case ResourceTypeGpu:
		allocatableGpuMem := nodeInfo.Allocatable.ScalarResources[ScaledResourceName]
		nodeRequestGpuMem := nodeInfo.Requested.ScalarResources[ScaledResourceName]
		futureRequestGpuMem := nodeRequestGpuMem + leadResource
		if allocatableGpuMem > futureRequestGpuMem {
			scoreFloat := float64(allocatableGpuMem-nodeRequestGpuMem-leadResource) * float64(100) / float64(allocatableGpuMem)
			score = (score + int64(scoreFloat)) / 2
		} else {
			score = 0
		}
		break
	}
	klog.V(5).Infof("node[%v] score[%d] resourceType: %s leadResource: %d", nodeInfo.Node().Name, score, resoureType, leadResource)
	return score, status
}

// getIdlenessPolicy 获取当前Pod 分配策略
func (r *Idleness) getIdlenessPolicy(pod *v1.Pod) IdlenessPolicy {
	// 默认为 spread 打散
	policy := Spread
	if value, found := pod.Annotations[IdlenessPolicyAnnotation]; found && len(value) > 0 {
		switch value {
		case string(Binpack):
			policy = Binpack
			break
		case string(Spread):
			policy = Spread
			break
		case string(Balanced):
			policy = Balanced
			break
		}
	}
	return policy
}

func (r *Idleness) NormalizeScore(ctx context.Context, state *framework.CycleState, p *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	// 找出最高分和最低分
	var highest int64 = -math.MaxInt64
	var lowest int64 = math.MaxInt64
	for _, nodeScore := range scores {
		if nodeScore.Score > highest {
			highest = nodeScore.Score
		}
		if nodeScore.Score < lowest {
			lowest = nodeScore.Score
		}
	}
	// Transform the highest to lowest score range to fit the framework's min to max node score range.
	oldRange := highest - lowest
	newRange := framework.MaxNodeScore - framework.MinNodeScore - 1
	for i, nodeScore := range scores {
		if oldRange == 0 {
			scores[i].Score = framework.MinNodeScore
		} else {
			scores[i].Score = ((nodeScore.Score - lowest) * newRange / oldRange) + framework.MinNodeScore
		}
	}

	policy := r.getIdlenessPolicy(p)
	hitNodes := map[string]int64{}
	if value, found := p.Annotations[bullfinch.IdelnessHitNodes]; found && len(value) > 0 {
		if err := json.Unmarshal([]byte(value), &hitNodes); err != nil {
			klog.Errorf("json err:%v", err)
		}
	}
	klog.V(5).Infof("idleness default policy: %v ,hitNodes length:%v ,total node size: %v", policy, len(hitNodes), len(scores))

	for i, node := range scores {
		switch policy {
		case Binpack:
			scores[i].Score = 100 - node.Score
		case Balanced:
			if len(hitNodes) > 0 {

				if _, ok := hitNodes[node.Name]; ok {
					scores[i].Score = 100 - node.Score
				} else {
					klog.V(5).Infof(">>>>>>>>>>>>>> old score :%v", node.Score)
					scores[i].Score = 0
				}
			}
		case Spread:
			klog.V(5).Infof("ignore..")
		}
		klog.V(5).Infof("[%v] score:%v", node.Name, scores[i].Score)
	}

	return nil
}

func (r *Idleness) ScoreExtensions() framework.ScoreExtensions {
	return r
}

// calculate Pod Lead Request Resource
func calculatePodLeadResource(pod *v1.Pod, nodeInfo *framework.NodeInfo) (string, int64) {
	var podCpuRequest int64
	var podMemRequest int64
	var podGpuMemRequest int64
	var podCpuPercent float64
	var podMemPercent float64
	var podGpuMemPercent float64
	var sumAllocatableCpu int64
	var sumAllocatableMem int64
	var sumAllocatableGpuMem int64

	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]

		podCpuRequest += container.Resources.Requests.Cpu().MilliValue()
		podMemRequest += container.Resources.Requests.Memory().Value()
		if GPUQuantity := container.Resources.Requests.Name(ScaledResourceName, resource.DecimalSI); GPUQuantity != nil {
			podGpuMemRequest = GPUQuantity.Value()
		}
	}

	// 通过pod的Annotations获取bullfinch里面存入的节点SumAllocatable
	if pod.Annotations != nil {
		value, ok := pod.Annotations[bullfinch.SumAllocatableCpu]
		if ok {
			var err error
			sumAllocatableCpu, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				sumAllocatableCpu = 0
				klog.Errorf("get pod Annotations SumAllocatableCpu err: %v", err)
			}
		}
	}

	if pod.Annotations != nil {
		value, ok := pod.Annotations[bullfinch.SumAllocatableMem]
		if ok {
			var err error
			sumAllocatableMem, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				sumAllocatableMem = 0
				klog.Errorf("get pod Annotations SumAllocatableMem err: %v", err)
			}
		}
	}

	if pod.Annotations != nil {
		value, ok := pod.Annotations[bullfinch.SumAllocatableGpuMem]
		if ok {
			var err error
			sumAllocatableGpuMem, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				sumAllocatableGpuMem = 0
				klog.Errorf("get pod Annotations SumAllocatableGpuMem err: %v", err)
			}
		}
	}
	//klog.Infof("Annotations: %d, %d, %d", sumAllocatableCpu, sumAllocatableMem, sumAllocatableGpuMem)
	//klog.Infof("Request: %d, %d, %d",podCpuRequest, podMemRequest, podGpuMemRequest)

	//podCpuPercent := float64(podCpuRequest) / float64(nodeInfo.Allocatable.MilliCPU)
	//podMemPercent := float64(podMemRequest) / float64(nodeInfo.Allocatable.Memory)
	// 分母不为0
	if sumAllocatableCpu != 0 {
		podCpuPercent = float64(podCpuRequest) / float64(sumAllocatableCpu)
	}
	if sumAllocatableMem != 0 {
		podMemPercent = float64(podMemRequest) / float64(sumAllocatableMem)
	}
	if sumAllocatableGpuMem != 0 {
		podGpuMemPercent = float64(podGpuMemRequest) / float64(sumAllocatableGpuMem)
	}

	// 选择主导资源
	if podCpuPercent-podMemPercent >= 0 {
		if podCpuPercent-podGpuMemPercent >= 0 {
			return ResourceTypeCpu, podCpuRequest
		} else {
			return ResourceTypeGpu, podGpuMemRequest
		}
	} else {
		if podMemPercent-podGpuMemPercent >= 0 {
			return ResourceTypeMem, podMemRequest
		} else {
			return ResourceTypeGpu, podGpuMemRequest
		}
	}
}

// New 初始化一个新插件并返回
func New(_ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	cache := gcache.New(3000).
		LRU().
		Expiration(1 * time.Minute).
		Build()
	return &Idleness{handle: h, appCache: cache}, nil
}
