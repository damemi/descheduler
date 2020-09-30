/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package strategies

import (
	"context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"math"

	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
)

// @seanmalloy notes:
//
// https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#topologyspreadconstraint-v1-core
// https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/
// https://github.com/kubernetes-sigs/descheduler/blob/master/pkg/descheduler/strategies/pod_antiaffinity.go

// AntiAffinityTerm's topology key value used in predicate metadata
type topologyConstraint struct {
	key           string
	value         string
	labelSelector string
}

type podSet map[*v1.Pod]struct{}

// for each topology pair, what is the set of pods
type topologyPairToPodSetMap map[topologyConstraint]podSet

// for each topologyKey, what is the map of topologyKey pairs to pods
type topologyKeyToTopologyPairSetMap map[string]topologyPairToPodSetMap

// TODO: remove this type?
type NamespacedTopologySpreadConstraint struct {
	Namespace                 string
	TopologySpreadConstraints []v1.TopologySpreadConstraint
}

func RemovePodsViolatingTopologySpreadConstraint(
	ctx context.Context,
	client clientset.Interface,
	strategy api.DeschedulerStrategy,
	nodes []*v1.Node,
	podEvictor *evictions.PodEvictor,
) {
	// START HERE

	// TODO: move code from this function here
	//
	//evictPodsViolatingSpreadConstraints(ds.Client, policyGroupVersion, nodes, ds.DryRun, nodePodCount, strategy.Params.NamespacedTopologySpreadConstraints)

	// contents of evictPodsViolatingSpreadConstraints
	//
	// Create a map of Node Name to v1.Node
	// for each namespace for which there is Topology Constraint
	// for each TopologySpreadyConstraint in that namespace
	// find all evictable pods in that namespace
	// for each evictable pod in that namespace
	// If the pod matches this TopologySpreadConstraint LabelSelector
	// If the pod nodeName is present in the nodeMap
	// create a topoPair with key as this TopologySpreadConstraint.TopologyKey and value as this pod's Node Label Value for this TopologyKey
	// add the pod with key as this topoPair
	// find the min number of pods in any topoPair for this topologyKey
	// iterate through all topoPairs for this topologyKey and diff currentPods -minPods <=maxSkew
	// if diff > maxSkew, add this pod in the current bucket for eviction

	// We get N podLists , one for each TopologyKey in a given Namespace
	// Find the pods which are common to each of these podLists
	// Evict these Pods

	// @seanmalloy
	//
	// find all canidate pods and namespaces for eviction

	namespacedTopologySpreadConstrainPods := make(map[string][]*v1.Pod)
	namespacedTpPairsToMatchingCount := make(map[string]map[topologyConstraint]int32)
	for _, node := range nodes {
		pods, err := podutil.ListPodsOnANode(
			ctx,
			client,
			node,
			podutil.WithFilter(podEvictor.Evictable().IsEvictable))
		if err != nil {
			return
		}

		// First record all of the constraints by namespace
		for _, pod := range pods {
			if pod.Spec.TopologySpreadConstraints != nil {
				namespacedTopologySpreadConstrainPods[pod.Namespace] = append(namespacedTopologySpreadConstrainPods[pod.Namespace], pod)
			}
			for _, c := range pod.Spec.TopologySpreadConstraints {
				if nodeValue, ok := node.Labels[c.TopologyKey]; ok {
					tp := topologyConstraint{key: c.TopologyKey, value: nodeValue, labelSelector: c.LabelSelector.String()}
					namespacedTpPairsToMatchingCount[pod.Namespace][tp] = 0
				}
			}
		}

		// Go through each constraint and pod in a namespace and find any that match:
		//  1. the topology key/value for that pod's node
		//  2. the labelSelector for the topology constraint
		for namespace, tps := range namespacedTpPairsToMatchingCount {
			namespacePods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				klog.ErrorS(err, "couldn't list pods in namespace", "namespace", namespace)
				return
			}
			for tp, _ := range tps {
				count := 0
				for _, pod := range namespacePods.Items {
					if pod.Spec.NodeName != node.Name {
						continue
					}
					selector, err := metav1.ParseToLabelSelector(tp.labelSelector)
					if err != nil {
						klog.ErrorS(err, "couldn't parse label selector", "selector", tp.labelSelector)
					}
					s, err := metav1.LabelSelectorAsSelector(selector)
					if err != nil {
						klog.ErrorS(err, "couldn't parse label selector as selector", "selector", tp.labelSelector)
					}
					if !s.Matches(labels.Set(pod.Labels)) {
						continue
					}
					count++
				}
				namespacedTpPairsToMatchingCount[namespace][tp] = int32(count)
			}
		}
	}

	for namespace, pods := range namespacedTopologySpreadConstrainPods {
		allNamespacePods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			klog.ErrorS(err, "couldn't list pods in namespace", "namespace", namespace)
			return
		}

		// 3D map: map[topologyKey][labelSelector][topologyValue]:count
		// This lets us count the size of different topologies,
		// measured in the number of pods matching a selector in that topology
		topologyBuckets := make(map[string]map[string]map[string]int)

		// For every constrained pod in the namespace, compare to all other pods in the namespace
		// including itself, because we need to include the constrained pods in the total to calculate skew
		for _, pod := range allNamespacePods.Items {
			for _, constrainedPod := range pods {
				for _, constraint := range constrainedPod.Spec.TopologySpreadConstraints {
					// Initialize this key's bucket, if necessary
					if topologyBuckets[constraint.TopologyKey] == nil {
						topologyBuckets[constraint.TopologyKey] = make(map[string]map[string]int)
					}

					// If this pod is the constrained pod, add +1 to this constraint's bucket and continue
					if equality.Semantic.DeepEqual(pod, constrainedPod) {
						// bucket +=1
						continue
					}

					// Check if this pod matches the constraint's labelSelector
					s, err := metav1.LabelSelectorAsSelector(constraint.LabelSelector)
					if err != nil {
						klog.ErrorS(err, "couldn't parse label selector as selector", "selector", constraint.LabelSelector)
					}
					if !s.Matches(labels.Set(pod.Labels)) {
						continue
					}

					// Check if this pod's node has this topology key
					node, err := client.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
					if err != nil {
						klog.ErrorS(err, "couldn't get node", "node", pod.Spec.NodeName)
					}
					if topologyValue, ok := node.Labels[constraint.TopologyKey]; !ok {
						continue
					} else {
						// Increase the count for this bucket by 1
						// This is the count for pods in a topology(Value), sorted by labelSelector, sorted by topologyKey
						//
						// bucket +1
					}
				}
			}
		}
	}


	// @seanmalloy need to calculate which pods should be evicted to "balance" based on topology domains
	//
	// need to implement the stubbed in function getPodsViolatingPodsTopologySpreadConstraint

	// @seanmalloy need to handle multiple TopologySpreadConstraints on single pod

	namespaceToTopologyKeySet := make(map[string]topologyKeyToTopologyPairSetMap)

	// create a node map matching nodeName to v1.Node
	nodeMap := make(map[string]*v1.Node)
	for _, node := range nodes {
		nodeMap[node.Name] = node
	}

	// TODO: the below line just makes an empty slice, and the struct type NamespacedTopologySpreadConstraint will be removed
	//
	// namespacedTopologySpreadConstraints variable was previously passed in as a strategy
	// parameter, but this is no longer a strategy parameter.
	namespacedTopologySpreadConstraints := []NamespacedTopologySpreadConstraint{}
	for _, namespacedConstraint := range namespacedTopologySpreadConstraints {
		if namespaceToTopologyKeySet[namespacedConstraint.Namespace] == nil {
			namespaceToTopologyKeySet[namespacedConstraint.Namespace] = make(topologyKeyToTopologyPairSetMap)
		}
		for _, topoConstraint := range namespacedConstraint.TopologySpreadConstraints {
			if namespaceToTopologyKeySet[namespacedConstraint.Namespace][topoConstraint.TopologyKey] == nil {
				namespaceToTopologyKeySet[namespacedConstraint.Namespace][topoConstraint.TopologyKey] = make(topologyPairToPodSetMap)
			}
			for _, node := range nodes {
				if node.Labels[topoConstraint.TopologyKey] == "" {
					continue
				}
				pair := topologyConstraint{key: topoConstraint.TopologyKey, value: node.Labels[topoConstraint.TopologyKey]}
				if namespaceToTopologyKeySet[namespacedConstraint.Namespace][topoConstraint.TopologyKey][pair] == nil {
					// this ensures that nodes which match topokey but no pods are accounted for
					namespaceToTopologyKeySet[namespacedConstraint.Namespace][topoConstraint.TopologyKey][pair] = make(podSet)
				}
			}

			// TODO: pods is hard coded to a slice of empty pods
			//
			//pods, err := podutil.ListEvictablePodsByNamespace(client, false, namespacedConstraint.Namespace)
			pods := []*v1.Pod{}
			//if err != nil || len(pods) == 0 {
			if len(pods) == 0 {
				klog.V(1).Infof("No Evictable pods found for Namespace %v", namespacedConstraint.Namespace)
				continue
			}

			for _, pod := range pods {
				klog.V(2).Infof("Processing pod %v", pod.Name)
				// does this pod labels match the constraint label selector
				selector, err := metav1.LabelSelectorAsSelector(topoConstraint.LabelSelector)
				if err != nil {
					klog.V(2).Infof("Pod Labels dont match for %v", pod.Name)
					continue
				}
				if !selector.Matches(labels.Set(pod.Labels)) {
					klog.V(2).Infof("Pod Labels dont match for %v", pod.Name)
					continue
				}
				klog.V(1).Infof("Pod %v matched labels", pod.Name)
				// TODO: Need to determine if the topokey already present in the node or not
				if pod.Spec.NodeName == "" {
					continue
				}
				// see of this pods NodeName exists in the candidates nodes, else ignore
				_, ok := nodeMap[pod.Spec.NodeName]
				if !ok {
					klog.V(2).Infof("Found a node %v in pod %v, which is not present in our map, ignoring it...", pod.Spec.NodeName, pod.Name)
					continue
				}
				pair := topologyConstraint{key: topoConstraint.TopologyKey, value: nodeMap[pod.Spec.NodeName].Labels[topoConstraint.TopologyKey]}
				if namespaceToTopologyKeySet[namespacedConstraint.Namespace][topoConstraint.TopologyKey][pair] == nil {
					// this ensures that nodes which match topokey but no pods are accounted for
					namespaceToTopologyKeySet[namespacedConstraint.Namespace][topoConstraint.TopologyKey][pair] = make(podSet)
				}
				namespaceToTopologyKeySet[namespacedConstraint.Namespace][topoConstraint.TopologyKey][pair][pod] = struct{}{}
				klog.V(2).Infof("Topo Pair %v, Count %v", pair, len(namespaceToTopologyKeySet[namespacedConstraint.Namespace][topoConstraint.TopologyKey][pair]))

			}
		}
	}

	// finalPodsToEvict := []*v1.Pod{}
	for _, namespacedConstraint := range namespacedTopologySpreadConstraints {
		allPodsToEvictPerTopoKey := make(map[string][]*v1.Pod)
		for _, topoConstraint := range namespacedConstraint.TopologySpreadConstraints {
			minPodsForGivenTopo := math.MaxInt32
			for _, v := range namespaceToTopologyKeySet[namespacedConstraint.Namespace][topoConstraint.TopologyKey] {
				if len(v) < minPodsForGivenTopo {
					minPodsForGivenTopo = len(v)
				}
			}

			topologyPairToPods := namespaceToTopologyKeySet[namespacedConstraint.Namespace][topoConstraint.TopologyKey]
			for pair, v := range topologyPairToPods {
				podsInTopo := len(v)
				klog.V(1).Infof("Min Pods in Any Pair %v, pair %v, PodCount %v", minPodsForGivenTopo, pair, podsInTopo)

				if int32(podsInTopo-minPodsForGivenTopo) > topoConstraint.MaxSkew {
					countToEvict := int32(podsInTopo-minPodsForGivenTopo) - topoConstraint.MaxSkew
					klog.V(1).Infof("pair %v, Count to evict %v", pair, countToEvict)
					podsListToEvict := getPodsToEvict(countToEvict, v)
					allPodsToEvictPerTopoKey[topoConstraint.TopologyKey] = append(allPodsToEvictPerTopoKey[topoConstraint.TopologyKey], podsListToEvict...)

				}
			}

		}

		// TODO: Sometimes we will have hierarchical TopoKeys, like a Building has Rooms and Rooms have Racks
		// Our Current Definition of TopologySpreadConstraint Doesnt allow you to capture that Constraint
		// If we could capture that Hierarchy, I would do the following:-
		// - Create a List of Pods to Evict per TopologyKey
		// - Take intersection of all lists to produce a list of pods to evict
		// This is because in hierarchical topologyKeys, if we make an indepdent decision of evicting only by
		// Rack, but didnt consider the Room spreading at all,we might mess up the Room Spreading. This is too
		// constrained though since, if we consider an intersection of all hierarchies, we would not even balance
		// properly. So we would need to define some sorta importance of which topologyKey has what weight, etc
		// finalPodsToEvict = intersectAllPodsList(allPodsToEvictPerTopoKey)

		// defer the decision as late as possible to cause less schedulings
		for topoKey, podList := range allPodsToEvictPerTopoKey {
			klog.V(1).Infof("Total pods to evict in TopoKey %v is %v", topoKey, len(podList))
			//evictPodsSimple(client, podList, policyGroupVersion, dryRun)
			for _, pod := range podList {
				// TODO: node variable not defined
				//
				//success, err := podEvictor.EvictPod(ctx, pod, node)
				//if success {
				//klog.V(1).Infof("Evicted pod: %#v because it violate pod topology constraint", pod.Name)
				//}

				//if err != nil {
				//	klog.Errorf("Error evicting pod: (%#v)", err)
				//	break
				//}
				klog.V(1).Infof("Evicted pod: %#v because it violate pod topology constraint", pod.Name)
			}
		}
	}

}

// @seanmalloy
//
// TODO: this should find pods that are not balanced and return them. Try
// to reuse logic from previous code to write this function.
func getPodsViolatingPodsTopologySpreadConstraint(pods []*v1.Pod) []*v1.Pod {
	return pods
}

// TODO: this function is not called
func intersectAllPodsList(allPodsToEvictPerTopoKey map[string][]*v1.Pod) []*v1.Pod {
	// increment each pod's count by 1
	// if the pod count reaches the number of topoKeys, it should be evicted
	perPodCount := make(map[string]int)

	finalList := []*v1.Pod{}
	totalTopoKeys := len(allPodsToEvictPerTopoKey)
	klog.V(1).Infof("Total topokeys found %v", totalTopoKeys)
	for _, podList := range allPodsToEvictPerTopoKey {
		for _, pod := range podList {
			key := pod.Name + "-" + pod.Namespace
			perPodCount[key] = perPodCount[key] + 1
			if perPodCount[key] == len(allPodsToEvictPerTopoKey) {
				finalList = append(finalList, pod)
			}
		}
	}
	return finalList
}

func getPodsToEvict(countToEvict int32, podMap map[*v1.Pod]struct{}) []*v1.Pod {
	count := int32(0)
	podList := []*v1.Pod{}
	for k := range podMap {
		if count == countToEvict {
			break
		}
		podList = append(podList, k)
		count++
	}

	return podList
}

// TODO: this function is not called
func addTopologyPair(topoMap map[topologyConstraint]podSet, pair topologyConstraint, pod *v1.Pod) {
	if topoMap[pair] == nil {
		topoMap[pair] = make(map[*v1.Pod]struct{})
	}
	topoMap[pair][pod] = struct{}{}
}
