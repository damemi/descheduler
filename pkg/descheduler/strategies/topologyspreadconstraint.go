/*
Copyright 2020 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
)

// @seanmalloy notes:
//
// https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#topologyspreadconstraint-v1-core
// https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/
// https://github.com/kubernetes-sigs/descheduler/blob/master/pkg/descheduler/strategies/pod_antiaffinity.go

// AntiAffinityTerm's topology key value used in predicate metadata
type topologyPair struct {
	key   string
	value string
}

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
	//
	// Create a map of Node Name to v1.Node
	// 1. for each namespace for which there is Topology Constraint
	// 2. for each TopologySpreadyConstraint in that namespace
	//  { find all evictable pods in that namespace
	//  { 3. for each evictable pod in that namespace
	// 4. If the pod matches this TopologySpreadConstraint LabelSelector
	// 5. If the pod nodeName is present in the nodeMap
	// 6. create a topoPair with key as this TopologySpreadConstraint.TopologyKey and value as this pod's Node Label Value for this TopologyKey
	// 7. add the pod with key as this topoPair
	// 8. find the min number of pods in any topoPair for this topologyKey
	// iterate through all topoPairs for this topologyKey and diff currentPods -minPods <=maxSkew
	// if diff > maxSkew, add this pod in the current bucket for eviction

	evictable := podEvictor.Evictable()

	namespacedTopologySpreadConstrainPods := make(map[string][]v1.Pod)
	// First record all of the constraints by namespace
	namespaces, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.ErrorS(err, "couldn't list namespaces")
		return
	}
	podsForEviction := make(map[*v1.Pod]struct{})
	// 1. for each namespace...
	for _, namespace := range namespaces.Items {
		namespacePods, err := client.CoreV1().Pods(namespace.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			klog.ErrorS(err, "couldn't list pods in namespace", "namespace", namespace)
			return
		}

		// ...where there is a topology constraint
		for _, pod := range namespacePods.Items {
			if pod.Spec.TopologySpreadConstraints != nil {
				namespacedTopologySpreadConstrainPods[pod.Namespace] = append(namespacedTopologySpreadConstrainPods[pod.Namespace], pod)
			}
		}
		if len(namespacedTopologySpreadConstrainPods[namespace.Name]) == 0 {
			continue
		}

		// 2. for each topologySpreadConstraint in that namespace
		for _, constrainedPod := range namespacedTopologySpreadConstrainPods[namespace.Name] {
			// find pods to evict one constraint at a time
			for _, constraint := range constrainedPod.Spec.TopologySpreadConstraints {
				constraintTopologies := make(map[topologyPair][]v1.Pod)

				// 3. for each evictable pod in that namespace
				// (this loop is where we count the number of pods per topologyValue that match this constraint's selector)
				for _, pod := range namespacePods.Items {
					if !evictable.IsEvictable(&pod) {
						continue
					}

					// 4. if the pod matches this TopologySpreadConstraint LabelSelector
					s, err := metav1.LabelSelectorAsSelector(constraint.LabelSelector)
					if err != nil {
						klog.ErrorS(err, "couldn't parse label selector as selector", "selector", constraint.LabelSelector)
					}
					if !s.Matches(labels.Set(pod.Labels)) {
						continue
					}

					// 5. If the pod's node matches this constraint's topologyKey, create a topoPair and add the pod
					node, err := client.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
					if err != nil {
						klog.ErrorS(err, "couldn't get node", "node", pod.Spec.NodeName)
					}
					nodeValue, ok := node.Labels[constraint.TopologyKey]
					if !ok {
						continue
					}
					// 6. create a topoPair with key as this TopologySpreadConstraint
					topoPair := topologyPair{key: constraint.TopologyKey, value: nodeValue}
					// 7. add the pod with key as this topoPair
					constraintTopologies[topoPair] = append(constraintTopologies[topoPair], pod)
				}

				// 8. find the min number of pods in any topoPair for this topologyKey
				// (in this loop, we'll find topologies with too many pods and mark pods in those topologies for eviction)
				// compare each topologyPair to all the others and find any that have a skew > maxSkew.
				// then add N pods from that pair's list for eviction (where N=skew-maxSkew)
				// (TODO this can probably be optimized, we might not need to compare N^2 here to cover every case)
				for topology, pods := range constraintTopologies {
					// because we might evict pods due to violating skew from one topology,
					// we may or may not need to continue evicting pods for future topologies,
					// based on the updated count we get here.
					remainingPods := len(pods)
					for topology2, pods2 := range constraintTopologies {
						// no need to compare to itself
						if topology == topology2 {
							continue
						}

						skew := remainingPods - len(pods2)
						if int32(skew) <= constraint.MaxSkew {
							continue
						}

						// the parent topology (in this loop) covers the maxSkew of this topology
						// mark N pods from the parent topology for eviction
						// then update the remaining pods list
						// (mark pods backwards so we don't need to worry about updating this list)
						// (TODO but we might have to update the list anyway for future comparisons to be accurate)
						//
						// start at (the last pod - #evictedSoFar)
						for i := (len(pods) - 1) - (len(pods) - remainingPods); i >= 0; i-- {
							podsForEviction[&pods[i]] = struct{}{}
							remainingPods--
						}
					}
				}
			}
		}
	}

	for pod := range podsForEviction {
		node, err := client.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
		if err != nil {
			klog.ErrorS(err, "couldn't get node", "node", pod.Spec.NodeName)
		}
		if _, err := podEvictor.EvictPod(ctx, pod, node, "PodTopologySpread"); err != nil {
			klog.ErrorS(err, "Error evicting pod", "pod", klog.KObj(pod))
			break
		}
	}
}