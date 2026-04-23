package main

import (
	"crypto/tls"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

//go:embed index.html
var staticFiles embed.FS

type Config struct {
	GroupNames map[string]string `json:"groupNames"`
	NoStop     []string          `json:"noStop"`
	Hide       []string          `json:"hide"`
}

type ServiceStatus struct {
	DisplayName   string   `json:"displayName"`
	Name          string   `json:"name"`
	Namespace     string   `json:"namespace"`
	Group         string   `json:"group"`
	Kind          string   `json:"kind"`
	CanStop       bool     `json:"canStop"`
	Replicas      int      `json:"replicas"`
	Ready         bool     `json:"ready"`
	RAMBytes      int64    `json:"ramBytes"`
	CPUMilli      int64    `json:"cpuMilli"`
	Nodes         []string `json:"nodes"`
	URLs          []string `json:"urls"`
	StatusDetail  string   `json:"statusDetail"`
	PreferredNode string   `json:"preferredNode"`
}

type NodeStatus struct {
	Name          string `json:"name"`
	Ready         bool   `json:"ready"`
	Unschedulable bool   `json:"unschedulable"`
	RAMUsed       int64  `json:"ramUsed"`
	RAMTotal      int64  `json:"ramTotal"`
	CPUUsed       int64  `json:"cpuUsed"`
	CPUTotal      int64  `json:"cpuTotal"`
}

type ServicesResponse struct {
	Services []ServiceStatus `json:"services"`
	Nodes    []NodeStatus    `json:"nodes"`
	RAMUsed  int64           `json:"ramUsed"`
	RAMTotal int64           `json:"ramTotal"`
	CPUUsed  int64           `json:"cpuUsed"`
	CPUTotal int64           `json:"cpuTotal"`
}

type ScaleRequest struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Replicas  int    `json:"replicas"`
}

type GroupScaleRequest struct {
	Group    string `json:"group"`
	Replicas int    `json:"replicas"`
}

type CordonRequest struct {
	Node   string `json:"node"`
	Cordon bool   `json:"cordon"`
}

type PreferredNodeRequest struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Node      string `json:"node"`
}

const (
	PausedAnnotation        = "switchboard.io/user-paused"
	PreferredNodeAnnotation = "switchboard.io/preferred-node"
)

var (
	k8sHost   string
	k8sHTTP   *http.Client
	cfg       Config
	mu        sync.Mutex
	hostRegex = regexp.MustCompile("Host\\(([^)]+)\\)")
)

func init() {
	k8sHost = "https://" + os.Getenv("KUBERNETES_SERVICE_HOST") + ":" + os.Getenv("KUBERNETES_SERVICE_PORT")
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	k8sHTTP = &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

func loadToken() string {
	data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		log.Printf("Warning: cannot read SA token: %v", err)
		return ""
	}
	return strings.TrimSpace(string(data))
}

func loadConfig() {
	data, err := os.ReadFile("/config/services.json")
	if err != nil {
		log.Fatalf("Cannot read /config/services.json: %v", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Invalid services.json: %v", err)
	}
	log.Printf("Loaded config: %d group overrides, %d noStop rules, %d hidden", len(cfg.GroupNames), len(cfg.NoStop), len(cfg.Hide))
}

func k8sRequest(method, path string, body io.Reader) (*http.Response, error) {
	token := loadToken()
	req, err := http.NewRequest(method, k8sHost+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		if method == "PATCH" {
			req.Header.Set("Content-Type", "application/merge-patch+json")
		} else {
			req.Header.Set("Content-Type", "application/json")
		}
	}
	return k8sHTTP.Do(req)
}

func isHidden(ns string) bool {
	for _, h := range cfg.Hide {
		if h == ns {
			return true
		}
	}
	return false
}

func groupName(ns string) string {
	if name, ok := cfg.GroupNames[ns]; ok {
		return name
	}
	parts := strings.Split(ns, "-")
	for i, p := range parts {
		if len(p) > 0 {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, " ")
}

func isNoStop(ns, name string) bool {
	for _, rule := range cfg.NoStop {
		parts := strings.SplitN(rule, "/", 2)
		if len(parts) != 2 {
			continue
		}
		if parts[0] == ns && (parts[1] == "*" || parts[1] == name) {
			return true
		}
	}
	return false
}

func deploymentDisplayName(name string) string {
	parts := strings.Split(name, "-")
	for i, p := range parts {
		if len(p) > 0 {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, " ")
}

type k8sWorkload struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name        string            `json:"name"`
		Namespace   string            `json:"namespace"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec struct {
		Replicas *int `json:"replicas"`
	} `json:"spec"`
	Status struct {
		ReadyReplicas int `json:"readyReplicas"`
	} `json:"status"`
}

func listWorkloads(apiPath, kind string) []k8sWorkload {
	resp, err := k8sRequest("GET", apiPath, nil)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil
	}
	var result struct {
		Items []k8sWorkload `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil
	}
	for i := range result.Items {
		result.Items[i].Kind = kind
	}
	return result.Items
}

func listAllWorkloads() []k8sWorkload {
	var all []k8sWorkload
	all = append(all, listWorkloads("/apis/apps/v1/deployments", "Deployment")...)
	all = append(all, listWorkloads("/apis/apps/v1/statefulsets", "StatefulSet")...)
	all = append(all, listWorkloads("/apis/apps/v1/daemonsets", "DaemonSet")...)
	return all
}

type podMetric struct {
	Namespace string
	PodName   string
	Node      string
	RAM       int64
	CPU       int64
}

type podInfo struct {
	Node    string
	Phase   string
	Reason  string
	Message string
}

// listPodInfo queries /api/v1/pods once and returns per-pod node placement
// plus the worst-case status reason (waiting/terminated state, or
// PodScheduled=False for stuck-Pending pods).
func listPodInfo() map[string]podInfo {
	resp, err := k8sRequest("GET", "/api/v1/pods", nil)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil
	}
	var result struct {
		Items []struct {
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
			Spec struct {
				NodeName string `json:"nodeName"`
			} `json:"spec"`
			Status struct {
				Phase             string `json:"phase"`
				ContainerStatuses []struct {
					State struct {
						Waiting *struct {
							Reason  string `json:"reason"`
							Message string `json:"message"`
						} `json:"waiting"`
						Terminated *struct {
							Reason  string `json:"reason"`
							Message string `json:"message"`
						} `json:"terminated"`
					} `json:"state"`
				} `json:"containerStatuses"`
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
					Reason string `json:"reason"`
				} `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil
	}
	m := make(map[string]podInfo, len(result.Items))
	for _, p := range result.Items {
		info := podInfo{Node: p.Spec.NodeName, Phase: p.Status.Phase}
		for _, cs := range p.Status.ContainerStatuses {
			if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
				info.Reason = cs.State.Waiting.Reason
				info.Message = cs.State.Waiting.Message
				break
			}
			if cs.State.Terminated != nil && cs.State.Terminated.Reason != "" && cs.State.Terminated.Reason != "Completed" {
				info.Reason = cs.State.Terminated.Reason
				info.Message = cs.State.Terminated.Message
				break
			}
		}
		if info.Reason == "" && info.Phase == "Pending" {
			for _, c := range p.Status.Conditions {
				if c.Type == "PodScheduled" && c.Status == "False" {
					info.Reason = c.Reason
					if info.Reason == "" {
						info.Reason = "NotScheduled"
					}
					break
				}
			}
		}
		m[p.Metadata.Namespace+"/"+p.Metadata.Name] = info
	}
	return m
}

func getPodMetricsAll(nodeByPod map[string]podInfo) []podMetric {
	resp, err := k8sRequest("GET", "/apis/metrics.k8s.io/v1beta1/pods", nil)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil
	}
	var result struct {
		Items []struct {
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
			Containers []struct {
				Usage struct {
					Memory string `json:"memory"`
					CPU    string `json:"cpu"`
				} `json:"usage"`
			} `json:"containers"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil
	}
	var metrics []podMetric
	for _, pod := range result.Items {
		var ram, cpu int64
		for _, c := range pod.Containers {
			ram += parseK8sMemory(c.Usage.Memory)
			cpu += parseK8sCPU(c.Usage.CPU)
		}
		metrics = append(metrics, podMetric{
			Namespace: pod.Metadata.Namespace,
			PodName:   pod.Metadata.Name,
			Node:      nodeByPod[pod.Metadata.Namespace+"/"+pod.Metadata.Name].Node,
			RAM:       ram,
			CPU:       cpu,
		})
	}
	return metrics
}

type workloadUsage struct {
	RAM          int64
	CPU          int64
	Nodes        map[string]bool
	StatusDetail string
}

// matchPodsToWorkloads aggregates per-deployment RAM, node placement, and
// status issues. Pods are matched to workloads by deployment-name prefix on
// the pod name (stable because ReplicaSet hashes keep the prefix intact).
func matchPodsToWorkloads(podInfos map[string]podInfo, pods []podMetric, deploymentNames map[string]bool) map[string]*workloadUsage {
	usage := make(map[string]*workloadUsage)

	// Try the longest workload-name prefix first so pods like `loki-canary-xyz`
	// match the `loki-canary` DaemonSet instead of leaking into the `loki`
	// StatefulSet. Random map iteration would otherwise flip the assignment
	// between renders.
	sortedKeys := make([]string, 0, len(deploymentNames))
	for k := range deploymentNames {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Slice(sortedKeys, func(i, j int) bool {
		return len(sortedKeys[i]) > len(sortedKeys[j])
	})

	// Walk all pods once to aggregate RAM + nodes + worst status per workload.
	// podInfos covers every pod (metrics might miss some); we iterate both.
	matchPod := func(ns, podName string) (string, bool) {
		for _, key := range sortedKeys {
			parts := strings.SplitN(key, "/", 2)
			if parts[0] != ns {
				continue
			}
			if strings.HasPrefix(podName, parts[1]+"-") {
				return key, true
			}
		}
		return "", false
	}

	for fullName, info := range podInfos {
		parts := strings.SplitN(fullName, "/", 2)
		if len(parts) != 2 {
			continue
		}
		key, ok := matchPod(parts[0], parts[1])
		if !ok {
			continue
		}
		u, exists := usage[key]
		if !exists {
			u = &workloadUsage{Nodes: make(map[string]bool)}
			usage[key] = u
		}
		if info.Node != "" {
			u.Nodes[info.Node] = true
		}
		if u.StatusDetail == "" && info.Reason != "" {
			u.StatusDetail = info.Reason
			if info.Message != "" && len(info.Message) < 120 {
				u.StatusDetail += ": " + info.Message
			}
		}
	}

	for _, pod := range pods {
		key, ok := matchPod(pod.Namespace, pod.PodName)
		if !ok {
			continue
		}
		u, exists := usage[key]
		if !exists {
			u = &workloadUsage{Nodes: make(map[string]bool)}
			usage[key] = u
		}
		u.RAM += pod.RAM
		u.CPU += pod.CPU
	}
	return usage
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// parseK8sCPU returns CPU quantity in millicores. Accepts the K8s canonical
// formats: "500m" (millicores), "1234567n" (nanocores), "500000u" (microcores),
// or a bare number in whole cores ("2" -> 2000m).
func parseK8sCPU(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if strings.HasSuffix(s, "n") {
		var v int64
		fmt.Sscanf(s, "%dn", &v)
		return v / 1_000_000
	}
	if strings.HasSuffix(s, "u") {
		var v int64
		fmt.Sscanf(s, "%du", &v)
		return v / 1000
	}
	if strings.HasSuffix(s, "m") {
		var v int64
		fmt.Sscanf(s, "%dm", &v)
		return v
	}
	var v int64
	fmt.Sscanf(s, "%d", &v)
	return v * 1000
}

func parseK8sMemory(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if strings.HasSuffix(s, "Ki") {
		var v int64
		fmt.Sscanf(s, "%dKi", &v)
		return v * 1024
	}
	if strings.HasSuffix(s, "Mi") {
		var v int64
		fmt.Sscanf(s, "%dMi", &v)
		return v * 1024 * 1024
	}
	if strings.HasSuffix(s, "Gi") {
		var v int64
		fmt.Sscanf(s, "%dGi", &v)
		return v * 1024 * 1024 * 1024
	}
	var v int64
	fmt.Sscanf(s, "%d", &v)
	return v
}

// listNodes returns per-node capacity + Ready condition + cordoned state.
// metrics.k8s.io is queried separately to fill in RAMUsed (best-effort).
func listNodes() []NodeStatus {
	resp, err := k8sRequest("GET", "/api/v1/nodes", nil)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var nodes struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				Unschedulable bool `json:"unschedulable"`
			} `json:"spec"`
			Status struct {
				Capacity   map[string]string `json:"capacity"`
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil
	}
	result := make([]NodeStatus, 0, len(nodes.Items))
	for _, n := range nodes.Items {
		ready := false
		for _, c := range n.Status.Conditions {
			if c.Type == "Ready" && c.Status == "True" {
				ready = true
				break
			}
		}
		result = append(result, NodeStatus{
			Name:          n.Metadata.Name,
			Ready:         ready,
			Unschedulable: n.Spec.Unschedulable,
			RAMTotal:      parseK8sMemory(n.Status.Capacity["memory"]),
			CPUTotal:      parseK8sCPU(n.Status.Capacity["cpu"]),
		})
	}

	resp2, err := k8sRequest("GET", "/apis/metrics.k8s.io/v1beta1/nodes", nil)
	if err == nil {
		defer resp2.Body.Close()
		var metrics struct {
			Items []struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
				Usage struct {
					Memory string `json:"memory"`
					CPU    string `json:"cpu"`
				} `json:"usage"`
			} `json:"items"`
		}
		if json.NewDecoder(resp2.Body).Decode(&metrics) == nil {
			usedRAM := make(map[string]int64, len(metrics.Items))
			usedCPU := make(map[string]int64, len(metrics.Items))
			for _, m := range metrics.Items {
				usedRAM[m.Metadata.Name] = parseK8sMemory(m.Usage.Memory)
				usedCPU[m.Metadata.Name] = parseK8sCPU(m.Usage.CPU)
			}
			for i := range result {
				result[i].RAMUsed = usedRAM[result[i].Name]
				result[i].CPUUsed = usedCPU[result[i].Name]
			}
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// listIngressHosts walks every Traefik IngressRoute and maps each referenced
// backend service (by ns/name) to the list of Host(`…`) values in its match
// rules. The deployment name equals the service name by convention in this
// homelab, so we can correlate ingress hosts with deployments directly.
func listIngressHosts() map[string][]string {
	result := make(map[string][]string)
	resp, err := k8sRequest("GET", "/apis/traefik.io/v1alpha1/ingressroutes", nil)
	if err != nil {
		return result
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return result
	}
	var routes struct {
		Items []struct {
			Metadata struct {
				Namespace string `json:"namespace"`
			} `json:"metadata"`
			Spec struct {
				Routes []struct {
					Match    string `json:"match"`
					Services []struct {
						Name string `json:"name"`
					} `json:"services"`
				} `json:"routes"`
			} `json:"spec"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&routes); err != nil {
		return result
	}
	for _, ir := range routes.Items {
		ns := ir.Metadata.Namespace
		for _, route := range ir.Spec.Routes {
			hosts := parseHosts(route.Match)
			if len(hosts) == 0 {
				continue
			}
			for _, svc := range route.Services {
				key := ns + "/" + svc.Name
				result[key] = appendUnique(result[key], hosts)
			}
		}
	}
	return result
}

func parseHosts(match string) []string {
	var hosts []string
	for _, m := range hostRegex.FindAllStringSubmatch(match, -1) {
		for _, part := range strings.Split(m[1], ",") {
			h := strings.TrimSpace(part)
			h = strings.Trim(h, "`\"' ")
			if h != "" {
				hosts = append(hosts, h)
			}
		}
	}
	return hosts
}

func appendUnique(dst []string, src []string) []string {
	seen := make(map[string]bool, len(dst))
	for _, h := range dst {
		seen[h] = true
	}
	for _, h := range src {
		if !seen[h] {
			seen[h] = true
			dst = append(dst, h)
		}
	}
	return dst
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	data, _ := staticFiles.ReadFile("index.html")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(data)
}

func handleGetServices(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", 405)
		return
	}

	deployments := listAllWorkloads()
	podInfos := listPodInfo()
	podMetrics := getPodMetricsAll(podInfos)
	nodes := listNodes()
	ingressHosts := listIngressHosts()

	depKeys := make(map[string]bool)
	for _, d := range deployments {
		if !isHidden(d.Metadata.Namespace) {
			depKeys[d.Metadata.Namespace+"/"+d.Metadata.Name] = true
		}
	}
	usage := matchPodsToWorkloads(podInfos, podMetrics, depKeys)

	var statuses []ServiceStatus
	for _, d := range deployments {
		ns := d.Metadata.Namespace
		if isHidden(ns) {
			continue
		}
		replicas := 1
		if d.Spec.Replicas != nil {
			replicas = *d.Spec.Replicas
		}
		key := ns + "/" + d.Metadata.Name
		var ram, cpu int64
		var nodeList []string
		var detail string
		if u, ok := usage[key]; ok {
			ram = u.RAM
			cpu = u.CPU
			nodeList = sortedKeys(u.Nodes)
			detail = u.StatusDetail
		}
		statuses = append(statuses, ServiceStatus{
			DisplayName:   deploymentDisplayName(d.Metadata.Name),
			Name:          d.Metadata.Name,
			Namespace:     ns,
			Group:         groupName(ns),
			Kind:          d.Kind,
			CanStop:       !isNoStop(ns, d.Metadata.Name),
			Replicas:      replicas,
			Ready:         d.Status.ReadyReplicas > 0,
			RAMBytes:      ram,
			CPUMilli:      cpu,
			Nodes:         nodeList,
			URLs:          ingressHosts[key],
			StatusDetail:  detail,
			PreferredNode: d.Metadata.Annotations[PreferredNodeAnnotation],
		})
	}

	sort.Slice(statuses, func(i, j int) bool {
		if statuses[i].Group != statuses[j].Group {
			return statuses[i].Group < statuses[j].Group
		}
		return statuses[i].Name < statuses[j].Name
	})

	var totalRAMUsed, totalRAMCap, totalCPUUsed, totalCPUCap int64
	for _, n := range nodes {
		totalRAMUsed += n.RAMUsed
		totalRAMCap += n.RAMTotal
		totalCPUUsed += n.CPUUsed
		totalCPUCap += n.CPUTotal
	}
	resp := ServicesResponse{
		Services: statuses,
		Nodes:    nodes,
		RAMUsed:  totalRAMUsed,
		RAMTotal: totalRAMCap,
		CPUUsed:  totalCPUUsed,
		CPUTotal: totalCPUCap,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleScaleService(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	var req ScaleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, 400)
		return
	}
	if req.Replicas < 0 || req.Replicas > 1 {
		http.Error(w, `{"error":"replicas must be 0 or 1"}`, 400)
		return
	}
	if isHidden(req.Namespace) {
		http.Error(w, `{"error":"namespace not managed"}`, 403)
		return
	}
	if req.Replicas == 0 && isNoStop(req.Namespace, req.Name) {
		http.Error(w, `{"error":"this service cannot be stopped"}`, 403)
		return
	}

	err := scaleDeployment(req.Namespace, req.Name, req.Replicas)
	if err != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func handleScaleGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	var req GroupScaleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, 400)
		return
	}
	if req.Replicas < 0 || req.Replicas > 1 {
		http.Error(w, `{"error":"replicas must be 0 or 1"}`, 400)
		return
	}

	deployments := listAllWorkloads()
	var matched []k8sWorkload
	for _, d := range deployments {
		if groupName(d.Metadata.Namespace) == req.Group {
			matched = append(matched, d)
		}
	}
	if len(matched) == 0 {
		http.Error(w, `{"error":"unknown group"}`, 404)
		return
	}

	if req.Replicas == 0 {
		for i := len(matched) - 1; i >= 0; i-- {
			ns := matched[i].Metadata.Namespace
			name := matched[i].Metadata.Name
			if isNoStop(ns, name) {
				continue
			}
			if err := scaleDeployment(ns, name, 0); err != nil {
				log.Printf("Error stopping %s/%s: %v", ns, name, err)
			}
		}
	} else {
		for _, d := range matched {
			if err := scaleDeployment(d.Metadata.Namespace, d.Metadata.Name, 1); err != nil {
				log.Printf("Error starting %s/%s: %v", d.Metadata.Namespace, d.Metadata.Name, err)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func findWorkloadKind(ns, name string) string {
	workloads := listAllWorkloads()
	for _, w := range workloads {
		if w.Metadata.Namespace == ns && w.Metadata.Name == name {
			return w.Kind
		}
	}
	return "Deployment"
}

func resourceForKind(kind string) string {
	switch kind {
	case "StatefulSet":
		return "statefulsets"
	case "DaemonSet":
		return "daemonsets"
	}
	return "deployments"
}

func patchPausedAnnotation(ns, resource, name string, paused bool) error {
	var value string
	if paused {
		value = `"true"`
	} else {
		value = "null"
	}
	payload := fmt.Sprintf(`{"metadata":{"annotations":{"%s":%s}}}`, PausedAnnotation, value)
	resp, err := k8sRequest("PATCH",
		fmt.Sprintf("/apis/apps/v1/namespaces/%s/%s/%s", ns, resource, name),
		strings.NewReader(payload))
	if err != nil {
		return fmt.Errorf("annotation patch error: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("annotation failed (%d): %s", resp.StatusCode, string(body))
	}
	return nil
}

func scaleDeployment(ns, name string, replicas int) error {
	kind := findWorkloadKind(ns, name)

	mu.Lock()
	defer mu.Unlock()

	resource := resourceForKind(kind)

	if kind == "DaemonSet" {
		var payload string
		if replicas == 0 {
			payload = `{"spec":{"template":{"spec":{"nodeSelector":{"non-existing":"true"}}}}}`
		} else {
			payload = `{"spec":{"template":{"spec":{"nodeSelector":null}}}}`
		}
		resp, err := k8sRequest("PATCH",
			fmt.Sprintf("/apis/apps/v1/namespaces/%s/daemonsets/%s", ns, name),
			strings.NewReader(payload))
		if err != nil {
			return fmt.Errorf("k8s API error: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("scale failed (%d): %s", resp.StatusCode, string(body))
		}
	} else {
		payload := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
		resp, err := k8sRequest("PATCH",
			fmt.Sprintf("/apis/apps/v1/namespaces/%s/%s/%s/scale", ns, resource, name),
			strings.NewReader(payload))
		if err != nil {
			return fmt.Errorf("k8s API error: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("scale failed (%d): %s", resp.StatusCode, string(body))
		}
	}

	if err := patchPausedAnnotation(ns, resource, name, replicas == 0); err != nil {
		log.Printf("Service %s/%s annotation update failed: %v", ns, name, err)
	}

	action := "stopped"
	if replicas > 0 {
		action = "started"
	}
	log.Printf("Service %s/%s (%s) %s", ns, name, kind, action)
	return nil
}

func restartDeployment(ns, name string) error {
	kind := findWorkloadKind(ns, name)

	mu.Lock()
	defer mu.Unlock()
	resource := resourceForKind(kind)

	timestamp := time.Now().Format(time.RFC3339)
	payload := fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":"%s"}}}}}`, timestamp)
	resp, err := k8sRequest("PATCH",
		fmt.Sprintf("/apis/apps/v1/namespaces/%s/%s/%s", ns, resource, name),
		strings.NewReader(payload))
	if err != nil {
		return fmt.Errorf("k8s API error: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("restart failed (%d): %s", resp.StatusCode, string(body))
	}
	log.Printf("Service %s/%s (%s) restarted", ns, name, kind)
	return nil
}

func handleRestartService(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	var req ScaleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, 400)
		return
	}
	if isHidden(req.Namespace) {
		http.Error(w, `{"error":"namespace not managed"}`, 403)
		return
	}
	err := restartDeployment(req.Namespace, req.Name)
	if err != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func handleCordonNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	var req CordonRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, 400)
		return
	}
	if req.Node == "" {
		http.Error(w, `{"error":"node required"}`, 400)
		return
	}
	payload := fmt.Sprintf(`{"spec":{"unschedulable":%t}}`, req.Cordon)
	resp, err := k8sRequest("PATCH", "/api/v1/nodes/"+req.Node, strings.NewReader(payload))
	if err != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		w.WriteHeader(resp.StatusCode)
		w.Write(body)
		return
	}
	state := "uncordoned"
	if req.Cordon {
		state = "cordoned"
	}
	log.Printf("Node %s %s", req.Node, state)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// operatorOwner tracks a prometheus-operator CR that owns a StatefulSet.
// Patching the StatefulSet's nodeAffinity gets reverted on the next operator
// reconcile, so we patch the CR's spec.affinity and let the operator propagate.
type operatorOwner struct {
	apiPath string
	kind    string
	name    string
}

// findOperatorOwner returns the owning prometheus-operator CR (Prometheus,
// Alertmanager, ThanosRuler) when the given StatefulSet is managed by one.
func findOperatorOwner(ns, kind, name string) (*operatorOwner, bool) {
	if kind != "StatefulSet" {
		return nil, false
	}
	resp, err := k8sRequest("GET",
		fmt.Sprintf("/apis/apps/v1/namespaces/%s/statefulsets/%s", ns, name), nil)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	var sts struct {
		Metadata struct {
			OwnerReferences []struct {
				APIVersion string `json:"apiVersion"`
				Kind       string `json:"kind"`
				Name       string `json:"name"`
			} `json:"ownerReferences"`
		} `json:"metadata"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&sts); err != nil {
		return nil, false
	}
	for _, o := range sts.Metadata.OwnerReferences {
		if !strings.HasPrefix(o.APIVersion, "monitoring.coreos.com/") {
			continue
		}
		var resource string
		switch o.Kind {
		case "Prometheus":
			resource = "prometheuses"
		case "Alertmanager":
			resource = "alertmanagers"
		case "ThanosRuler":
			resource = "thanosrulers"
		default:
			continue
		}
		return &operatorOwner{
			apiPath: fmt.Sprintf("/apis/%s/namespaces/%s/%s/%s", o.APIVersion, ns, resource, o.Name),
			kind:    o.Kind,
			name:    o.Name,
		}, true
	}
	return nil, false
}

// patchOperatorAffinity merges nodeAffinity into the CR's spec.affinity,
// preserving any existing podAntiAffinity/podAffinity set by the chart.
func patchOperatorAffinity(apiPath string, node string) error {
	resp, err := k8sRequest("GET", apiPath, nil)
	if err != nil {
		return fmt.Errorf("fetch CR: %w", err)
	}
	var cr struct {
		Spec struct {
			Affinity map[string]interface{} `json:"affinity"`
		} `json:"spec"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		resp.Body.Close()
		return fmt.Errorf("decode CR: %w", err)
	}
	resp.Body.Close()

	if cr.Spec.Affinity == nil {
		cr.Spec.Affinity = map[string]interface{}{}
	}
	if node == "" {
		delete(cr.Spec.Affinity, "nodeAffinity")
	} else {
		cr.Spec.Affinity["nodeAffinity"] = map[string]interface{}{
			"preferredDuringSchedulingIgnoredDuringExecution": []map[string]interface{}{{
				"weight": 100,
				"preference": map[string]interface{}{
					"matchExpressions": []map[string]interface{}{{
						"key":      "kubernetes.io/hostname",
						"operator": "In",
						"values":   []string{node},
					}},
				},
			}},
		}
	}

	var affinityValue interface{} = cr.Spec.Affinity
	if len(cr.Spec.Affinity) == 0 {
		affinityValue = nil
	}
	payload, err := json.Marshal(map[string]interface{}{
		"spec": map[string]interface{}{"affinity": affinityValue},
	})
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}
	resp2, err := k8sRequest("PATCH", apiPath, strings.NewReader(string(payload)))
	if err != nil {
		return fmt.Errorf("patch CR: %w", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode >= 300 {
		body, _ := io.ReadAll(resp2.Body)
		return fmt.Errorf("CR patch failed (%d): %s", resp2.StatusCode, string(body))
	}
	return nil
}

// setPreferredNode writes both the switchboard.io/preferred-node annotation
// (survives as documentation) and the matching soft nodeAffinity on the pod
// template (takes scheduling effect). On rebuild the setup scripts re-apply
// the Deployment, so service-scaledown re-patches the affinity from the
// annotation to keep scheduling intent across rebuilds.
//
// For StatefulSets managed by prometheus-operator (Prometheus, Alertmanager,
// ThanosRuler), the operator reverts direct StatefulSet affinity patches on
// every reconcile. We detect those via ownerReferences and patch the owning
// CR's spec.affinity instead — merging nodeAffinity with any existing
// podAntiAffinity so chart defaults survive.
func setPreferredNode(ns, name, node string) error {
	kind := findWorkloadKind(ns, name)
	if kind == "DaemonSet" {
		return fmt.Errorf("preferred node not supported for DaemonSets")
	}
	resource := resourceForKind(kind)

	var annotationVal, affinity string
	if node == "" {
		annotationVal = "null"
		affinity = "null"
	} else {
		annotationVal = fmt.Sprintf(`"%s"`, node)
		affinity = fmt.Sprintf(
			`{"nodeAffinity":{"preferredDuringSchedulingIgnoredDuringExecution":[{"weight":100,"preference":{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["%s"]}]}}]}}`,
			node)
	}

	if owner, ok := findOperatorOwner(ns, kind, name); ok {
		annPayload := fmt.Sprintf(`{"metadata":{"annotations":{"%s":%s}}}`, PreferredNodeAnnotation, annotationVal)
		resp, err := k8sRequest("PATCH",
			fmt.Sprintf("/apis/apps/v1/namespaces/%s/%s/%s", ns, resource, name),
			strings.NewReader(annPayload))
		if err != nil {
			return fmt.Errorf("annotation patch error: %w", err)
		}
		resp.Body.Close()
		if err := patchOperatorAffinity(owner.apiPath, node); err != nil {
			return err
		}
		log.Printf("Service %s/%s preferred node set to %q (via %s/%s)", ns, name, node, owner.kind, owner.name)
		return nil
	}

	payload := fmt.Sprintf(
		`{"metadata":{"annotations":{"%s":%s}},"spec":{"template":{"spec":{"affinity":%s}}}}`,
		PreferredNodeAnnotation, annotationVal, affinity)
	resp, err := k8sRequest("PATCH",
		fmt.Sprintf("/apis/apps/v1/namespaces/%s/%s/%s", ns, resource, name),
		strings.NewReader(payload))
	if err != nil {
		return fmt.Errorf("k8s API error: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("patch failed (%d): %s", resp.StatusCode, string(body))
	}
	log.Printf("Service %s/%s preferred node set to %q", ns, name, node)
	return nil
}

func handleSetPreferredNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	var req PreferredNodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, 400)
		return
	}
	if isHidden(req.Namespace) {
		http.Error(w, `{"error":"namespace not managed"}`, 403)
		return
	}
	if err := setPreferredNode(req.Namespace, req.Name, req.Node); err != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleRebalance assigns a preferred node to every running, stoppable
// Deployment/StatefulSet using best-fit-decreasing by RAM: sort workloads by
// current RAM desc, then place each on the schedulable node with the least
// assigned load so far. Skips hidden namespaces, noStop entries (so critical
// infra stays where it is), and DaemonSets (not scheduled by affinity).
func handleRebalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", 405)
		return
	}

	nodes := listNodes()
	var targets []string
	for _, n := range nodes {
		if n.Ready && !n.Unschedulable {
			targets = append(targets, n.Name)
		}
	}
	if len(targets) == 0 {
		http.Error(w, `{"error":"no schedulable nodes"}`, 409)
		return
	}

	workloads := listAllWorkloads()
	podInfos := listPodInfo()
	podMetrics := getPodMetricsAll(podInfos)

	depKeys := make(map[string]bool)
	for _, d := range workloads {
		if d.Kind == "DaemonSet" || isHidden(d.Metadata.Namespace) {
			continue
		}
		depKeys[d.Metadata.Namespace+"/"+d.Metadata.Name] = true
	}
	usage := matchPodsToWorkloads(podInfos, podMetrics, depKeys)

	type candidate struct {
		ns, name string
		ram      int64
	}
	var candidates []candidate
	for _, d := range workloads {
		if d.Kind == "DaemonSet" {
			continue
		}
		ns := d.Metadata.Namespace
		name := d.Metadata.Name
		if isHidden(ns) || isNoStop(ns, name) {
			continue
		}
		replicas := 1
		if d.Spec.Replicas != nil {
			replicas = *d.Spec.Replicas
		}
		if replicas == 0 {
			continue
		}
		var ram int64
		if u, ok := usage[ns+"/"+name]; ok {
			ram = u.RAM
		}
		candidates = append(candidates, candidate{ns, name, ram})
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].ram != candidates[j].ram {
			return candidates[i].ram > candidates[j].ram
		}
		return candidates[i].ns+"/"+candidates[i].name < candidates[j].ns+"/"+candidates[j].name
	})

	load := make(map[string]int64, len(targets))
	assignments := make(map[string]string, len(candidates))
	for _, c := range candidates {
		best := targets[0]
		for _, n := range targets[1:] {
			if load[n] < load[best] {
				best = n
			}
		}
		assignments[c.ns+"/"+c.name] = best
		// Zero-RAM workloads still consume a slot; charge 1 to spread them out.
		charge := c.ram
		if charge == 0 {
			charge = 1
		}
		load[best] += charge
	}

	var errs []string
	for key, node := range assignments {
		parts := strings.SplitN(key, "/", 2)
		if err := setPreferredNode(parts[0], parts[1], node); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", key, err))
		}
	}

	resp := map[string]interface{}{
		"status":      "ok",
		"assignments": assignments,
		"load":        load,
	}
	if len(errs) > 0 {
		resp["errors"] = errs
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
	log.Printf("Rebalance complete: %d workloads across %d nodes", len(assignments), len(targets))
}

func handlePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")

	if r.Method == "OPTIONS" {
		w.WriteHeader(200)
		return
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/ping/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		http.Error(w, "Not found", 404)
		return
	}
	ns, name := parts[0], parts[1]

	resp, err := k8sRequest("GET", fmt.Sprintf("/apis/apps/v1/namespaces/%s/deployments/%s", ns, name), nil)
	if err != nil {
		w.WriteHeader(503)
		fmt.Fprintf(w, "error")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		w.WriteHeader(503)
		fmt.Fprintf(w, "not found")
		return
	}
	var result struct {
		Spec struct {
			Replicas *int `json:"replicas"`
		} `json:"spec"`
		Status struct {
			ReadyReplicas int `json:"readyReplicas"`
		} `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		w.WriteHeader(503)
		fmt.Fprintf(w, "error")
		return
	}
	replicas := 1
	if result.Spec.Replicas != nil {
		replicas = *result.Spec.Replicas
	}
	if replicas == 0 || result.Status.ReadyReplicas == 0 {
		w.WriteHeader(503)
		fmt.Fprintf(w, "stopped")
		return
	}
	w.WriteHeader(200)
	fmt.Fprintf(w, "running")
}

func main() {
	loadConfig()

	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/api/services", handleGetServices)
	http.HandleFunc("/api/services/scale", handleScaleService)
	http.HandleFunc("/api/services/restart", handleRestartService)
	http.HandleFunc("/api/services/preferred-node", handleSetPreferredNode)
	http.HandleFunc("/api/groups/scale", handleScaleGroup)
	http.HandleFunc("/api/nodes/cordon", handleCordonNode)
	http.HandleFunc("/api/nodes/rebalance", handleRebalance)
	http.HandleFunc("/api/ping/", handlePing)

	log.Println("Switchboard listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
