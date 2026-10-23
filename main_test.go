package main

import (
	"reflect"
	"testing"
)

func TestImageTag(t *testing.T) {
	cases := map[string]string{
		"ghcr.io/foo/bar:1.2.3":                         "1.2.3",
		"nginx":                                         "latest",
		"registry.local:5000/foo/bar":                   "latest",
		"registry.local:5000/foo/bar:v2":                "v2",
		"ghcr.io/foo/bar:1.2.3@sha256:0123456789abcdef": "1.2.3",
		"ghcr.io/foo/bar@sha256:0123456789abcdef0123":   "0123456789ab",
	}
	for image, want := range cases {
		if got := imageTag(image); got != want {
			t.Errorf("imageTag(%q) = %q, want %q", image, got, want)
		}
	}
}

func TestWorkloadVersionPrefersContainerNamedLikeWorkload(t *testing.T) {
	containers := []k8sContainer{
		{Name: "sidecar", Image: "busybox:1.36"},
		{Name: "app", Image: "ghcr.io/foo/app:2.0.0"},
	}
	version, images := workloadVersion("app", containers)
	if version != "2.0.0" {
		t.Errorf("version = %q, want 2.0.0", version)
	}
	want := []string{"sidecar: busybox:1.36", "app: ghcr.io/foo/app:2.0.0"}
	if !reflect.DeepEqual(images, want) {
		t.Errorf("images = %v, want %v", images, want)
	}
}

func TestWorkloadVersionFallsBackToFirstContainer(t *testing.T) {
	version, _ := workloadVersion("other", []k8sContainer{{Name: "a", Image: "x:1"}, {Name: "b", Image: "y:2"}})
	if version != "1" {
		t.Errorf("version = %q, want 1", version)
	}
	if version, images := workloadVersion("x", nil); version != "" || images != nil {
		t.Errorf("empty containers gave %q %v", version, images)
	}
}

func TestMachineState(t *testing.T) {
	ready := &NodeStatus{Name: "n", Ready: true}
	down := &NodeStatus{Name: "n", Ready: false}
	cases := []struct {
		node    *NodeStatus
		desired string
		want    string
	}{
		{nil, "auto", "off"},
		{nil, "off", "off"},
		{nil, "on", "waking"},
		{down, "auto", "unreachable"},
		{ready, "auto", "on"},
		{ready, "on", "on"},
		{ready, "off", "sleeping"},
	}
	for _, c := range cases {
		if got := machineState(c.node, c.desired); got != c.want {
			t.Errorf("machineState(%v, %q) = %q, want %q", c.node, c.desired, got, c.want)
		}
	}
}
