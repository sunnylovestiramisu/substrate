// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plugin

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	imageprovider "github.com/ai-on-gke/image-provider-plugin/api/imageprovider/v1alpha1"
	"github.com/ai-on-gke/image-provider-plugin/client"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// fakeRootfsMounter records overlay mounts instead of making them.
type fakeRootfsMounter struct {
	mu       sync.Mutex
	mounted  map[string]int
	mounts   [][]string
	unmounts []string
	mountErr error
}

func newFakeRootfsMounter() *fakeRootfsMounter {
	return &fakeRootfsMounter{mounted: make(map[string]int)}
}

func (f *fakeRootfsMounter) Mount(target string, lowerDirs []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mountErr != nil {
		return f.mountErr
	}
	f.mounts = append(f.mounts, slices.Clone(lowerDirs))
	f.mounted[target]++
	return nil
}

func (f *fakeRootfsMounter) Unmount(target string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mounted[target] == 0 {
		return unix.EINVAL
	}
	f.mounted[target]--
	f.unmounts = append(f.unmounts, target)
	return nil
}

func (f *fakeRootfsMounter) IsMounted(target string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mounted[target] > 0, nil
}

// lost forgets every mount, as if the host lost them.
func (f *fakeRootfsMounter) lost() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.mounted)
}

func (f *fakeRootfsMounter) counts() (mounts, unmounts int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.mounts), len(f.unmounts)
}

func newRootfsTestDriver(t *testing.T, diffIDs []string, m RootfsMounter, opts ...Option) (*Driver, *fakeSnapshotsClient) {
	t.Helper()
	snap := newFakeSnapshotsClient(t, diffIDs)
	opts = append([]Option{
		WithWorkDir(filepath.Join(t.TempDir(), "work")),
		WithSnapshotsClient(snap),
		WithImageResolver(staticResolver(diffIDs)),
		WithRootfsMounter(m),
	}, opts...)
	d, err := NewDriver("test-provider", "", "test-snap", opts...)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d, snap
}

func prepareRootfs(t *testing.T, d *Driver, ref string) (*client.PrepareResult, string) {
	t.Helper()
	res, dir, err := d.PrepareRootfs(t.Context(), &client.PrepareRequest{ImageRef: ref})
	if err != nil {
		t.Fatalf("PrepareRootfs(%s): %v", ref, err)
	}
	return res, dir
}

// startRawServer serves d and returns a raw API client, which can ask for
// any layer form.
func startRawServer(t *testing.T, d *Driver) imageprovider.ImageProviderPluginClient {
	t.Helper()
	sock := filepath.Join(socketDir(t), "p.sock")
	lis, err := ListenUnixSocket(sock)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan struct{})
	go func() {
		_ = serve(ctx, lis, d)
		close(stopped)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
	conn, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return imageprovider.NewImageProviderPluginClient(conn)
}

func TestServerRootfsForm(t *testing.T) {
	m := newFakeRootfsMounter()
	d, _ := newRootfsTestDriver(t, testDiffIDs, m, WithLeaseIdleTTL(0), WithDrainTimeout(time.Second))
	c := startRawServer(t, d)
	ctx := t.Context()
	const ref = "example.com/app:v1"
	workDir := d.imageWorkDir(ref)
	rootfs := filepath.Join(workDir, "rootfs")

	for form, want := range map[imageprovider.LayerForm]bool{
		imageprovider.LayerForm_LAYER_FORM_UNSPECIFIED: true,
		imageprovider.LayerForm_LAYER_FORM_DIRS:        true,
		imageprovider.LayerForm_LAYER_FORM_ROOTFS:      true,
		imageprovider.LayerForm_LAYER_FORM_EROFS:       false,
	} {
		res, err := c.CanProvide(ctx, &imageprovider.CanProvideRequest{ImageRef: ref, Form: form})
		if err != nil || res.GetCanProvide() != want {
			t.Errorf("CanProvide(%s) = %v, %v; want %v", form, res.GetCanProvide(), err, want)
		}
	}
	if _, err := c.PrepareImage(ctx, &imageprovider.PrepareImageRequest{ImageRef: ref, Form: imageprovider.LayerForm_LAYER_FORM_EROFS}); status.Code(err) != codes.Unimplemented {
		t.Errorf("PrepareImage(EROFS) = %v, want Unimplemented", err)
	}

	// DIRS takes a lease and makes no rootfs.
	res, err := c.PrepareImage(ctx, &imageprovider.PrepareImageRequest{ImageRef: ref, Form: imageprovider.LayerForm_LAYER_FORM_DIRS})
	if err != nil {
		t.Fatalf("PrepareImage(DIRS): %v", err)
	}
	if got := res.GetLayerDirs().GetDirs(); len(got) != 2 || got[0] != filepath.Join(workDir, "layer-0") {
		t.Errorf("PrepareImage(DIRS) dirs = %v, want 2 layers under %s", got, workDir)
	}
	if _, err := os.Lstat(rootfs); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("DIRS made a rootfs: %v", err)
	}

	for range 2 {
		res, err := c.PrepareImage(ctx, &imageprovider.PrepareImageRequest{ImageRef: ref, Form: imageprovider.LayerForm_LAYER_FORM_ROOTFS})
		if err != nil {
			t.Fatalf("PrepareImage(ROOTFS): %v", err)
		}
		if res.GetRootfs().GetDir() != rootfs || res.GetLayerDirs() != nil {
			t.Errorf("PrepareImage(ROOTFS) layers = %v, want rootfs %s", res.GetLayers(), rootfs)
		}
		if res.GetProviderName() != "test-provider" || res.GetImageDigest() != testDigest {
			t.Errorf("PrepareImage(ROOTFS) = %v, want the image's digest and provider", res)
		}
	}
	m.mu.Lock()
	mounts := slices.Clone(m.mounts)
	m.mu.Unlock()
	want := [][]string{{filepath.Join(workDir, "layer-1", "fs"), filepath.Join(workDir, "layer-0", "fs")}}
	if !slices.EqualFunc(mounts, want, slices.Equal) {
		t.Errorf("mounts = %v, want one overlay with lowerdirs top first %v", mounts, want)
	}

	// One DIRS lease and two ROOTFS leases.
	for range 3 {
		if _, err := c.ReleaseImage(ctx, &imageprovider.ReleaseImageRequest{ImageRef: ref}); err != nil {
			t.Fatalf("ReleaseImage: %v", err)
		}
	}
	if _, unmounts := m.counts(); unmounts != 1 {
		t.Errorf("unmounted the rootfs %d times, want 1", unmounts)
	}
	if exists(workDir) {
		t.Error("work dir still exists after the last lease was released")
	}
}

func TestDriverRootfsSingleLayer(t *testing.T) {
	m := newFakeRootfsMounter()
	d, _ := newRootfsTestDriver(t, testDiffIDs[:1], m, WithLeaseIdleTTL(0))
	const ref = "example.com/one-layer:v1"
	res, dir := prepareRootfs(t, d, ref)
	_, dir2 := prepareRootfs(t, d, ref)
	if dir != dir2 || dir != filepath.Join(d.imageWorkDir(ref), "rootfs") {
		t.Errorf("rootfs = %s, %s; want %s/rootfs both times", dir, dir2, d.imageWorkDir(ref))
	}
	if target, err := os.Readlink(dir); err != nil || target != filepath.Join(res.LayerDirs[0], "fs") {
		t.Errorf("rootfs links to %q, %v; want %s/fs", target, err, res.LayerDirs[0])
	}
	if b, err := os.ReadFile(filepath.Join(dir, "hello.txt")); err != nil || string(b) != "streamed" {
		t.Errorf("reading through rootfs = %q, %v", b, err)
	}
	if mounts, _ := m.counts(); mounts != 0 {
		t.Errorf("mounted %d overlays for one layer, want 0", mounts)
	}
	release(t, d, ref)
	release(t, d, ref)
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("rootfs link survived the last release: %v", err)
	}
}

// The rootfs is unmounted when the idle image is drained, and a restarted
// plugin keeps or remounts the rootfs of images in use and unmounts the rest.
func TestDriverRootfsTeardown(t *testing.T) {
	m := newFakeRootfsMounter()
	d, snap := newRootfsTestDriver(t, testDiffIDs, m)
	const used, unused, idle = "example.com/used:v1", "example.com/unused:v1", "example.com/idle:v1"
	_, usedDir := prepareRootfs(t, d, used)
	_, unusedDir := prepareRootfs(t, d, unused)
	_ = d.Close()

	nd, err := NewDriver(d.name, "", d.snapshotterName,
		WithWorkDir(d.workDir),
		WithSnapshotsClient(snap),
		WithImageResolver(staticResolver(testDiffIDs)),
		WithRootfsMounter(m),
		WithDrainTimeout(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nd.Close() })
	active := []*client.ActiveImageLayers{{ImageRef: used, RefCount: 1}}
	if err := nd.ReconcileImages(t.Context(), active); err != nil {
		t.Fatalf("ReconcileImages: %v", err)
	}
	if mounts, unmounts := m.counts(); mounts != 2 || unmounts != 1 {
		t.Errorf("after reconcile: %d mounts, %d unmounts; want 2, 1", mounts, unmounts)
	}
	if exists(unusedDir) || !exists(usedDir) {
		t.Error("reconcile kept the wrong rootfs")
	}

	// The host lost the mount. Reconcile mounts it again.
	m.lost()
	if err := nd.ReconcileImages(t.Context(), active); err != nil {
		t.Fatalf("ReconcileImages: %v", err)
	}
	if ok, _ := m.IsMounted(usedDir); !ok {
		t.Error("reconcile did not mount a lost rootfs again")
	}

	_, idleDir := prepareRootfs(t, nd, idle)
	release(t, nd, idle)
	if !exists(idleDir) {
		t.Fatal("idle image lost its rootfs before its idle TTL")
	}
	nd.Drain(t.Context())
	if ok, _ := m.IsMounted(idleDir); ok || exists(filepath.Dir(idleDir)) {
		t.Error("drain left the idle image's rootfs mounted")
	}
}

// When the overlay cannot be mounted, PrepareRootfs fails and returns the
// lease it took.
func TestDriverRootfsMountFails(t *testing.T) {
	m := newFakeRootfsMounter()
	m.mountErr = unix.EPERM
	d, _ := newRootfsTestDriver(t, testDiffIDs, m, WithLeaseIdleTTL(0))
	const ref = "example.com/app:v1"
	if _, _, err := d.PrepareRootfs(t.Context(), &client.PrepareRequest{ImageRef: ref}); !errors.Is(err, unix.EPERM) {
		t.Fatalf("PrepareRootfs = %v, want EPERM", err)
	}
	if exists(d.imageWorkDir(ref)) {
		t.Error("work dir survived a failed PrepareRootfs")
	}
}

// A cold preparation over a stale rootfs mounts it again on the new views.
func TestDriverRootfsRepreparedLayers(t *testing.T) {
	m := newFakeRootfsMounter()
	d, _ := newRootfsTestDriver(t, testDiffIDs, m)
	const ref = "example.com/app:v1"
	res, dir := prepareRootfs(t, d, ref)
	if err := os.Remove(filepath.Join(res.LayerDirs[1], "fs")); err != nil {
		t.Fatal(err)
	}
	prepareRootfs(t, d, ref)
	if mounts, unmounts := m.counts(); mounts != 2 || unmounts != 1 {
		t.Errorf("%d mounts, %d unmounts; want the stale rootfs replaced", mounts, unmounts)
	}
	if ok, _ := m.IsMounted(dir); !ok {
		t.Error("rootfs not mounted after the layers were prepared again")
	}
}

// TestOverlayMounter needs root, for example under "unshare -rm".
func TestOverlayMounter(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to mount overlayfs")
	}
	dir := t.TempDir()
	var lowers []string
	for _, f := range []string{"bottom", "top"} {
		l := filepath.Join(dir, f)
		if err := os.MkdirAll(l, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(l, "f"), []byte(f), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(l, f), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		lowers = slices.Insert(lowers, 0, l)
	}
	target := filepath.Join(dir, "rootfs")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	var m OverlayMounter
	if err := m.Mount(target, lowers); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	if ok, err := m.IsMounted(target); err != nil || !ok {
		t.Fatalf("IsMounted = %v, %v; want true", ok, err)
	}
	if b, err := os.ReadFile(filepath.Join(target, "f")); err != nil || string(b) != "top" {
		t.Errorf("f = %q, %v; want the top layer's", b, err)
	}
	if !exists(filepath.Join(target, "bottom")) || !exists(filepath.Join(target, "top")) {
		t.Error("overlay does not merge both layers")
	}
	if err := os.WriteFile(filepath.Join(target, "new"), nil, 0o644); err == nil {
		t.Error("overlay is writable")
	}
	if err := m.Unmount(target); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
	if ok, err := m.IsMounted(target); err != nil || ok {
		t.Errorf("IsMounted after Unmount = %v, %v; want false", ok, err)
	}
}

// TestDriverRootfsOverlay needs root, for example under "unshare -rm".
func TestDriverRootfsOverlay(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to mount overlayfs")
	}
	d, _ := newRootfsTestDriver(t, testDiffIDs, OverlayMounter{}, WithLeaseIdleTTL(0))
	const ref = "example.com/app:v1"
	_, dir := prepareRootfs(t, d, ref)
	if b, err := os.ReadFile(filepath.Join(dir, "hello.txt")); err != nil || string(b) != "streamed" {
		t.Errorf("reading through rootfs = %q, %v", b, err)
	}
	release(t, d, ref)
	if exists(d.imageWorkDir(ref)) {
		t.Error("work dir survived the last release")
	}
}
