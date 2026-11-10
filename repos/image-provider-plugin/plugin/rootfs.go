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
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/ai-on-gke/image-provider-plugin/client"
	"golang.org/x/sys/unix"
)

// rootfsDirName is the merged root filesystem in an image's work dir.
const rootfsDirName = "rootfs"

// maxRootfsUnmounts bounds the unmounts of stacked mounts on a rootfs.
const maxRootfsUnmounts = 8

// RootfsMounter mounts the read-only overlayfs that merges an image's
// layers. The default is OverlayMounter.
type RootfsMounter interface {
	// Mount mounts a read-only overlayfs of lowerDirs, topmost first, on
	// target.
	Mount(target string, lowerDirs []string) error
	// Unmount detaches the mount on target.
	Unmount(target string) error
	// IsMounted reports whether a filesystem other than its parent's is
	// mounted on target.
	IsMounted(target string) (bool, error)
}

// WithRootfsMounter replaces OverlayMounter.
func WithRootfsMounter(m RootfsMounter) Option {
	return func(d *Driver) { d.rootfsMounter = m }
}

// OverlayMounter mounts kernel overlayfs.
type OverlayMounter struct{}

// Mount adds each lower dir with "lowerdir+", which has no limit on the
// total length of the paths and needs Linux 6.8. On older kernels it falls
// back to mount(2) with "lowerdir=", whose options must fit in a page.
func (OverlayMounter) Mount(target string, lowerDirs []string) error {
	err := mountOverlayFsconfig(target, lowerDirs)
	if err == nil {
		return nil
	}
	opts := "lowerdir=" + strings.Join(lowerDirs, ":")
	if len(opts) >= os.Getpagesize() {
		return fmt.Errorf("mounting overlayfs with lowerdir+: %w; lowerdir= options are too long for mount(2)", err)
	}
	if lerr := unix.Mount("overlay", target, "overlay", unix.MS_RDONLY, opts); lerr != nil {
		return fmt.Errorf("mounting overlayfs: %w (with lowerdir+: %w)", lerr, err)
	}
	return nil
}

func mountOverlayFsconfig(target string, lowerDirs []string) error {
	fsfd, err := unix.Fsopen("overlay", unix.FSOPEN_CLOEXEC)
	if err != nil {
		return fmt.Errorf("fsopen: %w", err)
	}
	defer func() { _ = unix.Close(fsfd) }()
	for _, l := range lowerDirs {
		if err := unix.FsconfigSetString(fsfd, "lowerdir+", l); err != nil {
			return fmt.Errorf("adding lowerdir %s: %w", l, err)
		}
	}
	if err := unix.FsconfigCreate(fsfd); err != nil {
		return fmt.Errorf("creating overlayfs: %w", err)
	}
	mfd, err := unix.Fsmount(fsfd, unix.FSMOUNT_CLOEXEC, unix.MOUNT_ATTR_RDONLY)
	if err != nil {
		return fmt.Errorf("fsmount: %w", err)
	}
	defer func() { _ = unix.Close(mfd) }()
	if err := unix.MoveMount(mfd, "", unix.AT_FDCWD, target, unix.MOVE_MOUNT_F_EMPTY_PATH); err != nil {
		return fmt.Errorf("move_mount to %s: %w", target, err)
	}
	return nil
}

// Unmount lazily detaches target, so sandboxes that still use it keep it.
func (OverlayMounter) Unmount(target string) error {
	return unix.Unmount(target, unix.MNT_DETACH|unix.UMOUNT_NOFOLLOW)
}

// IsMounted compares the device of target with that of its parent.
func (OverlayMounter) IsMounted(target string) (bool, error) {
	var st, parent unix.Stat_t
	if err := unix.Lstat(target, &st); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return false, nil
		}
		return false, err
	}
	if err := unix.Lstat(filepath.Dir(target), &parent); err != nil {
		return false, err
	}
	return st.Dev != parent.Dev, nil
}

// PrepareRootfs prepares the image as PrepareImage does, then merges its
// layers in <image work dir>/rootfs and returns that directory. The rootfs
// is a symlink to the layer view for a single layer, and a read-only
// overlayfs of the layers otherwise. It returns an error wrapping
// client.ErrNotSupported when PrepareImage declines the image.
func (d *Driver) PrepareRootfs(ctx context.Context, req *client.PrepareRequest) (*client.PrepareResult, string, error) {
	res, err := d.PrepareImage(ctx, req)
	if err != nil {
		return nil, "", err
	}
	dir, err := d.ensureRootfs(d.imageWorkDir(req.ImageRef), res.LayerDirs)
	if err != nil {
		if rerr := d.ReleaseImage(context.WithoutCancel(ctx), &client.ReleaseRequest{ImageRef: req.ImageRef}); rerr != nil {
			slog.WarnContext(ctx, "Failed to release the lease of an image without rootfs",
				slog.String("provider", d.name),
				slog.String("image", req.ImageRef),
				slog.Any("err", rerr))
		}
		return nil, "", fmt.Errorf("preparing rootfs of %s: %w", req.ImageRef, err)
	}
	return res, dir, nil
}

// rootfsLock returns the mutex that guards workDir's rootfs.
func (d *Driver) rootfsLock(workDir string) *sync.Mutex {
	d.mu.Lock()
	defer d.mu.Unlock()
	lk, ok := d.rootfsLocks[workDir]
	if !ok {
		lk = &sync.Mutex{}
		d.rootfsLocks[workDir] = lk
	}
	return lk
}

// ensureRootfs keeps a valid rootfs of layerDirs in workDir, and creates it
// if it is missing or broken.
func (d *Driver) ensureRootfs(workDir string, layerDirs []string) (string, error) {
	if len(layerDirs) == 0 {
		return "", errors.New("no layers")
	}
	lk := d.rootfsLock(workDir)
	lk.Lock()
	defer lk.Unlock()

	rootfs := filepath.Join(workDir, rootfsDirName)
	lowerDirs := make([]string, 0, len(layerDirs))
	for _, l := range slices.Backward(layerDirs) {
		lowerDirs = append(lowerDirs, filepath.Join(l, "fs"))
	}

	if len(lowerDirs) == 1 {
		if target, err := os.Readlink(rootfs); err == nil && target == lowerDirs[0] {
			if _, err := os.Stat(rootfs); err == nil {
				return rootfs, nil
			}
		}
		if err := d.removeRootfsLocked(rootfs); err != nil {
			return "", err
		}
		if err := os.Symlink(lowerDirs[0], rootfs); err != nil {
			return "", fmt.Errorf("linking rootfs: %w", err)
		}
		return rootfs, nil
	}

	if fi, err := os.Lstat(rootfs); err == nil && fi.IsDir() {
		mounted, err := d.rootfsMounter.IsMounted(rootfs)
		if err == nil && mounted {
			if _, err := os.ReadDir(rootfs); err == nil {
				return rootfs, nil
			}
		}
	}
	if err := d.removeRootfsLocked(rootfs); err != nil {
		return "", err
	}
	if err := os.Mkdir(rootfs, 0o755); err != nil {
		return "", fmt.Errorf("creating rootfs mount point: %w", err)
	}
	if err := d.rootfsMounter.Mount(rootfs, lowerDirs); err != nil {
		_ = os.Remove(rootfs)
		return "", fmt.Errorf("mounting rootfs at %s: %w", rootfs, err)
	}
	return rootfs, nil
}

// removeRootfs unmounts and removes the rootfs in workDir, if any.
func (d *Driver) removeRootfs(workDir string) error {
	lk := d.rootfsLock(workDir)
	lk.Lock()
	defer lk.Unlock()
	return d.removeRootfsLocked(filepath.Join(workDir, rootfsDirName))
}

func (d *Driver) removeRootfsLocked(rootfs string) error {
	fi, err := os.Lstat(rootfs)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("checking rootfs: %w", err)
	}
	if !fi.IsDir() {
		if err := os.Remove(rootfs); err != nil {
			return fmt.Errorf("removing rootfs link: %w", err)
		}
		return nil
	}
	// A mount point can hold more than one stacked mount.
	for i := 0; ; i++ {
		mounted, err := d.rootfsMounter.IsMounted(rootfs)
		if err == nil && !mounted {
			break
		}
		if i == maxRootfsUnmounts {
			return fmt.Errorf("rootfs %s still mounted after %d unmounts: %w", rootfs, i, err)
		}
		// A broken mount fails IsMounted. Unmount it all the same.
		if uerr := d.rootfsMounter.Unmount(rootfs); uerr != nil {
			return fmt.Errorf("unmounting rootfs %s: %w", rootfs, errors.Join(err, uerr))
		}
	}
	if err := os.Remove(rootfs); err != nil {
		return fmt.Errorf("removing rootfs mount point: %w", err)
	}
	return nil
}
