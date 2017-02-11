package containerd

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/Sirupsen/logrus"

	contentapi "github.com/docker/containerd/api/services/content"
	"github.com/docker/containerd/api/services/execution"
	"github.com/docker/containerd/api/types/container"
	"github.com/docker/containerd/api/types/mount"

	"github.com/docker/containerd/archive"
	"github.com/docker/containerd/archive/compression"
	"github.com/docker/containerd/images"
	"github.com/docker/containerd/remotes"
	"github.com/docker/containerd/remotes/docker"
	contentservice "github.com/docker/containerd/services/content"
	dockermount "github.com/docker/docker/pkg/mount"

	"github.com/docker/swarmkit/agent/exec"
	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/api/naming"
	"github.com/docker/swarmkit/log"

	"github.com/boltdb/bolt"

	protobuf "github.com/gogo/protobuf/types"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc"

	"github.com/pkg/errors"
	"github.com/tonistiigi/fifo"
	"golang.org/x/net/context"
)

var (
	mountPropagationReverseMap = map[api.Mount_BindOptions_MountPropagation]string{
		api.MountPropagationPrivate:  "private",
		api.MountPropagationRPrivate: "rprivate",
		api.MountPropagationShared:   "shared",
		api.MountPropagationRShared:  "rshared",
		api.MountPropagationRSlave:   "slave",
		api.MountPropagationSlave:    "rslave",
	}
)

// containerAdapter conducts remote operations for a container. All calls
// are mostly naked calls to the client API, seeded with information from
// containerConfig.
type containerAdapter struct {
	conn              *grpc.ClientConn
	client            execution.ContainerServiceClient
	container         *api.ContainerSpec
	task              *api.Task
	secrets           exec.SecretGetter
	dir               string
	resolvedImageName string
	imageDescDB       *bolt.DB
}

func newContainerAdapter(conn *grpc.ClientConn, containerDir string, imageDescDB *bolt.DB, task *api.Task, secrets exec.SecretGetter) (*containerAdapter, error) {
	container := task.Spec.GetContainer()
	if container == nil {
		return nil, exec.ErrRuntimeUnsupported
	}

	dir := filepath.Join(containerDir, task.ID)

	return &containerAdapter{
		conn:        conn,
		client:      execution.NewContainerServiceClient(conn),
		container:   container,
		task:        task,
		secrets:     secrets,
		dir:         dir,
		imageDescDB: imageDescDB,
	}, nil
}

func (c *containerAdapter) blobReader(ctx context.Context, d digest.Digest) (io.ReadCloser, error) {
	cc := contentapi.NewContentClient(c.conn)

	p := contentservice.NewProviderFromClient(cc)

	return p.Reader(ctx, d)
}

func (c *containerAdapter) blobReadAll(ctx context.Context, d digest.Digest) ([]byte, error) {
	br, err := c.blobReader(ctx, d)
	if err != nil {
		return nil, err
	}
	defer br.Close()
	return ioutil.ReadAll(br)
}

func (c *containerAdapter) applyLayer(ctx context.Context, rootfs string, layer digest.Digest) error {
	blob, err := c.blobReader(ctx, layer)
	if err != nil {
		return err
	}

	rd, err := compression.DecompressStream(blob)
	if err != nil {
		return err
	}

	_, err = archive.Apply(ctx, rootfs, rd)

	blob.Close()
	return err
}

// github.com/docker/containerd cmd/ctr/utils.go
func prepareStdio(stdin, stdout, stderr string, console bool) (*sync.WaitGroup, error) {
	var wg sync.WaitGroup
	ctx := context.Background()

	f, err := fifo.OpenFifo(ctx, stdin, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_NONBLOCK, 0700)
	if err != nil {
		return nil, err
	}
	defer func(c io.Closer) {
		if err != nil {
			c.Close()
		}
	}(f)
	go func(w io.WriteCloser) {
		io.Copy(w, os.Stdin)
		w.Close()
	}(f)

	f, err = fifo.OpenFifo(ctx, stdout, syscall.O_RDONLY|syscall.O_CREAT|syscall.O_NONBLOCK, 0700)
	if err != nil {
		return nil, err
	}
	defer func(c io.Closer) {
		if err != nil {
			c.Close()
		}
	}(f)
	wg.Add(1)
	go func(r io.ReadCloser) {
		io.Copy(os.Stdout, r)
		r.Close()
		wg.Done()
	}(f)

	f, err = fifo.OpenFifo(ctx, stderr, syscall.O_RDONLY|syscall.O_CREAT|syscall.O_NONBLOCK, 0700)
	if err != nil {
		return nil, err
	}
	defer func(c io.Closer) {
		if err != nil {
			c.Close()
		}
	}(f)
	if !console {
		wg.Add(1)
		go func(r io.ReadCloser) {
			io.Copy(os.Stderr, r)
			r.Close()
			wg.Done()
		}(f)
	}

	return &wg, nil
}

func (c *containerAdapter) pullImage(ctx context.Context) error {
	resolver := docker.NewResolver()

	name, desc, fetcher, err := resolver.Resolve(ctx, c.container.Image)
	if err != nil {
		return errors.Wrap(err, "Failed to resolve ref")
	}

	tx, err := c.imageDescDB.Begin(true)
	if err != nil {
		return errors.Wrap(err, "Begin Tx")
	}
	defer tx.Rollback()

	log.G(ctx).Infof("Canonical name %s", name)
	log.G(ctx).Infof("Desc %+v", desc)
	if err := images.Register(tx, name, desc); err != nil {
		log.G(ctx).Errorf("DB: %s", err)
		return errors.Wrap(err, "Register image")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "Commit Tx")
	}

	c.resolvedImageName = name

	ingester := contentservice.NewIngesterFromClient(contentapi.NewContentClient(c.conn))
	provider := contentservice.NewProviderFromClient(contentapi.NewContentClient(c.conn))

	return images.Dispatch(ctx,
		images.Handlers(
			remotes.FetchHandler(ingester, fetcher),
			images.ChildrenHandler(provider)),
		desc)
}

func (c *containerAdapter) makeAnonVolume(ctx context.Context, target string) (specs.Mount, error) {
	source := filepath.Join(c.dir, "anon-volumes", target)
	if err := os.MkdirAll(source, 0755); err != nil {
		return specs.Mount{}, err
	}

	return specs.Mount{
		Destination: target,
		Type:        "bind",
		Source:      source,
		Options:     []string{"rbind", "rprivate", "rw"},
	}, nil
}

// Somewhat like docker/docker/daemon/oci_linux.go:setMounts
func (c *containerAdapter) setMounts(ctx context.Context, s *specs.Spec, mounts []api.Mount, volumes map[string]struct{}) error {

	userMounts := make(map[string]struct{})
	for _, m := range mounts {
		userMounts[m.Target] = struct{}{}
	}

	// Filter out mounts that are overridden by user supplied mounts
	var defaultMounts []specs.Mount
	_, mountDev := userMounts["/dev"]
	for _, m := range s.Mounts {
		if _, ok := userMounts[m.Destination]; !ok {
			if mountDev && strings.HasPrefix(m.Destination, "/dev/") {
				continue
			}
			defaultMounts = append(defaultMounts, m)
		}
	}

	s.Mounts = defaultMounts
	for _, m := range mounts {
		if !filepath.IsAbs(m.Target) {
			return errors.Errorf("Mount %s is not absolute", m.Target)
		}

		for _, cm := range s.Mounts {
			if cm.Destination == m.Target {
				return errors.Errorf("Duplicate mount point '%s'", m.Target)
			}
		}

		delete(volumes, m.Target) // volume is no longer anon

		switch m.Type {
		case api.MountTypeTmpfs:
			opts := []string{"noexec", "nosuid", "nodev", "rprivate"}
			if m.TmpfsOptions != nil {
				if m.TmpfsOptions.SizeBytes <= 0 {
					return errors.New("Invalid tmpfs size give")
				}
				opts = append(opts, fmt.Sprintf("size=%d", m.TmpfsOptions.SizeBytes))
				opts = append(opts, fmt.Sprintf("mode=%o", m.TmpfsOptions.Mode))
			}
			if m.ReadOnly {
				opts = append(opts, "ro")
			} else {
				opts = append(opts, "rw")
			}

			opts, err := dockermount.MergeTmpfsOptions(opts)
			if err != nil {
				return err
			}

			s.Mounts = append(s.Mounts, specs.Mount{
				Destination: m.Target,
				Type:        "tmpfs",
				Source:      "tmpfs",
				Options:     opts,
			})

		case api.MountTypeVolume:
			if m.Source != "" {
				return errors.Errorf("Non-anon volume mounts not implemented, ignoring %v", m)
			}
			if m.VolumeOptions != nil {
				return errors.Errorf("VolumeOptions not implemented, ignoring %v", m)
			}

			mt, err := c.makeAnonVolume(ctx, m.Target)
			if err != nil {
				return err
			}

			s.Mounts = append(s.Mounts, mt)
			continue

		case api.MountTypeBind:
			opts := []string{"rbind"}
			if m.ReadOnly {
				opts = append(opts, "ro")
			} else {
				opts = append(opts, "rw")
			}

			// XXX should be from m.BindOptions.Propagation
			propagation := "rprivate"
			if m.BindOptions != nil {
				if p, ok := mountPropagationReverseMap[m.BindOptions.Propagation]; ok {
					propagation = p
				} else {
					log.G(ctx).Warningf("unknown bind mount propagation,  using %q", propagation)
				}
			}
			opts = append(opts, propagation)

			mt := specs.Mount{
				Destination: m.Target,
				Type:        "bind",
				Source:      m.Source,
				Options:     opts,
			}

			s.Mounts = append(s.Mounts, mt)
			continue
		}
	}

	for v := range volumes {
		mt, err := c.makeAnonVolume(ctx, v)
		if err != nil {
			return err
		}

		s.Mounts = append(s.Mounts, mt)
	}
	return nil
}

func (c *containerAdapter) spec(ctx context.Context, config *ocispec.ImageConfig, rootfs string) (*specs.Spec, error) {
	caps := []string{
		"CAP_CHOWN",
		"CAP_DAC_OVERRIDE",
		"CAP_FSETID",
		"CAP_FOWNER",
		"CAP_MKNOD",
		"CAP_NET_RAW",
		"CAP_SETGID",
		"CAP_SETUID",
		"CAP_SETFCAP",
		"CAP_SETPCAP",
		"CAP_NET_BIND_SERVICE",
		"CAP_SYS_CHROOT",
		"CAP_KILL",
		"CAP_AUDIT_WRITE",
	}

	// Need github.com/docker/docker/oci.DefaultSpec()
	spec := specs.Spec{
		Version: "1.0.0-rc2-dev",
		Root: specs.Root{
			Path: rootfs,
		},
		Mounts: []specs.Mount{
			{
				Destination: "/proc",
				Type:        "proc",
				Source:      "proc",
				Options:     []string{"nosuid", "noexec", "nodev"},
			},
			{
				Destination: "/dev",
				Type:        "tmpfs",
				Source:      "tmpfs",
				Options:     []string{"nosuid", "strictatime", "mode=755"},
			},
			{
				Destination: "/dev/pts",
				Type:        "devpts",
				Source:      "devpts",
				Options:     []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"},
			},
			{
				Destination: "/sys",
				Type:        "sysfs",
				Source:      "sysfs",
				Options:     []string{"nosuid", "noexec", "nodev", "ro"},
			},
			{
				Destination: "/sys/fs/cgroup",
				Type:        "cgroup",
				Source:      "cgroup",
				Options:     []string{"ro", "nosuid", "noexec", "nodev"},
			},
			{
				Destination: "/dev/mqueue",
				Type:        "mqueue",
				Source:      "mqueue",
				Options:     []string{"nosuid", "noexec", "nodev"},
			},
		},
		Process: specs.Process{
			Cwd: "/",
			Capabilities: &specs.LinuxCapabilities{
				Bounding:    caps,
				Effective:   caps,
				Inheritable: caps,
				Permitted:   caps,
				Ambient:     caps,
			},
		},
		Linux: &specs.Linux{
			Namespaces: []specs.LinuxNamespace{
				{Type: "mount"},
				{Type: "network"},
				{Type: "uts"},
				{Type: "pid"},
				{Type: "ipc"},
			},
		},
	}

	spec.Platform = specs.Platform{
		OS:   runtime.GOOS,
		Arch: runtime.GOARCH,
	}
	if config.WorkingDir != "" {
		spec.Process.Cwd = config.WorkingDir
	}
	spec.Process.Env = config.Env

	if len(c.container.Args) > 0 {
		spec.Process.Args = append(config.Entrypoint, c.container.Args...)
	} else {
		spec.Process.Args = append(config.Entrypoint, config.Cmd...)
	}

	if err := c.setMounts(ctx, &spec, c.container.Mounts, config.Volumes); err != nil {
		return nil, errors.Wrap(err, "Failed to set mounts")
	}
	sort.Sort(mounts(spec.Mounts))

	log.G(ctx).Debug("Mounts are:")
	for i, m := range spec.Mounts {
		log.G(ctx).Debugf(" %2d: %+v", i, m)
	}

	return &spec, nil
}

func (c *containerAdapter) create(ctx context.Context) error {
	if c.resolvedImageName == "" {
		return errors.New("Image has not been pulled")
	}

	tx, err := c.imageDescDB.Begin(true)
	if err != nil {
		return errors.Wrap(err, "Tx begin")
	}
	defer tx.Rollback()

	image, err := images.Get(tx, c.resolvedImageName)
	if err != nil {
		return errors.Wrap(err, "Image Get")
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "Commit Tx")
	}

	mbytes, err := c.blobReadAll(ctx, image.Descriptor.Digest)
	if err != nil {
		return err
	}

	rootfs := filepath.Join(c.dir, "rootfs")
	stdin := filepath.Join(c.dir, "stdin")
	stdout := filepath.Join(c.dir, "stdout")
	stderr := filepath.Join(c.dir, "stderr")

	if err := os.MkdirAll(rootfs, 0755); err != nil {
		return err
	}

	var config ocispec.Image

	var manifest ocispec.Manifest
	if err := json.Unmarshal(mbytes, &manifest); err != nil {
		return err
	}

	bytes, err := c.blobReadAll(ctx, manifest.Config.Digest)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(bytes, &config); err != nil {
		return err
	}

	for _, layer := range manifest.Layers {
		if err := c.applyLayer(ctx, rootfs, layer.Digest); err != nil {
			return errors.Wrapf(err, "Failed to apply layer %s", layer.Digest.String())
		}
	}

	spec, err := c.spec(ctx, &config.Config, rootfs)
	if err != nil {
		return err
	}

	console := false
	_, err = prepareStdio(stdin, stdout, stderr, console)
	if err != nil {
		return err
	}

	//data, err := json.Marshal(spec)
	data, err := json.MarshalIndent(spec, "    ", "    ")
	if err != nil {
		return err
	}

	_, err = c.client.Create(ctx, &execution.CreateRequest{
		ID: naming.Task(c.task),
		Spec: &protobuf.Any{
			TypeUrl: specs.Version,
			Value:   data,
		},
		Rootfs:   []*mount.Mount{},
		Runtime:  "linux",
		Stdin:    stdin,
		Stdout:   stdout,
		Stderr:   stderr,
		Terminal: console,
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *containerAdapter) start(ctx context.Context) error {
	_, err := c.client.Start(ctx, &execution.StartRequest{
		ID: naming.Task(c.task),
	})
	return err
}

func (c *containerAdapter) eventStream(ctx context.Context, id string) (<-chan container.Event, <-chan error, error) {

	var (
		evtch = make(chan container.Event)
		errch = make(chan error)
	)

	return evtch, errch, nil
}

// events issues a call to the events API and returns a channel with all
// events. The stream of events can be shutdown by cancelling the context.
//
// A chan struct{} is returned that will be closed if the event processing
// fails and needs to be restarted.
func (c *containerAdapter) events(ctx context.Context) (<-chan container.Event, <-chan struct{}, error) {
	id := naming.Task(c.task)

	l := log.G(ctx).WithFields(logrus.Fields{
		"ID": id,
	})

	// TODO(stevvooe): Move this to a single, global event dispatch. For
	// now, we create a connection per container.
	var (
		eventsq = make(chan container.Event)
		closed  = make(chan struct{})
	)

	l.Debugf("waiting on events")
	// TODO(stevvooe): For long running tasks, it is likely that we will have
	// to restart this under failure.
	cl, err := c.client.Events(ctx, &execution.EventsRequest{})
	if err != nil {
		return nil, nil, err
	}

	go func() {
		defer close(closed)

		for {
			l.Debugf("calling Recv")
			evt, err := cl.Recv()
			l.Debugf("Recv returned")
			if err != nil {
				l.WithError(err).Error("error from events stream")
				return
			}
			l.Debugf("Event: %v", evt)
			if evt.ID != id {
				continue
			}

			select {
			case eventsq <- *evt:
			case <-ctx.Done():
				return
			}
		}
	}()

	return eventsq, closed, nil
}

func (c *containerAdapter) inspect(ctx context.Context) (container.Container, error) {
	id := naming.Task(c.task)
	rsp, err := c.client.Info(ctx, &execution.InfoRequest{ID: id})
	if err != nil {
		return container.Container{}, err
	}
	return *rsp, nil
}

func (c *containerAdapter) shutdown(ctx context.Context) error {
	id := naming.Task(c.task)
	l := log.G(ctx).WithFields(logrus.Fields{
		"ID": id,
	})
	l.Debug("Shutdown")
	/*rsp*/ _, err := c.client.Delete(ctx, &execution.DeleteRequest{ID: id})
	if err != nil {
		return err
	}

	//l.Debugf("Status=%d", rsp.ExitStatus)
	return nil
}

func (c *containerAdapter) terminate(ctx context.Context) error {
	id := naming.Task(c.task)
	l := log.G(ctx).WithFields(logrus.Fields{
		"ID": id,
	})
	l.Debug("Terminate")
	return errors.New("terminate not implemented")
}

func (c *containerAdapter) remove(ctx context.Context) error {
	id := naming.Task(c.task)
	l := log.G(ctx).WithFields(logrus.Fields{
		"ID": id,
	})
	l.Debug("Remove")
	return os.RemoveAll(c.dir)
}

func isContainerCreateNameConflict(err error) bool {
	// container ".*" already exists
	splits := strings.SplitN(err.Error(), "\"", 3)
	return splits[0] == "container " && splits[2] == " already exists"
}

func isUnknownContainer(err error) bool {
	return strings.Contains(err.Error(), "container does not exist")
}

// For sort.Sort
type mounts []specs.Mount

// Len returns the number of mounts. Used in sorting.
func (m mounts) Len() int {
	return len(m)
}

// Less returns true if the number of parts (a/b/c would be 3 parts) in the
// mount indexed by parameter 1 is less than that of the mount indexed by
// parameter 2. Used in sorting.
func (m mounts) Less(i, j int) bool {
	return m.parts(i) < m.parts(j)
}

// Swap swaps two items in an array of mounts. Used in sorting
func (m mounts) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

// parts returns the number of parts in the destination of a mount. Used in sorting.
func (m mounts) parts(i int) int {
	return strings.Count(filepath.Clean(m[i].Destination), string(os.PathSeparator))
}
