package containerd

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/pkg/errors"

	"github.com/docker/swarmkit/agent/exec"
	"github.com/docker/swarmkit/agent/secrets"
	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/log"

	"github.com/docker/containerd/images"

	"golang.org/x/net/context"

	"github.com/boltdb/bolt"

	"google.golang.org/grpc"
	//"google.golang.org/grpc/grpclog"
)

type executor struct {
	conn         *grpc.ClientConn
	secrets      exec.SecretsManager
	containerDir string
	imageDescDB  *bolt.DB
}

var _ exec.Executor = &executor{}

// Lifted from github.com/docker/containerd/cmd/ctr/utils.go
// XXX library?
func getGRPCConnection(bindSocket string) (*grpc.ClientConn, error) {
	// XXX original code used a global grpcConn as a cache. Should we?

	// reset the logger for grpc to log to dev/null so that it does not mess with our stdio
	dialOpts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithTimeout(100 * time.Second)}
	dialOpts = append(dialOpts,
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", bindSocket, timeout)
		},
		))

	conn, err := grpc.Dial("unix://"+bindSocket, dialOpts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial %q", bindSocket)
	}

	return conn, nil
}

// NewExecutor returns an executor using the given containerd control socket
func NewExecutor(sock, stateDir string) exec.Executor {
	conn, err := getGRPCConnection(sock)
	if err != nil {
		panic(err)
	}

	containerDir := filepath.Join(stateDir, "containers")
	imageDescDB, err := bolt.Open(filepath.Join(stateDir, "images.db"), 0644, nil)
	if err != nil {
		panic(err)
	}
	if err := images.InitDB(imageDescDB); err != nil {
		panic(err)
	}

	return &executor{
		conn:         conn,
		secrets:      secrets.NewManager(),
		containerDir: containerDir,
		imageDescDB:  imageDescDB,
	}
}

// github.com/docker/docker/pkg/platform/architecture_linux.go
// runtimeArchitecture gets the name of the current architecture (x86, x86_64, …)
//func runtimeArchitecture() (string, error) {
//	utsname := &syscall.Utsname{}
//	if err := syscall.Uname(utsname); err != nil {
//		return "", err
//	}
//	return charsToString(utsname.Machine), nil
//}

// Describe returns the underlying node description from containerd
func (e *executor) Describe(ctx context.Context) (*api.NodeDescription, error) {
	hostname := ""
	if hn, err := os.Hostname(); err != nil {
		log.G(ctx).Warnf("Could not get hostname: %v", err)
	} else {
		hostname = hn
	}

	//arch := ""
	//if ra, err := runtimeArchitecture(); err != nil {
	//	log.G(ctx).Warnf("Could not get architecture: %v", err)
	//} else {
	//	arch = ra
	//}
	description := &api.NodeDescription{
		Hostname: hostname,
		Platform: &api.Platform{
			Architecture: runtime.GOARCH, //arch,
			OS:           runtime.GOOS,
		},
		//		Resources,
		// Not clear what makes sense here, but at least some code
		// expects desc.Engine.Plugins to be non-nil
		// 	nodePlugins := n.Description.Engine.Plugins
		// at the entry of `func (f *PluginFilter) Check(n *NodeInfo) bool`
		// in `manager/scheduler/filter.go`
		Engine: &api.EngineDescription{
			//EngineVersion: info.ServerVersion,
			Labels:  map[string]string{},
			Plugins: []api.PluginDescription{},
		},
	}

	return description, nil
}

func (e *executor) Configure(ctx context.Context, node *api.Node) error {
	return nil
}

// Controller returns a docker container controller.
func (e *executor) Controller(t *api.Task) (exec.Controller, error) {
	ctlr, err := newController(e.conn, e.containerDir, e.imageDescDB, t, secrets.Restrict(e.secrets, t))
	if err != nil {
		return nil, err
	}

	return ctlr, nil
}

func (e *executor) SetNetworkBootstrapKeys([]*api.EncryptionKey) error {
	return nil
}

func (e *executor) Secrets() exec.SecretsManager {
	return e.secrets
}
