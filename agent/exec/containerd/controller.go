package containerd

import (
	"github.com/docker/containerd/api/types/container"

	"github.com/docker/swarmkit/agent/exec"
	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/log"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/boltdb/bolt"

	"google.golang.org/grpc"
)

type controller struct {
	task    *api.Task
	adapter *containerAdapter
	closed  chan struct{}
	err     error

	pulled     chan struct{} // closed after pull
	cancelPull func()        // cancels pull context if not nil
	pullErr    error         // pull error, protected by close of pulled
}

var _ exec.Controller = &controller{}

func newController(conn *grpc.ClientConn, containerDir string, imageDescDB *bolt.DB, task *api.Task, secrets exec.SecretGetter) (exec.Controller, error) {
	adapter, err := newContainerAdapter(conn, containerDir, imageDescDB, task, secrets)
	if err != nil {
		return nil, err
	}

	return &controller{
		task:    task,
		adapter: adapter,
		closed:  make(chan struct{}),
	}, nil
}

// Update tasks a recent task update and applies it to the container.
func (r *controller) Update(ctx context.Context, t *api.Task) error {
	log.G(ctx).Warnf("task updates not yet supported")
	// TODO(stevvooe): While assignment of tasks is idempotent, we do allow
	// updates of metadata, such as labelling, as well as any other properties
	// that make sense.
	return nil
}

// Prepare creates a container and ensures the image is pulled.
//
// If the container has already be created, exec.ErrTaskPrepared is returned.
// XXX currently does none of this
func (r *controller) Prepare(ctx context.Context) error {
	if err := r.checkClosed(); err != nil {
		return err
	}

	container := r.task.Spec.GetContainer()
	log.G(ctx).Debugf("Networks: %v", r.task.Networks)
	//// Make sure all the networks that the task needs are created.
	//if err := r.adapter.createNetworks(ctx); err != nil {
	//	return err
	//}

	log.G(ctx).Debugf("Mounts: %v", container.Mounts)
	//// Make sure all the volumes that the task needs are created.
	//if err := r.adapter.createVolumes(ctx); err != nil {
	//	return err
	//}

	if r.pulled == nil {
		// Launches a re-entrant pull operation associated with controller,
		// dissociating the context from the caller's context. Allows pull
		// operation to be re-entrant on calls to prepare, resuming from the
		// same point after cancellation.
		var pctx context.Context

		r.pulled = make(chan struct{})
		pctx, r.cancelPull = context.WithCancel(context.Background()) // TODO(stevvooe): Bind a context to the entire controller.

		go func() {
			defer close(r.pulled)
			r.pullErr = r.adapter.pullImage(pctx)
		}()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.pulled:
		if r.pullErr != nil {
			// NOTE(stevvooe): We always try to pull the image to make sure we have
			// the most up to date version. This will return an error, but we only
			// log it. If the image truly doesn't exist, the create below will
			// error out.
			//
			// This gives us some nice behavior where we use up to date versions of
			// mutable tags, but will still run if the old image is available but a
			// registry is down.
			//
			// If you don't want this behavior, lock down your image to an
			// immutable tag or digest.
			log.G(ctx).WithError(r.pullErr).Error("pulling image failed")
		}
	}

	if err := r.adapter.create(ctx); err != nil {
		if isContainerCreateNameConflict(err) {
			if _, err := r.adapter.inspect(ctx); err != nil {
				return err
			}

			// container is already created. success!
			return exec.ErrTaskPrepared
		}

		return errors.Wrap(err, "create container failed")
	}

	return nil
}

// Start the container. An error will be returned if the container is already started.
func (r *controller) Start(ctx context.Context) error {
	if err := r.checkClosed(); err != nil {
		return err
	}

	ctnr, err := r.adapter.inspect(ctx)
	if err != nil {
		return err
	}

	// Detect whether the container has *ever* been started. If so, we don't
	// issue the start.
	//
	// TODO(stevvooe): This is very racy. While reading inspect, another could
	// start the process and we could end up starting it twice.
	if ctnr.Status != container.Status_CREATED {
		return exec.ErrTaskStarted
	}

	if err := r.adapter.start(ctx); err != nil {
		return errors.Wrap(err, "starting container failed")
	}

	// no health check yet
	return nil

	//if ctnr.Config == nil || ctnr.Config.Healthcheck == nil {
	//	return nil
	//}

	//healthCmd := ctnr.Config.Healthcheck.Test

	//if len(healthCmd) == 0 {
	//	// this field should be filled, even if inherited from image
	//	// if it's empty, health check will always be at starting status
	//	// so treat it as no health check, and return directly
	//	return nil
	//}

	// health check is disabled
	//if healthCmd[0] == "NONE" {
	//	return nil
	//}

	// wait for container to be healthy
	//eventq, closed, err := r.adapter.events(ctx)
	//if err != nil {
	//	return err
	//}
	//for {
	//	select {
	//	case event := <-eventq:
	//		if !r.matchevent(event) {
	//			continue
	//		}

	//		switch event.Action {
	//		case "die": // exit on terminal events
	//			//ctnr, err := r.adapter.inspect(ctx)
	//			//if err != nil {
	//			//	return errors.Wrap(err, "die event received")
	//			//}

	//			return makeExitError(ctnr)
	//		case "destroy":
	//			// If we get here, something has gone wrong but we want to exit
	//			// and report anyways.
	//			return ErrContainerDestroyed

	//		case "health_status: unhealthy":
	//			// in this case, we stop the container and report unhealthy status
	//			// TODO(runshenzhu): double check if it can cause a dead lock issue here
	//			if err := r.Shutdown(ctx); err != nil {
	//				return errors.Wrap(err, "unhealthy container shutdown failed")
	//			}
	//			return ErrContainerUnhealthy

	//		case "health_status: healthy":
	//			return nil
	//		}
	//	case <-closed:
	//		// restart!
	//		eventq, closed, err = r.adapter.events(ctx)
	//		if err != nil {
	//			return err
	//		}
	//	case <-ctx.Done():
	//		return ctx.Err()
	//	case <-r.closed:
	//		return r.err
	//	}
	//}
}

// Wait on the container to exit.
func (r *controller) Wait(ctx context.Context) error {
	if err := r.checkClosed(); err != nil {
		return err
	}

	// check the initial state and report that.
	ctnr, err := r.adapter.inspect(ctx)
	if err != nil {
		return errors.Wrap(err, "inspecting container failed")
	}

	switch ctnr.Status {
	case container.Status_STOPPED:
		//XXX TODO return makeExitError(ctnr)
		return errors.Errorf("Exit error")
	}

	eventq, closed, err := r.adapter.events(ctx)
	if err != nil {
		return err
	}

	log.G(ctx).Infof("Wait")
	for {
		select {
		case event := <-eventq:
			log.G(ctx).Debugf("Event: %v", event)
			//if !r.matchevent(event) {
			//	continue
			//}

			switch event.Type {
			case container.Event_EXIT:
				if event.ExitStatus == 0 {
					//makeExitError()
					return errors.Errorf("Container exited with state %d", event.ExitStatus)
				}
				return nil
			case container.Event_OOM:
				return errors.Errorf("Container OOMd %d", event.ExitStatus)
			case container.Event_CREATE, container.Event_START, container.Event_EXEC_ADDED, container.Event_PAUSED:
				continue
			default:
				return errors.Errorf("Unknown event type %s\n", event.Type.String())
			}
			//case "die": // exit on terminal events
			//	ctnr, err := r.adapter.inspect(ctx)
			//	if err != nil {
			//		return errors.Wrap(err, "die event received")
			//	}

			//	return makeExitError(ctnr)
			//case "destroy":
			//	// If we get here, something has gone wrong but we want to exit
			//	// and report anyways.
			//	return ErrContainerDestroyed

			//case "health_status: unhealthy":
			//	// in this case, we stop the container and report unhealthy status
			//	// TODO(runshenzhu): double check if it can cause a dead lock issue here
			//	if err := r.Shutdown(ctx); err != nil {
			//		return errors.Wrap(err, "unhealthy container shutdown failed")
			//	}
			//	return ErrContainerUnhealthy
			//}
		case <-closed:
			// restart!
			eventq, closed, err = r.adapter.events(ctx)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-r.closed:
			return r.err
		}
	}
}

// Shutdown the container cleanly.
func (r *controller) Shutdown(ctx context.Context) error {
	if err := r.checkClosed(); err != nil {
		return err
	}

	if r.cancelPull != nil {
		r.cancelPull()
	}

	if err := r.adapter.shutdown(ctx); err != nil {
		if isUnknownContainer(err) { //|| isStoppedContainer(err) {
			return nil
		}

		return err
	}

	return nil
}

// Terminate the container, with force.
func (r *controller) Terminate(ctx context.Context) error {
	if err := r.checkClosed(); err != nil {
		return err
	}

	if r.cancelPull != nil {
		r.cancelPull()
	}

	if err := r.adapter.terminate(ctx); err != nil {
		if isUnknownContainer(err) {
			return nil
		}

		return err
	}

	return nil
}

// Remove the container and its resources.
func (r *controller) Remove(ctx context.Context) error {
	if err := r.checkClosed(); err != nil {
		return err
	}

	if r.cancelPull != nil {
		r.cancelPull()
	}

	// It may be necessary to shut down the task before removing it.
	if err := r.Shutdown(ctx); err != nil {
		if isUnknownContainer(err) {
			return nil
		}

		// This may fail if the task was already shut down.
		log.G(ctx).WithError(err).Debug("shutdown failed on removal")
	}

	// Try removing networks referenced in this task in case this
	// task is the last one referencing it
	//if err := r.adapter.removeNetworks(ctx); err != nil {
	//	if isUnknownContainer(err) {
	//		return nil
	//	}

	//	return err
	//}

	if err := r.adapter.remove(ctx); err != nil {
		if isUnknownContainer(err) {
			return nil
		}

		return err
	}

	return nil
}

// Close the controller and clean up any ephemeral resources.
func (r *controller) Close() error {
	select {
	case <-r.closed:
		return r.err
	default:
		if r.cancelPull != nil {
			r.cancelPull()
		}

		r.err = exec.ErrControllerClosed
		close(r.closed)
	}
	return nil
}

func (r *controller) checkClosed() error {
	select {
	case <-r.closed:
		return r.err
	default:
		return nil
	}
}
