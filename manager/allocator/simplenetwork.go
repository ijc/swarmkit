// Simple network attachment handling for the non CNM case
package allocator

import (
	"github.com/docker/go-events"
	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/log"
	"github.com/docker/swarmkit/manager/state/store"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

const (
	// SimpleNetwork allocator Voter ID for task allocation vote.
	simpleNetworkVoter = "simplenetwork"
)

func (a *Allocator) doSimpleNetworkInit(ctx context.Context) (err error) {
	// Allocate tasks in the store so far before we started watching.
	var (
		tasks          []*api.Task
		allocatedTasks []*api.Task
	)
	a.store.View(func(tx store.ReadTx) {
		tasks, err = store.FindTasks(tx, store.All)
	})
	if err != nil {
		return errors.Wrap(err, "error listing all tasks in store while trying to allocate during simpleNetwork init")
	}

	for _, t := range tasks {
		if t.Status.State >= api.TaskStatePending {
			continue
		}
		if len(t.Networks) > 0 {
			continue
		}

		var s *api.Service
		if t.ServiceID != "" {
			a.store.View(func(tx store.ReadTx) {
				s = store.GetService(tx, t.ServiceID)
			})
			if s == nil {
				// If the task is running it is not normal to
				// not be able to find the associated
				// service. If the task is not running (task
				// is either dead or the desired state is set
				// to dead) then the service may not be
				// available in store. But we still need to
				// cleanup network resources associated with
				// the task.
				if t.Status.State <= api.TaskStateRunning /*&& !isDelete*/ {
					log.G(ctx).Errorf("Failed to get service %s for task %s state %s: could not find service %s", t.ServiceID, t.ID, t.Status.State, t.ServiceID)
					return
				}
			}
		}

		a.taskCreateNetworkAttachments(t, s)

		if a.taskAllocateVote(simpleNetworkVoter, t.ID) {
			updateTaskStatus(t, api.TaskStatePending, allocatedStatusMessage)
			allocatedTasks = append(allocatedTasks, t)
		}
	}

	if _, err := a.store.Batch(func(batch *store.Batch) error {
		for _, t := range allocatedTasks {
			if err := a.commitAllocatedTask(ctx, batch, t); err != nil {
				log.G(ctx).WithError(err).Errorf("failed committing allocation of task %s during init", t.ID)
			}
		}

		return nil
	}); err != nil {
		log.G(ctx).WithError(err).Error("failed committing allocation of tasks during simpleNetwork init")
	}

	return nil
}

func (a *Allocator) doSimpleNetworkAlloc(ctx context.Context, ev events.Event) {
	log.G(ctx).Infof("doSimpleNetworkAlloc: %T", ev)
	switch ev.(type) {
	case api.EventCreateTask, api.EventUpdateTask:
		var t *api.Task
		switch v := ev.(type) {
		case api.EventCreateTask:
			a.store.View(func(tx store.ReadTx) {
				t = store.GetTask(tx, v.Task.ID)
			})
		case api.EventUpdateTask:
			a.store.View(func(tx store.ReadTx) {
				t = store.GetTask(tx, v.Task.ID)
			})
		}

		if len(t.Networks) > 0 {
			return
		}

		var s *api.Service
		if t.ServiceID != "" {
			a.store.View(func(tx store.ReadTx) {
				s = store.GetService(tx, t.ServiceID)
			})
			if s == nil {
				// If the task is running it is not normal to
				// not be able to find the associated
				// service. If the task is not running (task
				// is either dead or the desired state is set
				// to dead) then the service may not be
				// available in store. But we still need to
				// cleanup network resources associated with
				// the task.
				if t.Status.State <= api.TaskStateRunning /*&& !isDelete*/ {
					log.G(ctx).Errorf("Event %T: Failed to get service %s for task %s state %s: could not find service %s", ev, t.ServiceID, t.ID, t.Status.State, t.ServiceID)
					return
				}
			}
		}

		a.taskCreateNetworkAttachments(t, s)

		if _, err := a.store.Batch(func(batch *store.Batch) error {
			if err := a.commitAllocatedTask(ctx, batch, t); err != nil {
				log.G(ctx).WithError(err).Errorf("failed committing allocation of task %s during init", t.ID)
			}

			return nil
		}); err != nil {
			log.G(ctx).WithError(err).Error("failed committing allocation of tasks during simpleNetwork init")
		}

		if a.taskAllocateVote(simpleNetworkVoter, t.ID) {
			if t.Status.State < api.TaskStatePending {
				updateTaskStatus(t, api.TaskStatePending, allocatedStatusMessage)
			}
		}

		// Should this batch until state.EventCommit?
		if _, err := a.store.Batch(func(batch *store.Batch) error {
			//for _, t := range allocatedTasks {
			if err := a.commitAllocatedTask(ctx, batch, t); err != nil {
				log.G(ctx).WithError(err).Errorf("failed committing allocation of task %s during init", t.ID)
			}
			//}

			return nil
		}); err != nil {
			log.G(ctx).WithError(err).Error("failed committing allocation of tasks during simpleNetwork init")
		}

	}
}
