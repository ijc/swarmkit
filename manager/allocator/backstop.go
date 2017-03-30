// Acts as a backstop in the case there are no other active allocators.
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
	// Backstop allocator Voter ID for task allocation vote.
	backstopVoter = "backstop"
)

func (a *Allocator) doBackstopInit(ctx context.Context) (err error) {
	// Allocate tasks in the store so far before we started watching.
	var (
		tasks          []*api.Task
		allocatedTasks []*api.Task
	)
	a.store.View(func(tx store.ReadTx) {
		tasks, err = store.FindTasks(tx, store.All)
	})
	if err != nil {
		return errors.Wrap(err, "error listing all tasks in store while trying to allocate during backstop init")
	}

	for _, t := range tasks {
		if t.Status.State >= api.TaskStatePending {
			continue
		}

		if a.taskAllocateVote(backstopVoter, t.ID) {
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
		log.G(ctx).WithError(err).Error("failed committing allocation of tasks during backstop init")
	}

	return nil
}

func (a *Allocator) doBackstopAlloc(ctx context.Context, ev events.Event) {
	switch v := ev.(type) {
	case api.EventCreateTask:
		var t *api.Task

		a.store.View(func(tx store.ReadTx) {
			t = store.GetTask(tx, v.Task.ID)
		})

		if a.taskAllocateVote(backstopVoter, t.ID) {
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
			log.G(ctx).WithError(err).Error("failed committing allocation of tasks during backstop init")
		}

	}
}
