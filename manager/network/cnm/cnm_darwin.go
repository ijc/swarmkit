package cnm

import (
	"github.com/docker/swarmkit/manager/allocator/networkallocator"
)

// PredefinedNetworks returns the list of predefined network structures
func (nm *cnm) PredefinedNetworks() []networkallocator.PredefinedNetworkData {
	return nil
}
