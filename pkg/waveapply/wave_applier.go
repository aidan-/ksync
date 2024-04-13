package waveapply

import (
	"context"
	"fmt"

	"k8s.io/klog/v2"
	"sigs.k8s.io/cli-utils/pkg/apply"
	"sigs.k8s.io/cli-utils/pkg/apply/event"
	"sigs.k8s.io/cli-utils/pkg/common"
	"sigs.k8s.io/cli-utils/pkg/inventory"
	"sigs.k8s.io/cli-utils/pkg/object"

	"github.com/aidan-/ksync/pkg/syncwave"
)

// WaveApplier wraps a common Applier with the ability to apply resources in waves.
type WaveApplier struct {
}

// NewWaveApplier returns a new WaveApplier.
func NewWaveApplier() *WaveApplier {
	return &WaveApplier{
		// Defaults, if any, go here.
	}
}

func (w *WaveApplier) Run(
	ctx context.Context,
	invInfo inventory.Info,
	objects object.UnstructuredSet,
	applier *apply.Applier,
	options apply.ApplierOptions,
) <-chan event.Event {
	klog.V(4).Infof("sync run for %d objects", len(objects))

	waves, grouped, err := syncwave.GroupUnstructureds(objects)
	if err != nil {
		// TODO: handle error properly.
	}

	klog.V(4).Infof("identified %d sync waves: %v", len(waves), waves)

	// Create a channel to receive events from the individual waves.
	eventChannel := make(chan event.Event)
	go func() {
		defer close(eventChannel)
		for _, wave := range waves {
			klog.V(4).Infof("applying sync wave %d", wave)
			waveObjects := grouped[wave]

			// Create a new inventory object for the wave based on the original inventory.
			// TODO: Need to think this through - moving an object between waves with
			// this approach could result in temporary deletion of the object.
			waveInvInfo, err := waveInvInfo(invInfo, wave)
			if err != nil {
				// TODO: Handle error.
			}

			// Apply the objects in the wave.
			ch := applier.Run(ctx, waveInvInfo, waveObjects, options)
			for e := range ch {
				// TODO: Need to actually process the events before forwarding them
				// to the event channel.
				// We want to emit wave events but the strict structure o the event.Event
				// struct means we probably need to wrap it in a new event type.
				eventChannel <- e
			}
		}
	}()
	return eventChannel
}

// waveInvInfo creates a new inventory object for the given wave based on the
// original inventory object.
// The wave number is added to the inventory data as a suffix to the name and
// the inventory label.
func waveInvInfo(invInfo inventory.Info, wave int) (inventory.Info, error) {
	cm := inventory.InvInfoToConfigMap(invInfo)
	if cm == nil {
		return nil, fmt.Errorf("failed to convert inventory info to config map")
	}
	cmWave := cm.DeepCopy()

	// Add the wave key to the inventory data.
	waveKeySuffix := fmt.Sprintf("-wave%d", wave)
	cmWave.SetName(cm.GetName() + waveKeySuffix)
	labels := cmWave.GetLabels()
	if _, ok := labels[common.InventoryLabel]; !ok {
		return nil, fmt.Errorf("missing inventory label on inventory object")
	}

	labels[common.InventoryLabel] = labels[common.InventoryLabel] + waveKeySuffix
	cmWave.SetLabels(labels)

	return inventory.WrapInventoryInfoObj(cmWave), nil
}
