package waveapply

import (
	"context"
	"fmt"
	"sort"

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

	// Group the objects by wave number.
	grouped, err := groupWaves(objects)
	if err != nil {
		// TODO: Handle error.
	}

	// Create ordered slice of wave numbers.
	waves := make([]int, 0, len(grouped))
	for wave := range grouped {
		waves = append(waves, wave)
	}
	sort.Ints(waves)

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
				eventChannel <- e
			}
		}
	}()
	return eventChannel
}

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

func groupWaves(objects object.UnstructuredSet) (map[int]object.UnstructuredSet, error) {
	grouped := make(map[int]object.UnstructuredSet)
	for _, obj := range objects {
		wave, err := syncwave.ReadAnnotation(obj)
		if err != nil {
			return nil, err
		}
		grouped[wave] = append(grouped[wave], obj)
	}

	return grouped, nil
}
