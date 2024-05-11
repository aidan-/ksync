// Copyright 2019 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package apply

import (
	"context"
	"fmt"
	"time"

	"github.com/aidan-/ksync/pkg/syncwave"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cli-utils/pkg/apis/actuation"
	"sigs.k8s.io/cli-utils/pkg/apply/cache"
	"sigs.k8s.io/cli-utils/pkg/apply/event"
	"sigs.k8s.io/cli-utils/pkg/apply/filter"
	"sigs.k8s.io/cli-utils/pkg/apply/info"
	"sigs.k8s.io/cli-utils/pkg/apply/mutator"
	"sigs.k8s.io/cli-utils/pkg/apply/prune"
	"sigs.k8s.io/cli-utils/pkg/apply/solver"
	"sigs.k8s.io/cli-utils/pkg/apply/taskrunner"
	"sigs.k8s.io/cli-utils/pkg/common"
	"sigs.k8s.io/cli-utils/pkg/inventory"
	"sigs.k8s.io/cli-utils/pkg/kstatus/watcher"
	"sigs.k8s.io/cli-utils/pkg/object"
	"sigs.k8s.io/cli-utils/pkg/object/validation"
)

// Applier performs the step of applying a set of resources into a cluster,
// conditionally waits for all of them to be fully reconciled and finally
// performs prune to clean up any resources that has been deleted.
// The applier performs its function by executing a list queue of tasks,
// each of which is one of the steps in the process of applying a set
// of resources to the cluster. The actual execution of these tasks are
// handled by a StatusRunner. So the taskqueue is effectively a
// specification that is executed by the StatusRunner. Based on input
// parameters and/or the set of resources that needs to be applied to the
// cluster, different sets of tasks might be needed.
type Applier struct {
	pruner        *prune.Pruner
	statusWatcher watcher.StatusWatcher
	invClient     inventory.Client
	client        dynamic.Interface
	openAPIGetter discovery.OpenAPISchemaInterface
	mapper        meta.RESTMapper
	infoHelper    info.Helper
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

// syncWave represents a wave of resources to apply and prune.
type syncWave struct {
	wave      int                    // Wave identifier
	inv       inventory.Info         // Wave specific inventory
	applyObjs object.UnstructuredSet // Set of objects to create or update
	pruneObjs object.UnstructuredSet // Set of objects to delete
}

// prepareSyncWaves returns a list of sync waves determined from the provided localObjs.
// A sync wave includes a generated inventory.Info formed from the provided localInv and
// the set of objects to apply and to prune or an error if one occurred.
// Objects that have been moved to a different sync wave sync last sync are removed from the
// prune and apply list of the original sync wave.
func (a *Applier) prepareSyncWaves(
	localInv inventory.Info,
	localObjs object.UnstructuredSet,
	o ApplierOptions,
) (map[string]int, []syncWave, error) {
	if localInv == nil {
		return nil, nil, fmt.Errorf("the local inventory can't be nil")
	}
	if err := inventory.ValidateNoInventory(localObjs); err != nil {
		return nil, nil, err
	}

	waves, grouped, err := syncwave.GroupUnstructureds(localObjs)
	if err != nil {
		return nil, nil, err
	}

	syncWaves := make([]syncWave, len(waves))
	//unstructuredToWave := make(map[string]int)

	for idx, wave := range waves {
		sw := syncWave{}
		sw.wave = wave
		var err error

		// Create inventory for the calculated sync-wave
		sw.inv, err = waveInvInfo(localInv, wave)
		if err != nil {
			return nil, nil, fmt.Errorf("error creating sync wave inventory: %s", err)
		}

		// Add the inventory annotation to the resources being applied.
		for _, localObj := range localObjs {
			inventory.AddInventoryIDAnnotation(localObj, sw.inv)
		}
		// If the inventory uses the Name strategy and an inventory ID is provided,
		// verify that the existing inventory object (if there is one) has an ID
		// label that matches.
		// TODO(seans): This inventory id validation should happen in destroy and status.
		if sw.inv.Strategy() == inventory.NameStrategy && sw.inv.ID() != "" {
			prevInvObjs, err := a.invClient.GetClusterInventoryObjs(sw.inv)
			if err != nil {
				return nil, nil, err
			}
			if len(prevInvObjs) > 1 {
				panic(fmt.Errorf("found %d inv objects with Name strategy", len(prevInvObjs)))
			}
			if len(prevInvObjs) == 1 {
				invObj := prevInvObjs[0]
				val := invObj.GetLabels()[common.InventoryLabel]
				if val != sw.inv.ID() {
					return nil, nil, fmt.Errorf("inventory-id of inventory object in cluster doesn't match provided id %q", sw.inv.ID())
				}
			}
		}
		pruneObjs, err := a.pruner.GetPruneObjs(sw.inv, localObjs, prune.Options{
			DryRunStrategy: o.DryRunStrategy,
		})
		if err != nil {
			return nil, nil, err
		}

		sw.applyObjs = grouped[wave]
		sw.pruneObjs = pruneObjs

		syncWaves[idx] = sw
	}

	return nil, syncWaves, nil
}

// Run individual sync wave
func (a *Applier) runWaves(ctx context.Context, waves []syncWave, options ApplierOptions) <-chan event.Event {
	eventChannel := make(chan event.Event)
	setDefaults(&options)

	go func() {
		defer close(eventChannel)
		for _, wave := range waves {

			var err error
			klog.V(4).Infof("sync wave %d run for %d objects", wave.wave, len(wave.applyObjs)+len(wave.pruneObjs))

			// Validate the resources to make sure we catch those problems early
			// before anything has been updated in the cluster.
			vCollector := &validation.Collector{}
			validator := &validation.Validator{
				Collector: vCollector,
				Mapper:    a.mapper,
			}
			allObjs := append(wave.applyObjs, wave.pruneObjs...)
			validator.Validate(allObjs)

			// Build a TaskContext for passing info between tasks
			resourceCache := cache.NewResourceCacheMap()
			taskContext := taskrunner.NewTaskContext(eventChannel, resourceCache)

			// Fetch the queue (channel) of tasks that should be executed.
			klog.V(4).Infoln("applier building task queue...")
			// Build list of apply validation filters.
			applyFilters := []filter.ValidationFilter{
				filter.InventoryPolicyApplyFilter{
					Client:    a.client,
					Mapper:    a.mapper,
					Inv:       wave.inv,
					InvPolicy: options.InventoryPolicy,
				},
				filter.DependencyFilter{
					TaskContext:       taskContext,
					ActuationStrategy: actuation.ActuationStrategyApply,
					DryRunStrategy:    options.DryRunStrategy,
				},
			}
			// Build list of prune validation filters.
			pruneFilters := []filter.ValidationFilter{
				filter.PreventRemoveFilter{},
				filter.InventoryPolicyPruneFilter{
					Inv:       wave.inv,
					InvPolicy: options.InventoryPolicy,
				},
				filter.LocalNamespacesFilter{
					LocalNamespaces: localNamespaces(wave.inv, object.UnstructuredSetToObjMetadataSet(allObjs)),
				},
				filter.DependencyFilter{
					TaskContext:       taskContext,
					ActuationStrategy: actuation.ActuationStrategyDelete,
					DryRunStrategy:    options.DryRunStrategy,
				},
			}
			// Build list of apply mutators.
			applyMutators := []mutator.Interface{
				&mutator.ApplyTimeMutator{
					Client:        a.client,
					Mapper:        a.mapper,
					ResourceCache: resourceCache,
				},
			}
			taskBuilder := &solver.TaskQueueBuilder{
				Pruner:        a.pruner,
				DynamicClient: a.client,
				OpenAPIGetter: a.openAPIGetter,
				InfoHelper:    a.infoHelper,
				Mapper:        a.mapper,
				InvClient:     a.invClient,
				Collector:     vCollector,
				ApplyFilters:  applyFilters,
				ApplyMutators: applyMutators,
				PruneFilters:  pruneFilters,
			}
			opts := solver.Options{
				ServerSideOptions:      options.ServerSideOptions,
				ReconcileTimeout:       options.ReconcileTimeout,
				Destroy:                false,
				Prune:                  !options.NoPrune,
				DryRunStrategy:         options.DryRunStrategy,
				PrunePropagationPolicy: options.PrunePropagationPolicy,
				PruneTimeout:           options.PruneTimeout,
				InventoryPolicy:        options.InventoryPolicy,
			}

			// Build the ordered set of tasks to execute.
			taskQueue := taskBuilder.
				WithApplyObjects(wave.applyObjs).
				WithPruneObjects(wave.pruneObjs).
				WithInventory(wave.inv).
				Build(taskContext, opts)

			klog.V(4).Infof("validation errors: %d", len(vCollector.Errors))
			klog.V(4).Infof("invalid objects: %d", len(vCollector.InvalidIds))

			// Handle validation errors
			switch options.ValidationPolicy {
			case validation.ExitEarly:
				err = vCollector.ToError()
				if err != nil {
					handleError(eventChannel, err)
					return
				}
			case validation.SkipInvalid:
				for _, err := range vCollector.Errors {
					handleValidationError(eventChannel, err)
				}
			default:
				handleError(eventChannel, fmt.Errorf("invalid ValidationPolicy: %q", options.ValidationPolicy))
				return
			}

			// Register invalid objects to be retained in the inventory, if present.
			for _, id := range vCollector.InvalidIds {
				taskContext.AddInvalidObject(id)
			}

			// Send event to inform the caller about the resources that
			// will be applied/pruned.
			eventChannel <- event.Event{
				Type: event.InitType,
				InitEvent: event.InitEvent{
					ActionGroups: taskQueue.ToActionGroups(),
				},
			}
			// Create a new TaskStatusRunner to execute the taskQueue.
			klog.V(4).Infoln("applier building TaskStatusRunner...")
			allIds := object.UnstructuredSetToObjMetadataSet(allObjs)
			statusWatcher := a.statusWatcher
			// Disable watcher for dry runs
			if opts.DryRunStrategy.ClientOrServerDryRun() {
				statusWatcher = watcher.BlindStatusWatcher{}
			}
			runner := taskrunner.NewTaskStatusRunner(allIds, statusWatcher)
			klog.V(4).Infoln("applier running TaskStatusRunner...")
			err = runner.Run(ctx, taskContext, taskQueue.ToChannel(), taskrunner.Options{
				EmitStatusEvents:         options.EmitStatusEvents,
				WatcherRESTScopeStrategy: options.WatcherRESTScopeStrategy,
			})
			if err != nil {
				handleError(eventChannel, err)
				return
			}
		}
	}()

	return eventChannel
}

// handleSyncWaveShuffles ensures that Unstructured's that have moved between
// sync waves since the last execution are correctly modified to be associated
// to their new wave inventory to avoid pruning or adoption errors.
func handleSyncWaveShuffles(waves []syncWave) ([]syncWave, error) {
	applyObjsToWave := make(map[string]int)
	// Iterate through each wave and build up a map of obj IDs to wave int
	for _, wave := range waves {
		for _, unstructured := range wave.applyObjs {
			obj := object.UnstructuredToObjMetadata(unstructured)
			id := obj.String()

			if _, exists := applyObjsToWave[id]; exists {
				// Double handling of unstructured - duplicate resource?
				// TODO(aidan): handle error scenario with event emitting
				panic("duplicate resource detected")
			}

			applyObjsToWave[id] = wave.wave
		}
	}

	// Iterate through all pruneObjs to determine if any objs have been moved
	// between waves.
	for _, wave := range waves {
		tempPrune := wave.pruneObjs[:0]

		for _, unstructured := range wave.pruneObjs {
			obj := object.UnstructuredToObjMetadata(unstructured)
			id := obj.String()

			// Identify Objs that exists in the waves pruneObjs but also exists
			// in a wave's applyObj.  This means an object has been moved to a
			// different wave since the last execution.
			if _, exists := applyObjsToWave[id]; !exists {
				// Remove the moved obj from the pruneObjs to prevent deletion
				tempPrune = append(tempPrune, unstructured)
				// TODO(aidan): Update new wave's inventory to add new obj and
				// update objs inventory annotation to match
				// or just force-adopt as part of this tool.
			}
		}
		wave.pruneObjs = tempPrune
	}

	return waves, nil
}

// Run performs the Apply step. This happens asynchronously with updates
// on progress and any errors reported back on the event channel.
// Cancelling the operation or setting timeout on how long to Wait
// for it complete can be done with the passed in context.
// Note: There isn't currently any way to interrupt the operation
// before all the given resources have been applied to the cluster. Any
// cancellation or timeout will only affect how long we Wait for the
// resources to become current.
func (a *Applier) Run(ctx context.Context, invInfo inventory.Info, objects object.UnstructuredSet, options ApplierOptions) <-chan event.Event {
	klog.V(4).Infof("apply run for %d objects", len(objects))
	_, waves, err := a.prepareSyncWaves(invInfo, objects, options)
	if err != nil {
		//TODO(aidan): handle emitting error to event channel
		panic("failed to calculate sync waves")
	}

	// Now we have all of the sync waves we need to do 2 things
	// 1) clean up any 'sync wave' inventory files that are not longer in use (ie, no resources belong to a pre-existig sync wave)
	// 2) detect resources that have moved between sync waves and ensure they not marked as prune
	//	  this also means ensuring they are added to the target inv objects inventory before the sync wave begins
	waves, err = handleSyncWaveShuffles(waves)
	if err != nil {
		panic("unhandled sync wave shuffle failure")
	}

	// TODO(aidan): need to now determine if there are any inventory files that need cleaning up..
	// do this by querying cluster for inventory objects with sync waves not in waves
	// Add this is a task to the taskqueue

	klog.V(4).Infof("executing sync across %d waves", len(waves))
	return a.runWaves(ctx, waves, options)
}

type ApplierOptions struct {
	// Encapsulates the fields for server-side apply.
	ServerSideOptions common.ServerSideOptions

	// ReconcileTimeout defines whether the applier should wait
	// until all applied resources have been reconciled, and if so,
	// how long to wait.
	ReconcileTimeout time.Duration

	// EmitStatusEvents defines whether status events should be
	// emitted on the eventChannel to the caller.
	EmitStatusEvents bool

	// NoPrune defines whether pruning of previously applied
	// objects should happen after apply.
	NoPrune bool

	// DryRunStrategy defines whether changes should actually be performed,
	// or if it is just talk and no action.
	DryRunStrategy common.DryRunStrategy

	// PrunePropagationPolicy defines the deletion propagation policy
	// that should be used for pruning. If this is not provided, the
	// default is to use the Background policy.
	PrunePropagationPolicy metav1.DeletionPropagation

	// PruneTimeout defines whether we should wait for all resources
	// to be fully deleted after pruning, and if so, how long we should
	// wait.
	PruneTimeout time.Duration

	// InventoryPolicy defines the inventory policy of apply.
	InventoryPolicy inventory.Policy

	// ValidationPolicy defines how to handle invalid objects.
	ValidationPolicy validation.Policy

	// RESTScopeStrategy specifies which strategy to use when listing and
	// watching resources. By default, the strategy is selected automatically.
	WatcherRESTScopeStrategy watcher.RESTScopeStrategy
}

// setDefaults set the options to the default values if they
// have not been provided.
func setDefaults(o *ApplierOptions) {
	if o.PrunePropagationPolicy == "" {
		o.PrunePropagationPolicy = metav1.DeletePropagationBackground
	}
}

func handleError(eventChannel chan event.Event, err error) {
	eventChannel <- event.Event{
		Type: event.ErrorType,
		ErrorEvent: event.ErrorEvent{
			Err: err,
		},
	}
}

// localNamespaces stores a set of strings of all the namespaces
// for the passed non cluster-scoped localObjs, plus the namespace
// of the passed inventory object. This is used to skip deleting
// namespaces which have currently applied objects in them.
func localNamespaces(localInv inventory.Info, localObjs []object.ObjMetadata) sets.String { // nolint:staticcheck
	namespaces := sets.NewString()
	for _, obj := range localObjs {
		if obj.Namespace != "" {
			namespaces.Insert(obj.Namespace)
		}
	}
	invNamespace := localInv.Namespace()
	if invNamespace != "" {
		namespaces.Insert(invNamespace)
	}
	return namespaces
}

func handleValidationError(eventChannel chan<- event.Event, err error) {
	switch tErr := err.(type) {
	case *validation.Error:
		// handle validation error about one or more specific objects
		eventChannel <- event.Event{
			Type: event.ValidationType,
			ValidationEvent: event.ValidationEvent{
				Identifiers: tErr.Identifiers(),
				Error:       tErr,
			},
		}
	default:
		// handle general validation error (no specific object)
		eventChannel <- event.Event{
			Type: event.ValidationType,
			ValidationEvent: event.ValidationEvent{
				Error: tErr,
			},
		}
	}
}
