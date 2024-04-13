package syncwave

import (
	"sort"

	"sigs.k8s.io/cli-utils/pkg/object"
)

func GroupUnstructureds(objects object.UnstructuredSet) ([]int, map[int]object.UnstructuredSet, error) {
	grouped := make(map[int]object.UnstructuredSet)
	for _, obj := range objects {
		wave, err := ReadAnnotation(obj)
		if err != nil {
			return nil, nil, err
		}
		grouped[wave] = append(grouped[wave], obj)
	}

	// Create ordered slice of wave numbers.
	waves := make([]int, 0, len(grouped))
	for wave := range grouped {
		waves = append(waves, wave)
	}
	sort.Ints(waves)

	return waves, grouped, nil
}
