package syncwave

import (
	"strconv"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	Annotation = "config.kubernetes.io/sync-wave"
)

// HasAnnotation returns true if the config.kubernetes.io/sync-wave annotation
// is present, false if not.
func HasAnnotation(u *unstructured.Unstructured) bool {
	if u == nil {
		return false
	}
	_, found := u.GetAnnotations()[Annotation]
	return found
}

// ReadAnnotation reads the config.kubernetes.io/sync-wave annotation from the
// provided unstructured object and returns the wave number. If the annotation
// is not present, it returns 0.
func ReadAnnotation(u *unstructured.Unstructured) (int, error) {
	if u == nil {
		return 0, nil
	}
	waveStr := u.GetAnnotations()[Annotation]
	if waveStr == "" {
		return 0, nil
	}

	wave, err := strconv.Atoi(waveStr)
	if err != nil {
		return 0, err
	}

	return wave, nil
}
