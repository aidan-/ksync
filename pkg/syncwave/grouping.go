package syncwave

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

func GroupUnstructureds(unstructureds []*unstructured.Unstructured) map[string][]*unstructured.Unstructured {
	grouped := make(map[string][]*unstructured.Unstructured)
	for _, u := range unstructureds {
		wave := u.GetAnnotations()[Annotation]
		if wave == "" {
			wave = "0"
		}
		grouped[wave] = append(grouped[wave], u)
	}

	waves := make([]string, 0)
	for wave, _ := range grouped {
		waves = append(waves, wave)
	}

	return grouped
}
