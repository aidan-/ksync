package syncwave

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/cli-utils/pkg/object"
)

func TestGroupUnstructureds(t *testing.T) {
	tests := []struct {
		name    string
		objects object.UnstructuredSet
		want    []int
	}{
		{
			name:    "empty objects",
			objects: object.UnstructuredSet{},
			want:    []int{},
		},
		{
			name: "single object",
			objects: object.UnstructuredSet{
				&unstructured.Unstructured{},
			},
			want: []int{0},
		},
		{
			name: "multiple objects",
			objects: object.UnstructuredSet{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"annotations": map[string]interface{}{
								Annotation: "1",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"annotations": map[string]interface{}{
								Annotation: "2",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"annotations": map[string]interface{}{
								Annotation: "1",
							},
						},
					},
				},
			},
			want: []int{1, 2},
		},
		{
			name: "multiple objects, negative wave and default",
			objects: object.UnstructuredSet{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"annotations": map[string]interface{}{
								Annotation: "-1",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"annotations": map[string]interface{}{
								Annotation: "2",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
			},
			want: []int{-1, 0, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, grouped, err := GroupUnstructureds(tt.objects)
			if err != nil {
				t.Errorf("GroupUnstructureds() unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupUnstructureds() got = %v, want %v", got, tt.want)
			}
			if len(grouped) != len(tt.want) {
				t.Errorf("GroupUnstructureds() grouped length got = %d, want %d", len(grouped), len(tt.want))
			}
			for _, wave := range tt.want {
				if _, ok := grouped[wave]; !ok {
					t.Errorf("GroupUnstructureds() grouped wave %d not found", wave)
				}
			}
		})
	}
}
