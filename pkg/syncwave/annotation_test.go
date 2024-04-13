package syncwave

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestHasAnnotation(t *testing.T) {
	tests := []struct {
		name string
		u    *unstructured.Unstructured
		want bool
	}{
		{
			name: "nil unstructured",
			u:    nil,
			want: false,
		},
		{
			name: "no annotation",
			u:    &unstructured.Unstructured{},
			want: false,
		},
		{
			name: "annotation present",
			u: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							Annotation: "1",
						},
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasAnnotation(tt.u); got != tt.want {
				t.Errorf("HasAnnotation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadAnnotation(t *testing.T) {
	tests := []struct {
		name    string
		u       *unstructured.Unstructured
		want    int
		wantErr bool
	}{
		{
			name:    "nil unstructured",
			u:       nil,
			want:    0,
			wantErr: false,
		},
		{
			name:    "no annotation",
			u:       &unstructured.Unstructured{},
			want:    0,
			wantErr: false,
		},
		{
			name: "annotation present",
			u: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							Annotation: "1",
						},
					},
				},
			},
			want:    1,
			wantErr: false,
		},
		{
			name: "annotation present, negative number",
			u: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							Annotation: "-1",
						},
					},
				},
			},
			want:    -1,
			wantErr: false,
		},
		{
			name: "annotation present, but not a number",
			u: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							Annotation: "a",
						},
					},
				},
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadAnnotation(tt.u)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadAnnotation() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ReadAnnotation() = %v, want %v", got, tt.want)
			}
		})
	}
}
