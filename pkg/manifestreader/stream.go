// Copyright 2020 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package manifestreader

import (
	"io"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	mr "sigs.k8s.io/cli-utils/pkg/manifestreader"
	"sigs.k8s.io/kustomize/kyaml/kio"
	"sigs.k8s.io/kustomize/kyaml/kio/kioutil"
)

// StreamManifestReader implements ManifestReader interface.
var _ mr.ManifestReader = &StreamManifestReader{}

// StreamManifestReader reads manifest from the provided io.Reader
// and returns them as Info objects. The returned Infos will not have
// client or mapping set.
type StreamManifestReader struct {
	ReaderName string
	Reader     io.Reader

	mr.ReaderOptions
}

// Read reads the manifests and returns them as Info objects.
func (r *StreamManifestReader) Read() ([]*unstructured.Unstructured, error) {
	var objs []*unstructured.Unstructured
	nodes, err := (&kio.ByteReader{
		Reader: r.Reader,
	}).Read()
	if err != nil {
		return objs, err
	}

	for _, n := range nodes {
		err = mr.RemoveAnnotations(n, kioutil.IndexAnnotation)
		if err != nil {
			return objs, err
		}
		u, err := mr.KyamlNodeToUnstructured(n)
		if err != nil {
			return objs, err
		}
		objs = append(objs, u)
	}

	objs = mr.FilterLocalConfig(objs)

	err = mr.SetNamespaces(r.Mapper, objs, r.Namespace, r.EnforceNamespace)
	return objs, err
}
