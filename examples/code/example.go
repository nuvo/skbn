package main

import (
	"log"

	"github.com/nuvo/skbn/pkg/skbn"
)

func main() {
	src := "k8s://namespace/pod/container/path/to/copy/from"
	dst := "s3://bucket/path/to/copy/to"
	parallel := 0                        // all at once
	bufferSize := 1.0                    // 1GB of in memory buffer size
	s3partSize := int64(5 * 1024 * 1024) // 5MB
	s3maxUploadParts := 10000
	verbose := true

	if err := skbn.Copy(src, dst, parallel, bufferSize, s3partSize, s3maxUploadParts, verbose); err != nil {
		log.Fatal(err)
	}
}
