package main

import (
	"log"
	"time"

	"github.com/nuvo/skbn/pkg/skbn"
)

func main() {
	src := "k8s://namespace/pod/container/path/to/copy/from"
	dst := "s3://bucket/path/to/copy/to"
	parallel := 0     // all at once
	bufferSize := 1.0 // 1GB of in memory buffer size

	start := time.Now()
	if err := skbn.Copy(src, dst, parallel, bufferSize); err != nil {
		log.Fatal(err)
	}
	elapsed := time.Since(start)
	log.Printf("Copy execution time: %s", elapsed)
}
