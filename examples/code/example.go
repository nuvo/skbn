package main

import (
	"log"

	"github.com/maorfr/skbn/pkg/skbn"
)

func main() {
	src := "k8s://namespace/pod/container/path/to/copy/from"
	dst := "s3://bucket/path/to/copy/to"
	parallel := 0 // all at once

	if err := skbn.Copy(src, dst, parallel); err != nil {
		log.Fatal(err)
	}
}
