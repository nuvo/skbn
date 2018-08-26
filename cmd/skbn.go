package main

import (
	"io"
	"log"
	"os"
	"skbn/pkg/skbn"

	"github.com/spf13/cobra"
)

func main() {
	cmd := NewRootCmd(os.Args[1:])
	if err := cmd.Execute(); err != nil {
		log.Fatal("Failed to execute command")
	}
}

// NewRootCmd represents the base command when called without any subcommands
func NewRootCmd(args []string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "skbn",
		Short: "",
		Long:  ``,
	}

	out := cmd.OutOrStdout()

	cmd.AddCommand(NewCpCmd(out))

	return cmd
}

type cpCmd struct {
	src      string
	dst      string
	parallel int

	out io.Writer
}

// NewCpCmd represents the copy command
func NewCpCmd(out io.Writer) *cobra.Command {
	c := &cpCmd{out: out}

	cmd := &cobra.Command{
		Use:   "cp",
		Short: "Copy files or directories Kubernetes <--> S3 (and more?)",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			if err := skbn.Copy(c.src, c.dst, c.parallel); err != nil {
				log.Fatal(err)
			}
		},
	}
	f := cmd.Flags()

	f.StringVar(&c.src, "src", "", "path to copy from. Example: k8s://<namespace>/<podName>/<containerName>/path/to/copyfrom")
	f.StringVar(&c.dst, "dst", "", "path to copy to. Example: s3://<bucketName>/path/to/copyto")
	f.IntVar(&c.parallel, "parallel", 1, "number of files to copy in parallel. set this flag to 0 for full parallelism")

	cmd.MarkFlagRequired("src")
	cmd.MarkFlagRequired("dst")

	return cmd
}
