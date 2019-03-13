package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/nuvo/skbn/pkg/skbn"

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
	cmd.AddCommand(NewVersionCmd(out))

	return cmd
}

type cpCmd struct {
	src        string
	dst        string
	parallel   int
	bufferSize float64

	out io.Writer
}

// NewCpCmd represents the copy command
func NewCpCmd(out io.Writer) *cobra.Command {
	c := &cpCmd{out: out}

	cmd := &cobra.Command{
		Use:   "cp",
		Short: "Copy files or directories Kubernetes and Cloud storage",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			if err := skbn.Copy(c.src, c.dst, c.parallel, c.bufferSize); err != nil {
				log.Fatal(err)
			}
		},
	}
	f := cmd.Flags()

	f.StringVar(&c.src, "src", "", "path to copy from. Example: k8s://<namespace>/<podName>/<containerName>/path/to/copyfrom")
	f.StringVar(&c.dst, "dst", "", "path to copy to. Example: s3://<bucketName>/path/to/copyto")
	f.IntVarP(&c.parallel, "parallel", "p", 1, "number of files to copy in parallel. set this flag to 0 for full parallelism")
	f.Float64VarP(&c.bufferSize, "buffer-size", "b", 6.75, "in memory buffer size (MB) to use for files copy (buffer per file)")

	cmd.MarkFlagRequired("src")
	cmd.MarkFlagRequired("dst")

	return cmd
}

var (
	// GitTag stands for a git tag
	GitTag string
	// GitCommit stands for a git commit hash
	GitCommit string
)

// NewVersionCmd prints version information
func NewVersionCmd(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Version %s (git-%s)\n", GitTag, GitCommit)
		},
	}

	return cmd
}
