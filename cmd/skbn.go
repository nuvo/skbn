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
	src              string
	dst              string
	parallel         int
	bufferSize       float64
	s3partSize       int64
	s3maxUploadParts int
	verbose          bool

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
			if err := skbn.Copy(c.src, c.dst, c.parallel, c.bufferSize, c.s3partSize, c.s3maxUploadParts, c.verbose); err != nil {
				log.Fatal(err)
			}
		},
	}
	f := cmd.Flags()

	f.StringVar(&c.src, "src", "", "path to copy from. Example: k8s://<namespace>/<podName>/<containerName>/path/to/copyfrom")
	f.StringVar(&c.dst, "dst", "", "path to copy to. Example: s3://<bucketName>/path/to/copyto")
	f.IntVarP(&c.parallel, "parallel", "p", 1, "number of files to copy in parallel. set this flag to 0 for full parallelism")
	f.Float64VarP(&c.bufferSize, "buffer-size", "b", 6.75, "in memory buffer size (MB) to use for files copy (buffer per file)")
	f.Int64VarP(&c.s3partSize, "s3-part-size", "s", 128*1024*1024, "size of each part in bytes for multipart upload to S3. Default is 128MB. Consider that the default MaxUploadParts is 10000 so max file size with default s3 settings is 1.28TB.")
	f.IntVarP(&c.s3maxUploadParts, "s3-max-upload-parts", "m", 10000, "maximum number of parts for multipart upload to S3. Default is 10000.")
	f.BoolVarP(&c.verbose, "verbose", "v", false, "verbose output")

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
