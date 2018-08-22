package main

import (
	"io"
	"log"
	"os"
	"skbn/pkg/skbn"
	"skbn/pkg/utils"

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
	src string
	dst string

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

			srcPrefix, srcPath := utils.SplitInTwo(c.src, "://")
			dstPrefix, dstPath := utils.SplitInTwo(c.dst, "://")

			// CheckConnections

			// Copy
			var buffer []byte

			if srcPrefix == "k8s" {
				bytes, err := skbn.DownloadFromK8s(srcPath)
				if err != nil {
					log.Fatal(err)
				}
				buffer = bytes
			}
			if srcPrefix == "s3" {
				bytes, err := skbn.DownloadFromS3(dstPath)
				if err != nil {
					log.Fatal(err)
				}
				buffer = bytes
			}

			if dstPrefix == "k8s" {
				log.Fatal("Not yet implemented")
			}
			if dstPrefix == "s3" {
				os.Setenv("AWS_PROFILE", "nuvo-dev-access")
				os.Setenv("AWS_SDK_LOAD_CONFIG", "1")

				// Upload
				err := skbn.UploadToS3(dstPath, buffer)
				if err != nil {
					log.Fatal(err)
				}
			}
		},
	}
	f := cmd.Flags()

	f.StringVar(&c.src, "src", "", "path to copy from. Example: k8s://<namespace>/<podName>/<containerName>/path/to/copyfrom")
	f.StringVar(&c.dst, "dst", "", "path to copy to. Example: s3://<bucketName>/path/to/copyto")

	return cmd
}
