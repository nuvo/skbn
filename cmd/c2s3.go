package main

import (
	"c2s3/pkg/c2s3"
	"fmt"
	"log"
	"os"

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
		Use:   "c2s3",
		Short: "",
		Long:  ``,
		Run: func(cmd *cobra.Command, args []string) {

			namespace := "dev1"
			podName := "cassandra-0"
			containerName := "cassandra"
			command := "cat docker-entrypoint.sh"

			output, stderr, err := c2s3.Exec(command, containerName, podName, namespace, nil)

			if len(stderr) != 0 {
				fmt.Println("STDERR:", stderr)
			}
			if err != nil {
				fmt.Printf("Error occured while `exec`ing to the Pod %q, namespace %q, command %q. Error: %+v\n", podName, namespace, command, err)
			} else {
				fmt.Println("Output:")
				fmt.Println(output)
			}

			return

			s3Region := "eu-central-1"
			s3Bucket := "nuvo-c2s3-test"
			filePath := "testfile"
			buffer := []byte(`First line
Second line
Third line`)

			os.Setenv("AWS_PROFILE", "nuvo-dev-access")
			os.Setenv("AWS_SDK_LOAD_CONFIG", "1")

			// Upload
			err = c2s3.UploadToS3(s3Region, s3Bucket, filePath, buffer)
			if err != nil {
				log.Fatal(err)
			}

			// Download
			buffer, err = c2s3.DownloadFromS3(s3Region, s3Bucket, filePath)
			if err != nil {
				log.Fatal(err)
			}

			// Print
			fmt.Println((string)(buffer))
		},
	}

	return cmd
}
