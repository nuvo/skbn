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

			s3Region := "eu-central-1"
			s3Bucket := "nuvo-c2s3-test"
			filePath := "glide.yaml"

			// Open the file for use
			file, err := os.Open(filePath)
			if err != nil {
				log.Fatal(err.Error())
			}
			defer file.Close()

			// Get file size and read the file content into a buffer
			fileInfo, _ := file.Stat()
			var size = fileInfo.Size()
			buffer := make([]byte, size)
			file.Read(buffer)

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
			fmt.Println((string)(buffer))
		},
	}

	return cmd
}
