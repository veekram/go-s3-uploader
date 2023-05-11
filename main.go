package main

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type TreeNode struct {
	Name     string
	Children []*TreeNode
}

func buildTree(rootPath string) (*TreeNode, error) {
	files, err := os.ReadDir(rootPath)
	if err != nil {
		return nil, err
	}

	node := &TreeNode{
		Name: rootPath,
	}

	for _, file := range files {
		if file.IsDir() {
			if file.Name() == "__MACOSX" {
				continue // Ignore __MACOSX folder
			}
			childNode, err := buildTree(filepath.Join(rootPath, file.Name()))
			if err != nil {
				return nil, err
			}
			node.Children = append(node.Children, childNode)
		}
	}

	return node, nil
}

func extractZipFiles(zipFilePath, extractPath string) error {
	err := os.MkdirAll(extractPath, 0755)
	if err != nil {
		return err
	}

	reader, err := zip.OpenReader(zipFilePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	for _, file := range reader.File {
		// Skip files and directories under __MACOSX
		if strings.HasPrefix(file.Name, "__MACOSX/") {
			continue
		}

		path := filepath.Join(extractPath, file.Name)

		// Ensure the extracted file path is within the designated extraction directory
		if !strings.HasPrefix(filepath.Clean(path), filepath.Clean(extractPath)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path: %s", file.Name)
		}

		if file.FileInfo().IsDir() {
			err := os.MkdirAll(path, file.Mode())
			if err != nil {
				return err
			}
			continue
		}

		// Create parent directories if they don't exist
		err = os.MkdirAll(filepath.Dir(path), 0755)
		if err != nil {
			return err
		}

		srcFile, err := file.Open()
		if err != nil {
			return err
		}
		defer srcFile.Close()

		destFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
		if err != nil {
			return err
		}
		defer destFile.Close()

		_, err = io.Copy(destFile, srcFile)
		if err != nil {
			return err
		}

		// Extract recursively if the file is a zip file
		if strings.HasSuffix(file.Name, ".zip") {
			err = extractZipFiles(path, filepath.Dir(path))
			if err != nil {
				return err
			}
			// Delete the zip file after extraction
			err = os.Remove(path)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func printTree(node *TreeNode, level int) {
	indent := strings.Repeat("  ", level)
	fmt.Println(indent + node.Name)

	for _, child := range node.Children {
		printTree(child, level+1)
	}
}

func uploadFileToS3(s3Client *s3.S3, bucketName, filePath, key string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		return err
	}

	return nil
}

func uploadDirectoryToS3(s3Client *s3.S3, bucketName, directoryPath, prefix string, wg *sync.WaitGroup) {
	defer wg.Done()

	var totalFiles int64
	var uploadedFiles int64
	startTime := time.Now()

	err := filepath.Walk(directoryPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		totalFiles++ // Increment total files count

		// Determine the S3 key based on the prefix and relative path
		relativePath := strings.TrimPrefix(path, directoryPath)
		key := filepath.Join(prefix, relativePath)

		fileStartTime := time.Now() // Start time for current file upload

		// Upload the file to S3
		err = uploadFileToS3(s3Client, bucketName, path, key)
		if err != nil {
			return err
		}

		fileEndTime := time.Now() // End time for current file upload

		// Increment uploaded files count and update progress
		atomic.AddInt64(&uploadedFiles, 1)
		printProgress(uploadedFiles, totalFiles, path, fileStartTime, fileEndTime)

		return nil
	})

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)

	if err != nil {
		fmt.Printf("Error uploading directory %s: %s\n", directoryPath, err.Error())
	} else {
		fmt.Printf("\nUploaded directory %s\n", directoryPath)
		fmt.Printf("Total upload time: %s\n", elapsedTime.String())
	}
}

func printProgress(uploadedFiles, totalFiles int64, fileName string, startTime, endTime time.Time) {
	progress := float64(uploadedFiles) / float64(totalFiles) * 100
	fileTime := endTime.Sub(startTime)
	fmt.Printf("\rUploading: %.2f%% (%d/%d) - %s - Time: %s", progress, uploadedFiles, totalFiles, fileName, fileTime.String())
}

func main() {
	zipFilePath := "ziptest/<big-zip-file>" // Replace with the path to the zip file
	extractPath := "ziptest/extracted"      // Replace with the desired extraction directory

	err := extractZipFiles(zipFilePath, extractPath)
	if err != nil {
		fmt.Println("Error extracting zip files:", err)
		return
	}

	tree, err := buildTree(extractPath)
	if err != nil {
		fmt.Println("Error building tree:", err)
		return
	}

	printTree(tree, 0)

	// Specify your AWS credentials and region
	awsConfig := &aws.Config{
		Region:      aws.String("S3-REGION"),
		Credentials: credentials.NewStaticCredentials("S3-ACCESS-ID", "S3-ACCESS-SECRET", ""),
	}
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		fmt.Println("Failed to create AWS session:", err)
		return
	}

	// Create an S3 client
	s3Client := s3.New(sess)

	// Specify your S3 bucket name
	bucketName := "S3-BUCKET-NAME"

	// Specify the path of the directory to upload
	directoryPath := "ziptest/extracted"

	// Specify the prefix for S3 keys (optional)
	uploadPrefix := "uploads/"

	// Concurrently upload the directory to S3
	var wg sync.WaitGroup
	wg.Add(1)
	go uploadDirectoryToS3(s3Client, bucketName, directoryPath, uploadPrefix, &wg)

	// Wait for the upload to complete
	wg.Wait()
}
