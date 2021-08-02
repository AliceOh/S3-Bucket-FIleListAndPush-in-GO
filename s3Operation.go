//
// Copyright (C) Eratos Group Pty Ltd and its affiliates.
//
// This file is a part of the Eratos platform. It is proprietary software, you may not:
//
//   a) redistribute it and/or modify without permission from Eratos Group Pty Ltd.
//   b) reuse the code in part or in full without permission from Eratos Group Pty Ltd.
//
// If permission has been granted for reuse and/or redistribution it is subject
// to the following conditions:
//
//   a) The above copyright notice and this permission notice shall be included
//      in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
//

package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/mattetti/filebuffer"
)

func listS3(s3BucketName string) {
	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)

	// Get the first page of results for ListObjectsV2 for a bucket
	// output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
	// 	Bucket: aws.String("eratos-meter-service-test"),
	// })
	output, err := client.ListObjects(context.TODO(), &s3.ListObjectsInput{
		Bucket: aws.String(s3BucketName),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("first page results:")
	for _, object := range output.Contents {
		log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
	}
}

var bucketName string = "eratos-meter-service-test"

func main() {

	listS3(bucketName)

	// Create the context for the following requests. We allow 60 seconds for
	// pushing the files.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Get arguments. TODO: make it input
	filePath := "02082021_test.csv"
	s3URI := "s3://eratos-meter-service-test/event1/02082021_test.csv"

	// Check s3URI argument.
	uri, err := url.ParseRequestURI(s3URI)
	if err != nil {
		fmt.Printf("invalid URI '%s' for S3 content: %v", s3URI, err)
		return
	}
	if uri.Scheme != "s3" {
		fmt.Printf("invalid URI '%s' for S3 content: expected scheme to be s3", s3URI)
		return
	}
	if uri.Host == "" {
		fmt.Printf("invalid URI '%s' for S3 content: expected host to be bucket name", s3URI)
		return
	}
	if uri.Path == "" {
		fmt.Printf("invalid URI '%s' for S3 content: path is empty", s3URI)
		return
	}

	// Read the file.
	inp, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("error reading file from path '%s', err is %v\n", filePath, err)
		return
	}

	// Upload file to S3
	s3Req := &s3.PutObjectInput{
		Bucket: aws.String(uri.Host),
		Key:    aws.String(strings.TrimPrefix(uri.Path, "/")),
		Body:   filebuffer.New(inp),
	}

	// Load the Shared AWS Configuration (~/.aws/config)
	// cfg, err := config.LoadDefaultConfig(context.TODO())
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)

	// _, err = client.PutObject(context.TODO(), s3Req)
	_, err = client.PutObject(ctx, s3Req)
	if err != nil {
		fmt.Printf("failed to put '%s': %v", s3URI, err)
		return
	}

	log.Println("first page results again after pushing files to S3:")
	listS3(bucketName)
}
