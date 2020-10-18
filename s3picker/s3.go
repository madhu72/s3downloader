package s3picker
/* Copyright 2020 Venkateswara Rao Thota
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom
the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
import (
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
)

type S3Manager struct {
	S3 struct {
		AccessKeyId string `yaml:"access_key_id"`
		SecretKey string `yaml:"secret_key"`
		Bucket string `yaml:"bucket"`
		Region string `yaml:"region"`
		DownloadPath string `yaml:"download_path"`
		PathSeparator string `yaml:"path_sep"`
	}
}

func (mgr *S3Manager) WriteFileToFS(filename string, contents []byte) error {
	err := ioutil.WriteFile(filename, contents, 0644)
	return err
}
func (mgr *S3Manager) ReadFileFromFS(filename string) []byte {
	dat, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil
	}
	return dat
}

func (mgr *S3Manager) FileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil { return true, nil }
	if os.IsNotExist(err) { return false, nil }
	return false, err
}

func (mgr *S3Manager) LoadYaml(conf string) error {
	contents := mgr.ReadFileFromFS(conf)
	err := yaml.Unmarshal(contents,&mgr)
	if err != nil {
		fmt.Printf("Error occurred while loading yaml file: %s\nError:%v\n",conf,err)
		return err
	}
	return nil
}

func (mgr *S3Manager) ShowConfig() (error) {
	output, err := json.Marshal(&mgr)
	if err != nil {
		return err
	}
	fmt.Printf("The converted config is:%v\n",string(output))
	return nil
}

func (mgr *S3Manager) ShowSecurityDetails() {
	fmt.Printf("Access Token:%s\n",mgr.S3.AccessKeyId)
	fmt.Printf("Secret Key:%s\n",mgr.S3.SecretKey)
	fmt.Printf("Bucket:%s\n",mgr.S3.Bucket)
	fmt.Printf("Region:%s\n",mgr.S3.Region)
}

func (mgr *S3Manager) DownloadDocuments() {
	config := &aws.Config{
		Region:      aws.String(mgr.S3.Region),
		Credentials: credentials.NewStaticCredentials(mgr.S3.AccessKeyId, mgr.S3.SecretKey, ""),
	}
	sess := session.Must(session.NewSession(config))
	svc := s3.New(sess)
	i := 0
	err := svc.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: &mgr.S3.Bucket,
	}, func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {
		fmt.Println("Page,", i)
		i++

		for _, obj := range p.Contents {
			fmt.Printf("Object:%s", *obj.Key)
			if found, _:= mgr.FileExists(mgr.S3.DownloadPath+mgr.S3.PathSeparator+*obj.Key); !found {
				fmt.Printf("\n")
				err := mgr.DownloadDocument(*obj.Key,mgr.S3.DownloadPath,*obj.Key)
				if err!= nil {
					fmt.Printf("Error Occurred:%v\n",err)
				}
			} else {
				fmt.Printf(" Already Exists.\n")
			}
		}
		return true
	})
	if err != nil {
		fmt.Println("failed to list objects", err)
		return
	}
}

func (mgr *S3Manager) DownloadDocument(key, filepath2, filename string) error {
	config := &aws.Config{
		Region:      aws.String(mgr.S3.Region),
		Credentials: credentials.NewStaticCredentials(mgr.S3.AccessKeyId, mgr.S3.SecretKey, ""),
	}
	sess := session.Must(session.NewSession(config))
	downloader := s3manager.NewDownloader(sess)

	/*downloader := s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
	     d.PartSize = 64 * 1024 * 1024 // 64MB per part
	})*/
	fmt.Printf("Opening File:%s\n",filepath2+mgr.S3.PathSeparator+filename)
	downloadedFile, err := os.Create(filepath2+mgr.S3.PathSeparator+filename)
	if err != nil {
		fmt.Printf("Failed to open file for download:%v\n",err)
		return err
	}
	defer downloadedFile.Close()

	objects := []s3manager.BatchDownloadObject {
		{
			Object: &s3.GetObjectInput {
				Bucket: aws.String(mgr.S3.Bucket),
				Key: aws.String(key),
			},
			Writer: downloadedFile,
		},
	}

	iter := &s3manager.DownloadObjectsIterator{Objects: objects}
	if err := downloader.DownloadWithIterator(aws.BackgroundContext(), iter); err != nil {
		return err
	}
	return nil
}