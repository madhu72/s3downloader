package main

/* Copyright (C) Venkateswara Rao Thota - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Venkateswara Rao Thota <thota.v.rao@gmail.com>, Oct 17, 2020
 */

import (
	"fmt"
	"s3downloader/s3picker"
)

func main() {
	fmt.Printf("Running Fetch Documents...\n")
	app := &s3picker.S3Manager{}
	app.LoadYaml("app.yml")
	//app.ShowConfig()
	//app.ShowSecurityDetails()
	app.DownloadDocuments()
}