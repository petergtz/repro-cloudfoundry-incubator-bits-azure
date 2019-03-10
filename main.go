package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	bitazu "github.com/chgeuer/repro-cloudfoundry-incubator-bits-azure/azure"
)

func getInput(len int) io.ReadSeeker {
	var str strings.Builder
	for i := 0; i < len; i++ {
		str.WriteString(fmt.Sprintf("%d\n", i))
	}
	return bytes.NewReader([]byte(str.String()))
}

func getFile(name string) io.ReadSeeker {
	f, _ := os.Open(name)
	return f
}

type UploadJob struct {
	name   string
	length int
	file   string
	delay  time.Duration
	wg     *sync.WaitGroup
	error  error
}

func main() {
	config := bitazu.AzureBlobstoreConfig{
		AccountName:   os.Getenv("SAMPLE_STORAGE_ACCOUNT_NAME"),
		AccountKey:    os.Getenv("SAMPLE_STORAGE_ACCOUNT_KEY"),
		ContainerName: "ocirocks2",
	}
	var blockSize int64 = 50 << 20 // 256k
	store := bitazu.NewBlobstoreWithDetails(config, blockSize, 5000)

	upload := func(job *UploadJob) {
		defer job.wg.Done()
		time.Sleep(job.delay)
		var (
			dat  io.ReadSeeker
			name string
		)

		if job.file != "" {
			dat = getFile(job.file)
			name = job.file
		} else {
			dat = getInput(job.length)
			name = "readme.txt"
		}
		if err := store.Put(name, dat); err != nil {
			job.error = err
			return
		}
	}

	var wg sync.WaitGroup
	jobs := make([]*UploadJob, 0)

	for _, f := range os.Args[1:] {
		jobs = append(jobs, &UploadJob{name: f, file: f, delay: 0 * time.Second, wg: &wg})
	}

	// jobs = append(jobs, &UploadJob{name: "early big", length: 1 << 20, delay: 0 * time.Second, wg: &wg})
	// jobs = append(jobs, &UploadJob{name: "early big", file: "main", delay: 0 * time.Second, wg: &wg})
	// jobs = append(jobs, &UploadJob{name: "late small", length: 1 << 10, delay: 3 * time.Second, wg: &wg})

	wg.Add(len(jobs))
	for _, j := range jobs {
		go upload(j)
	}
	wg.Wait()

	for _, j := range jobs {
		if j.error != nil {
			fmt.Printf("Error for job %s: %s", j.name, j.error)
		}
	}
}
