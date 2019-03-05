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

func main() {
	config := bitazu.AzureBlobstoreConfig{
		AccountName:   os.Getenv("SAMPLE_STORAGE_ACCOUNT_NAME"),
		AccountKey:    os.Getenv("SAMPLE_STORAGE_ACCOUNT_KEY"),
		ContainerName: "ocirocks2",
	}
	var blockSize int64 = 1 << 20 // 256 << 10 // 256k
	store := bitazu.NewBlobstoreWithDetails(config, blockSize, 5000)
	// if err := store.PutCOncurrent("foo"); err != nil {
	// 	fmt.Printf("%s", err)

	// }

	// return

	// f := "/mnt/c/Users/chgeuer/Videos/2013.02.20 [MVP2013R1] .NET - MVP - Erlang - Bryan Hunter.mp4"
	// dat, err := os.Open(f)
	// if err != nil {
	// 	fmt.Println("Problem ", err)
	// 	return
	// }

	upload := func(len int, duration time.Duration, wg *sync.WaitGroup) error {
		defer wg.Done()

		time.Sleep(duration)

		dat := getInput(len)
		if err := store.Put("readme.txt", dat); err != nil {
			fmt.Printf("Error %d: %s", len, err)
			return err
		}
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go upload(1<<20, 0*time.Second, &wg)
	go upload(1<<10, 3*time.Second, &wg)
	wg.Wait()
}
