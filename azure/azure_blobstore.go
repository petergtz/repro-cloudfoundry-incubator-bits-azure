package azure

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"time"

	bitsgo "github.com/chgeuer/repro-cloudfoundry-incubator-bits-azure/bits-service"
	"github.com/chgeuer/repro-cloudfoundry-incubator-bits-azure/logger"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/go-autorest/autorest"
	"github.com/cenkalti/backoff"
	"github.com/pkg/errors"
)

const (
	maxBlocksPerBlob = 50000
	defaultBlockSize = 50 << 20 // 50 MByte
	maxBlockSize     = 100 << 20
)

type AzureBlobstoreConfig struct {
	ContainerName string `yaml:"container_name"`
	AccountName   string `yaml:"account_name"`
	AccountKey    string `yaml:"account_key"`
	Environment   string
}

func (c *AzureBlobstoreConfig) EnvironmentName() string {
	if c.Environment != "" {
		return c.Environment
	}
	return "AzurePublicCloud"
}

type Blobstore struct {
	containerName  string
	client         storage.BlobStorageClient
	putBlockSize   int64
	maxListResults uint
}

func NewBlobstore(config AzureBlobstoreConfig) *Blobstore {
	return NewBlobstoreWithDetails(config, defaultBlockSize, 5000)
}

// NetworkErrorRetryingSender is a replacement for the storage.DefaultSender.
// storage.DefaultSender does not retry on network errors which is rarely what we want in
// a production system.
type NetworkErrorRetryingSender struct{}

var retryableStatusCodes = []int{
	http.StatusRequestTimeout,      // 408
	http.StatusInternalServerError, // 500
	http.StatusBadGateway,          // 502
	http.StatusServiceUnavailable,  // 503
	http.StatusGatewayTimeout,      // 504
}

// Send is mostly emulating storage.DefaultSender.Send, so most code was copied.
// But we use the backoff library for convenience and retry on errors returned from
// HTTPClient.Do.
func (sender *NetworkErrorRetryingSender) Send(c *storage.Client, req *http.Request) (*http.Response, error) {
	rr := autorest.NewRetriableRequest(req)
	var resp *http.Response

	err := backoff.Retry(func() error {
		err := rr.Prepare()
		if err != nil {
			return backoff.Permanent(err)
		}
		resp, err = c.HTTPClient.Do(rr.Request())
		// We deliberately mark errors *not* as permanent, because an error
		// here means network connectivity or similar. This is different to the
		// storage.DefaultSender which stops on any error.
		if err != nil {
			return err
		}
		if !autorest.ResponseHasStatusCode(resp, retryableStatusCodes...) {
			return backoff.Permanent(err)
		}
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		return nil
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))

	return resp, err
}

func NewBlobstoreWithDetails(config AzureBlobstoreConfig, putBlockSize int64, maxListResults uint) *Blobstore {
	if putBlockSize > (maxBlockSize) {
		logger.Log.Fatalw("putBlockSize must be equal or less than 100 MB", "putBlockSize", putBlockSize)
		return nil
	}
	// environment, e := azure.EnvironmentFromName(config.EnvironmentName())
	// if e != nil {
	// 	logger.Log.Fatalw("Could not get Azure Environment from Name", "error", e, "environment", config.EnvironmentName())
	// }
	// client, e := storage.NewBasicClientOnSovereignCloud(config.AccountName, config.AccountKey, environment)
	client, e := storage.NewClient(config.AccountName, config.AccountKey, "core.windows.net", "2018-03-28", false)
	if e != nil {
		logger.Log.Fatalw("Could not instantiate Azure Basic Client", "error", e)
	}
	client.Sender = &NetworkErrorRetryingSender{}
	return &Blobstore{
		client:         client.GetBlobService(),
		containerName:  config.ContainerName,
		putBlockSize:   putBlockSize,
		maxListResults: maxListResults,
	}
}

func (blobstore *Blobstore) Exists(path string) (bool, error) {
	exists, e := blobstore.client.GetContainerReference(blobstore.containerName).GetBlobReference(path).Exists()
	if e != nil {
		return false, errors.Wrapf(e, "Failed to check for %v/%v", blobstore.containerName, path)
	}
	return exists, nil
}

func (blobstore *Blobstore) Get(path string) (body io.ReadCloser, err error) {
	logger.Log.Debugw("Get", "bucket", blobstore.containerName, "path", path)

	reader, e := blobstore.client.GetContainerReference(blobstore.containerName).GetBlobReference(path).Get(nil)
	if e != nil {
		return nil, blobstore.handleError(e, "Path %v", path)
	}
	return reader, nil
}

func (blobstore *Blobstore) GetOrRedirect(path string) (body io.ReadCloser, redirectLocation string, err error) {
	signedUrl, e := blobstore.client.GetContainerReference(blobstore.containerName).GetBlobReference(path).GetSASURI(storage.BlobSASOptions{
		BlobServiceSASPermissions: storage.BlobServiceSASPermissions{Read: true},
		SASOptions:                storage.SASOptions{Expiry: time.Now().Add(time.Hour)},
	})
	return nil, signedUrl, e
}

func (blobstore *Blobstore) getBlobProps(path string) (exists bool, contentLength int64, contentMD5 string, err error) {
	blob := blobstore.client.GetContainerReference(blobstore.containerName).GetBlobReference(path)
	err = blob.GetProperties(&storage.GetBlobPropertiesOptions{})
	if err != nil {
		exists = false
		contentLength = -1
		contentMD5 = ""

		if azse, ok := err.(storage.AzureStorageServiceError); ok && azse.StatusCode == 404 && strings.Contains(azse.Code, "blob does not exist") {
			err = nil
		}
		return
	}
	exists = true
	contentLength = blob.Properties.ContentLength
	contentMD5 = blob.Properties.ContentMD5
	return
}

func uploadIsRequired(src io.ReadSeeker, blobContentLength int64, blobContentMD5 string) (uploadRequired bool) {
	inputLen, err := src.Seek(0, io.SeekEnd)
	defer src.Seek(0, io.SeekStart)
	if err != nil {
		uploadRequired = true
		return
	}
	if inputLen != blobContentLength {
		uploadRequired = true
		return
	}
	if blobContentMD5 == "" {
		uploadRequired = true
		return
	}
	src.Seek(0, io.SeekStart)
	hash := md5.New()
	io.Copy(hash, src)
	localResourceMD5 := base64.StdEncoding.EncodeToString(hash.Sum(nil))

	uploadRequired = blobContentMD5 != localResourceMD5
	return
}

func (blobstore *Blobstore) Put(path string, src io.ReadSeeker) error {
	putRequestID := rand.Int63()
	l := logger.Log.With("put-request-id", putRequestID)

	exists, contentLength, contentMD5, err := blobstore.getBlobProps(path)
	if err != nil {
		return errors.Wrapf(err, "Could not reach Azure blob storage. path: %v, put-request-id: %v", path, putRequestID)
	}
	l.Debugw("Put", "bucket", blobstore.containerName, "path", path, "exists", exists, "contentLength", contentLength, "contentMD5", contentMD5)

	uploadRequired := uploadIsRequired(src, contentLength, contentMD5)
	l.Infow("Put", "bucket", blobstore.containerName, "path", path, "uploadRequired", uploadRequired)
	if !uploadRequired {
		return nil
	}

	//
	// Upload strategy:
	// It can happen that multiple clients request uploads into the same blob.
	// To prevent collisions, each client uploads to a unique file first, then copies to the final blob, and deletes the original upload.
	//
	temporaryPath := fmt.Sprintf("%s.__upload__%d", path, putRequestID)

	l.Debugw("Put", "bucket", blobstore.containerName, "path", path, "temporaryPath", temporaryPath)

	temporaryBlob := blobstore.client.GetContainerReference(blobstore.containerName).GetBlobReference(temporaryPath)
	overallMD5 := md5.New()
	uncommittedBlocksList := make([]storage.Block, 0)
	for i, eof := 0, false; !eof; {
		if i >= maxBlocksPerBlob {
			return errors.Errorf("block blob cannot have more than 50,000 blocks. path: %v, put-request-id: %v", temporaryPath, putRequestID)
		}
		blockID := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%05d", i)))

		data := make([]byte, blobstore.putBlockSize)
		t1 := time.Now()
		numBytesRead, e := src.Read(data)
		t2 := time.Now()
		if numBytesRead > 0 {
			l.Debugw("Put",
				"disk-read-bytes", numBytesRead,
				"disk-read-duration", t2.Sub(t1).Seconds(),
				"disk-read-throughput", fmt.Sprintf("%.1f byte/sec", float64(numBytesRead)/t2.Sub(t1).Seconds()))
		}

		if e != nil {
			if e.Error() == "EOF" {
				eof = true
			} else {
				return errors.Wrapf(e, "put block failed. path: %v, put-request-id: %v", path, putRequestID)
			}
		}
		if numBytesRead == 0 {
			if eof {
				break
			} else {
				l.Errorf("Empty read", "block-index", i, "block-id", blockID, "is-eof", eof)
				continue
			}
		}
		l.Debugw("PutBlock", "block-index", i, "block-id", blockID, "block-size", numBytesRead, "is-eof", eof)

		bytesToUpload := data[:numBytesRead]
		blockMD5 := getMD5(bytesToUpload)
		overallMD5.Write(bytesToUpload)

		e = backoff.RetryNotify(func() error {
			// return blob.PutBlock(blockID, data[:numBytesRead], &storage.PutBlockOptions{LeaseID: lease.LeaseID})
			t1 := time.Now()
			eInner := temporaryBlob.PutBlock(blockID, bytesToUpload, &storage.PutBlockOptions{ContentMD5: blockMD5})
			t2 := time.Now()
			l.Debugw("Put",
				"azure-upload-bytes", numBytesRead,
				"azure-upload-duration", t2.Sub(t1).Seconds(),
				"azure-upload-throughput", fmt.Sprintf("%.1f byte/sec", float64(numBytesRead)/t2.Sub(t1).Seconds()))
			return eInner
		}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 2), func(e error, d time.Duration) {
			if azse, ok := e.(storage.AzureStorageServiceError); ok {
				l.Infow("Retry PutBlock", "block-index", i, "block-id", blockID, "block-size", numBytesRead, "is-eof", eof, "error", e, "x-ms-request-id", azse.RequestID)
			} else {
				l.Infow("Retry PutBlock", "block-index", i, "block-id", blockID, "block-size", numBytesRead, "is-eof", eof, "error", e)
				// blobstore.metricsService.SendCounterMetric("put-block-retry", 1)
			}
		})
		if e != nil {
			if azse, ok := e.(storage.AzureStorageServiceError); ok {
				return errors.Wrapf(e, "PutBlock() failed. path: %v, temporaryPath: %v, put-request-id: %v, x-ms-request-id: %s", path, temporaryPath, putRequestID, azse.RequestID)
			} else {
				return errors.Wrapf(e, "PutBlock() failed. path: %v, temporaryPath: %v, put-request-id: %v", path, temporaryPath, putRequestID)
			}
		}
		uncommittedBlocksList = append(uncommittedBlocksList, storage.Block{
			ID:     blockID,
			Status: storage.BlockStatusUncommitted,
		})
		i++
	}

	debugBlockIDAsString := func(l []storage.Block) string {
		length := len(l)
		if length == 0 {
			return "[] (empty?!?)"
		} else if length == 1 {
			return fmt.Sprintf("[%v] (%d blocks overall)", l[0].ID, length)
		} else if length == 2 {
			return fmt.Sprintf("[%v, %v] (%d blocks overall)", l[0].ID, l[length-1].ID, length)
		} else {
			return fmt.Sprintf("[%v, ..., %v] (%d blocks overall)", l[0].ID, l[length-1].ID, length)
		}
	}
	l.Debugw("PutBlockList", "uncommitted-block-list", debugBlockIDAsString(uncommittedBlocksList))
	putListErr := backoff.RetryNotify(func() error {
		// return blob.PutBlockList(uncommittedBlocksList, &storage.PutBlockListOptions{LeaseID: lease.LeaseID})
		return temporaryBlob.PutBlockList(uncommittedBlocksList, &storage.PutBlockListOptions{})
	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 2), func(error, time.Duration) {
		l.Infow("Retry PutBlockList", "uncommitted-block-list", debugBlockIDAsString(uncommittedBlocksList))
		// blobstore.metricsService.SendCounterMetric("put-block-list-retry", 1)
	})
	if putListErr != nil {
		if azse, ok := putListErr.(storage.AzureStorageServiceError); ok {
			return errors.Wrapf(putListErr, "PutBlockList() failed. path: %v, put-request-id: %v, x-ms-request-id: %s", path, putRequestID, azse.RequestID)
		} else {
			return errors.Wrapf(putListErr, "PutBlockList() failed. path: %v, put-request-id: %v", path, putRequestID)
		}
	}

	temporaryBlob.Properties.ContentMD5 = base64.StdEncoding.EncodeToString(overallMD5.Sum(nil))
	l.Infow("Set ContentMD5", "ContentMD5", temporaryBlob.Properties.ContentMD5)
	if e := temporaryBlob.SetProperties(&storage.SetBlobPropertiesOptions{}); e != nil {
		return errors.Wrapf(e, "Could not set Content-MD5: %v, path: %v, put-request-id: %v", blobstore.containerName, path, putRequestID)
	}

	blob := blobstore.client.GetContainerReference(blobstore.containerName).GetBlobReference(path)
	if e := blob.CreateBlockBlob(&storage.PutBlobOptions{}); e != nil {
		if azse, ok := e.(storage.AzureStorageServiceError); ok && azse.StatusCode == http.StatusPreconditionFailed && azse.Code == "LeaseIdMissing" {
			return blobstore.handleError(azse, "Another upload is in progress for blob %s/%s (x-ms-request-id %s)", blobstore.containerName, path, azse.RequestID)
		}
		return errors.Wrapf(e, "CreateBlockBlob() failed. container: %v, path: %v, put-request-id: %v", blobstore.containerName, path, putRequestID)
	}

	lease := blobstore.newBlobLease(-1*time.Second, blob) // -1 is infinity
	if err := lease.acquire(); err != nil {
		return err
	}
	defer func() {
		if err := lease.release(); err != nil {
			l.Errorf("Error during lease handling: %s", err)
		}
		l.Debugw("ReleaseLease", "lease-id", lease.LeaseID)
	}()
	l.Debugw("AcquireLease", "lease-id", lease.LeaseID)

	if copyErr := blob.Copy(temporaryBlob.GetURL(), &storage.CopyOptions{Destiny: storage.CopyOptionsConditions{LeaseID: lease.LeaseID}}); copyErr != nil {
		if azse, ok := copyErr.(storage.AzureStorageServiceError); ok {
			return errors.Wrapf(copyErr, "Copy() failed. path: %v, temporaryPath: %v, put-request-id: %v, x-ms-request-id: %s", path, temporaryPath, putRequestID, azse.RequestID)
		} else {
			return errors.Wrapf(copyErr, "Copy() failed. path: %v, temporaryPath: %v, put-request-id: %v", path, temporaryPath, putRequestID)
		}
	} else {
		l.Debugw("Copy", "source-blob", temporaryBlob.GetURL(), "destination-blob", blob.GetURL())
	}

	if deleteTempErr := temporaryBlob.Delete(&storage.DeleteBlobOptions{}); deleteTempErr != nil {
		if azse, ok := deleteTempErr.(storage.AzureStorageServiceError); ok {
			return errors.Wrapf(deleteTempErr, "Delete() failed. temporaryPath: %v, put-request-id: %v, x-ms-request-id: %s", temporaryPath, putRequestID, azse.RequestID)
		} else {
			return errors.Wrapf(deleteTempErr, "Delete() failed. temporaryPath: %v, put-request-id: %v", temporaryPath, putRequestID)
		}
	} else {
		l.Debugw("Delete", "blob", temporaryBlob.GetURL())
	}

	return nil
}

func (blobstore *Blobstore) Copy(src, dest string) error {
	logger.Log.Debugw("Copy in Azure", "container", blobstore.containerName, "src", src, "dest", dest)
	e := blobstore.client.GetContainerReference(blobstore.containerName).GetBlobReference(dest).Copy(
		blobstore.client.GetContainerReference(blobstore.containerName).GetBlobReference(src).GetURL(), nil)

	if e != nil {
		blobstore.handleError(e, "Error while trying to copy src %v to dest %v in bucket %v", src, dest, blobstore.containerName)
	}
	return nil
}

func (blobstore *Blobstore) Delete(path string) error {
	deleted, e := blobstore.client.GetContainerReference(blobstore.containerName).GetBlobReference(path).DeleteIfExists(nil)
	if e != nil {
		return errors.Wrapf(e, "Path %v", path)
	}
	if !deleted {
		return bitsgo.NewNotFoundError()
	}
	return nil
}

func (blobstore *Blobstore) DeleteDir(prefix string) error {
	deletionErrs := []error{}
	marker := ""
	for {
		response, e := blobstore.client.GetContainerReference(blobstore.containerName).ListBlobs(storage.ListBlobsParameters{
			Prefix:     prefix,
			MaxResults: blobstore.maxListResults,
			Marker:     marker,
		})
		if e != nil {
			return errors.Wrapf(e, "Prefix %v", prefix)
		}
		for _, blob := range response.Blobs {
			e = blobstore.Delete(blob.Name)
			if e != nil {
				// if _, isNotFoundError := e.(*bitsgo.NotFoundError); !isNotFoundError {
				deletionErrs = append(deletionErrs, e)
				// }
			}
		}
		if response.NextMarker == "" {
			break
		}
		marker = response.NextMarker
	}

	if len(deletionErrs) != 0 {
		return errors.Errorf("Prefix %v, errors from deleting: %v", prefix, deletionErrs)
	}
	return nil
}

func (blobstore *Blobstore) Sign(resource string, method string, expirationTime time.Time) (signedURL string) {
	var e error
	switch strings.ToLower(method) {
	case "put":
		signedURL, e = blobstore.client.GetContainerReference(blobstore.containerName).GetBlobReference(resource).GetSASURI(storage.BlobSASOptions{
			BlobServiceSASPermissions: storage.BlobServiceSASPermissions{Write: true, Create: true},
			SASOptions:                storage.SASOptions{Expiry: expirationTime},
		})
	case "get":
		signedURL, e = blobstore.client.GetContainerReference(blobstore.containerName).GetBlobReference(resource).GetSASURI(storage.BlobSASOptions{
			BlobServiceSASPermissions: storage.BlobServiceSASPermissions{Read: true},
			SASOptions:                storage.SASOptions{Expiry: expirationTime},
		})
	default:
		panic("The only supported methods are 'put' and 'get'")
	}
	if e != nil {
		panic(e)
	}
	return
}

func (blobstore *Blobstore) handleError(e error, context string, args ...interface{}) error {
	if azse, ok := e.(storage.AzureStorageServiceError); ok && azse.StatusCode == http.StatusNotFound {
		exists, e := blobstore.client.GetContainerReference(blobstore.containerName).Exists()
		if e != nil {
			return errors.Wrapf(e, context, args...)
		}
		if !exists {
			return errors.Errorf("Container does not exist '%v", blobstore.containerName)
		}
		return errors.Errorf("bitsgo.NewNotFoundError()")
	}
	return errors.Wrapf(e, context, args...)
}

func getMD5(content []byte) string {
	checkSumMD5 := md5.New()
	io.Copy(checkSumMD5, bytes.NewReader(content))
	return base64.StdEncoding.EncodeToString(checkSumMD5.Sum(nil))
}

func (blobstore *Blobstore) newBlobLease(duration time.Duration, blob *storage.Blob) *azureBlobLease {
	return &azureBlobLease{
		DoneChannel:  make(chan bool, 1),
		ErrorChannel: make(chan error, 1),
		Duration:     duration,
		Blobstore:    blobstore,
		Blob:         blob,
		LeaseID:      "",
	}
}

type azureBlobLease struct {
	Duration     time.Duration
	Blobstore    *Blobstore
	Blob         *storage.Blob
	LeaseID      string
	DoneChannel  chan bool
	ErrorChannel chan error
}

func (l *azureBlobLease) acquire() error {
	leaseDurationInSeconds := int(l.Duration.Seconds())
	returnedLeaseID, err := l.Blob.AcquireLease(leaseDurationInSeconds, "", nil)
	if err != nil {
		if azse, ok := err.(storage.AzureStorageServiceError); ok && azse.StatusCode == http.StatusPreconditionFailed && azse.Code == "LeaseIdMissing" {
			return l.Blobstore.handleError(azse, "Another upload is in progress for blob %s/%s", l.Blobstore.containerName, l.Blob.Name)
		}
		return err
	}

	l.LeaseID = returnedLeaseID
	go func(l *azureBlobLease) {
		if l.Duration.Seconds() == -1 {
			// for an infinite lease, wait for the DoneChannel and release it
			_ = <-l.DoneChannel
			l.ErrorChannel <- l.Blob.ReleaseLease(l.LeaseID, &storage.LeaseOptions{})
			return
		} else {
			for {
				select {
				case done := <-l.DoneChannel:
					if !done {
					}
					l.ErrorChannel <- l.Blob.ReleaseLease(l.LeaseID, &storage.LeaseOptions{})
					return
				case <-time.After(l.Duration):
					err := l.Blob.RenewLease(l.LeaseID, nil)
					if err != nil {
						fmt.Printf("Error during RenewLease: %s\n", err)
					}
				}
			}
		}
	}(l)

	return nil
}

func (l *azureBlobLease) release() error {
	if l.LeaseID == "" {
		return fmt.Errorf("Lease was not properly created")
	}
	l.DoneChannel <- true
	return <-l.ErrorChannel
}
