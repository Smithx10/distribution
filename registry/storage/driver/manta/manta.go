package manta

import (
	"context"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
	triton "github.com/joyent/triton-go/v2"
	"github.com/joyent/triton-go/v2/authentication"
	"github.com/joyent/triton-go/v2/storage"
)

const (
	driverName = "manta"
)

type driver struct{}

type driverParameters struct {
	path   string `json:"displayName"`
	client *storage.StorageClient
}

func FromParameters(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	mantaPath, ok := parameters["path"]
	if !ok || fmt.Sprint(mantaPath) == "" {
		return nil, fmt.Errorf("No path parameter provided")
	}

	mClient, err := newMantaClient()
	if err != nil {
		return nil, err
	}

	params := driverParameters{
		path:   fmt.Sprint(mantaPath),
		client: mClient,
	}

	return New(params)
}

func newMantaClient() (*storage.StorageClient, error) {
	var (
		signer authentication.Signer
		err    error

		keyID       = triton.GetEnv("KEY_ID")
		accountName = triton.GetEnv("USER")
		keyMaterial = triton.GetEnv("KEY_MATERIAL")
		userName    = triton.GetEnv("USER")
	)

	if keyMaterial == "" {
		input := authentication.SSHAgentSignerInput{
			KeyID:       keyID,
			AccountName: accountName,
			Username:    userName,
		}
		signer, err = authentication.NewSSHAgentSigner(input)
		if err != nil {
			log.Fatalf("error creating SSH agent signer: %v", err)
		}
	} else {
		var keyBytes []byte
		if _, err = os.Stat(keyMaterial); err == nil {
			keyBytes, err = ioutil.ReadFile(keyMaterial)
			if err != nil {
				log.Fatalf("error reading key material from %q: %v",
					keyMaterial, err)
			}
			block, _ := pem.Decode(keyBytes)
			if block == nil {
				log.Fatalf(
					"failed to read key material %q: no key found", keyMaterial)
			}

			if block.Headers["Proc-Type"] == "4,ENCRYPTED" {
				log.Fatalf("failed to read key %q: password protected keys are\n"+
					"not currently supported, decrypt key prior to use",
					keyMaterial)
			}

		} else {
			keyBytes = []byte(keyMaterial)
		}

		input := authentication.PrivateKeySignerInput{
			KeyID:              keyID,
			PrivateKeyMaterial: keyBytes,
			AccountName:        accountName,
			Username:           userName,
		}
		signer, err = authentication.NewPrivateKeySigner(input)
		if err != nil {
			return nil, err
		}
	}

	config := &triton.ClientConfig{
		MantaURL:    triton.GetEnv("URL"),
		AccountName: accountName,
		Username:    userName,
		Signers:     []authentication.Signer{signer},
	}

	client, err := storage.NewClient(config)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func New(params driverParameters) (storagedriver.StorageDriver, error) {

	d := &driver{}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

func (d *driver) Name() string {
	return driverName
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, from string, f storagedriver.WalkFn) error {
	return nil
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	return nil, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, appendParam bool) (storagedriver.FileWriter, error) {
	return nil, storagedriver.PathNotFoundError{Path: path}
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	return nil, nil
}

// Move moves an object stored at sourcePath to destPath, removing the
// original object.
func (d *driver) Move(context context.Context, sourcePath string, destPath string) error {
	return nil
}

// List returns a list of the objects that are direct descendants of the
//given path.
func (d *driver) List(context context.Context, path string) ([]string, error) {
	return nil, nil
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(context context.Context, path string) error {

	return nil
}

// GetContent retrieves the content stored at "path" as a []byte.
// This should primarily be used for small objects.
func (d *driver) GetContent(context context.Context, path string) ([]byte, error) {

	return nil, nil
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driver) PutContent(context context.Context, path string, contents []byte) error {
	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {

	return "", nil
}

func init() {
	factory.Register(driverName, &mantaDriverFactory{})
}

// mantaDriverFactory implements the factory.StorageDriverFactory interface
type mantaDriverFactory struct{}

// Create StorageDriver from parameters
func (factory *mantaDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type Driver struct {
	baseEmbed
}

type baseEmbed struct {
	base.Base
}
