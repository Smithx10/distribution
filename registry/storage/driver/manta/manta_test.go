package manta

import (
	"io/ioutil"
	"os"
	"testing"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
	"gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

var mantaDriverConstructor func(path string) (*Driver, error)
var skipManta func() string

func init() {
	root, err := ioutil.TempDir("", "driver-")
	if err != nil {
		panic(err)
	}
	defer os.Remove(root)

	mantaDriverConstructor = func(path string) (*Driver, error) {
		mClient, err := newMantaClient()
		if err != nil {
			return nil, err
		}
		parameters := driverParameters{
			path:   path,
			client: mClient,
		}

		return New(parameters)
	}

	// Skip Manta storage driver tests if environment variable parameters are not provided
	skipManta = func() string {
		if false {

			return "Must set ..."
		}
		return ""
	}

	testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return mantaDriverConstructor(root)
	}, skipManta)
}
