package objectserver

import (
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/iqiyi/auklet/common"
)

const (
	TEST_DEVICE = "not_existing_dev"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	var err error
	glogger, err = zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
}

func newKVAsyncJob() *KVAsyncJob {
	ts := common.GetTimestamp()
	return &KVAsyncJob{
		Method:    http.MethodPut,
		Account:   fmt.Sprintf("a-%d", rand.Int63()),
		Container: fmt.Sprintf("a-%d", rand.Int63()),
		Object:    fmt.Sprintf("a-%d", rand.Int63()),
		Headers:   map[string]string{common.XTimestamp: ts},
		Device:    TEST_DEVICE,
		Policy:    0,
	}
}

func toGeneric(orig []*KVAsyncJob) []AsyncJob {
	var jobs []AsyncJob
	for _, j := range orig {
		jobs = append(jobs, j)
	}
	return jobs
}

func expctedEqual(t *testing.T, expected, actual []AsyncJob) {
	if expected == nil && actual == nil {
		return
	}

	require.NotNil(t, expected)
	require.NotNil(t, actual)

	sort.Slice(expected, func(i, j int) bool { return expected[i].GetAccount() < expected[j].GetAccount() })
	sort.Slice(actual, func(i, j int) bool { return actual[i].GetAccount() < actual[j].GetAccount() })
	require.Equal(t, expected, actual)
}

func newFSAsyncJob() *FSAsyncJob {
	ts := common.GetTimestamp()
	return &FSAsyncJob{
		Method:    http.MethodPut,
		Account:   fmt.Sprintf("a-%d", rand.Int63()),
		Container: fmt.Sprintf("a-%d", rand.Int63()),
		Object:    fmt.Sprintf("a-%d", rand.Int63()),
		Headers:   map[string]string{common.XTimestamp: ts},
		Device:    TEST_DEVICE,
		Policy:    0,
	}
}
