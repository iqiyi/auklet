package objectserver

import (
	"math/rand"
	"time"

	"go.uber.org/zap"
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
