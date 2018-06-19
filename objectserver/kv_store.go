package objectserver

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/golang/protobuf/proto"
	rocksdb "github.com/tecbot/gorocksdb"
	"go.uber.org/zap"

	"github.com/iqiyi/auklet/common"
	"github.com/iqiyi/auklet/common/conf"
)

type KVStore struct {
	driveRoot  string
	hashPrefix string
	hashSuffix string
	wopt       *rocksdb.WriteOptions
	ropt       *rocksdb.ReadOptions
	dbs        map[string]*rocksdb.DB

	sync.RWMutex
}

func (s *KVStore) asyncJobPrefix(policy int) string {
	suffix := ""
	if policy != 0 {
		suffix = fmt.Sprintf("-%d", policy)
	}

	return fmt.Sprintf("/async_pending%s", suffix)
}

func (s *KVStore) asyncJobKey(job *KVAsyncJob) string {
	if job == nil {
		return ""
	}
	hash := common.HashObjectName(
		s.hashPrefix, job.Account, job.Container, job.Object, s.hashSuffix)
	prefix := s.asyncJobPrefix(int(job.Policy))
	return fmt.Sprintf(
		"%s/%s/%s-%s", prefix, hash[29:32], hash, job.Headers[common.XTimestamp])
}

func (s *KVStore) openAsyncJobDB(device string) (*rocksdb.DB, error) {
	opts := rocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetWalSizeLimitMb(64)

	p := filepath.Join(s.driveRoot, device, "async-jobs")

	db, err := rocksdb.OpenDb(opts, p)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s *KVStore) getDB(device string) (*rocksdb.DB, error) {
	s.RLock()
	db := s.dbs[device]
	s.RUnlock()
	if db != nil {
		return db, nil
	}

	s.Lock()
	defer s.Unlock()

	db, err := s.openAsyncJobDB(device)
	if err != nil {
		return nil, err
	}

	s.dbs[device] = db
	return db, nil
}

func (s *KVStore) SaveAsyncJob(job *KVAsyncJob) error {
	db, err := s.getDB(job.Device)
	if err != nil {
		glogger.Error("unable to find RocksDB",
			zap.String("device", job.Device), zap.Error(err))
		return err
	}

	key := []byte(s.asyncJobKey(job))
	val, err := proto.Marshal(job)
	if err != nil {
		glogger.Error("unable to marshal async job", zap.Error(err))
		return err
	}

	return db.Put(s.wopt, key, val)
}

func (s *KVStore) ListAsyncJobs(device string, policy int,
	position *KVAsyncJob, num int) ([]*KVAsyncJob, error) {
	db, err := s.getDB(device)
	if err != nil {
		return nil, err
	}

	var jobs []*KVAsyncJob
	iter := db.NewIterator(s.ropt)
	defer iter.Close()

	p := s.asyncJobKey(position)
	if p == "" {
		p = s.asyncJobPrefix(policy)
	}
	for iter.Seek([]byte(p)); iter.Valid() && num > 0; iter.Next() {
		key := string(iter.Key().Data())
		if key == p {
			continue
		}

		job := new(KVAsyncJob)
		if err := proto.Unmarshal(iter.Value().Data(), job); err != nil {
			glogger.Error("unable to unmarshal pending job",
				zap.String("object-key", key), zap.Error(err))
			continue
		}

		jobs = append(jobs, job)
		num--
	}

	return jobs, nil
}

func (s *KVStore) CleanAsyncJob(job *KVAsyncJob) error {
	db, err := s.getDB(job.Device)
	if err != nil {
		return err
	}
	key := []byte(s.asyncJobKey(job))

	return db.Delete(s.wopt, key)
}

func NewKVStore(driveRoot string) *KVStore {
	s := &KVStore{
		driveRoot: driveRoot,
		dbs:       make(map[string]*rocksdb.DB),
		wopt:      rocksdb.NewDefaultWriteOptions(),
		ropt:      rocksdb.NewDefaultReadOptions(),
	}
	var err error
	s.hashPrefix, s.hashSuffix, err = conf.GetHashPrefixAndSuffix()
	if err != nil {
		glogger.Error("unable to find hash prefix/suffix", zap.Error(err))
		return nil
	}

	return s
}
