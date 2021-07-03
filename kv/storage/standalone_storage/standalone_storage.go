package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	Kv     *badger.DB
	KvPath string
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	db := engine_util.CreateDB(conf.DBPath, false)
	return &StandAloneStorage{
		Kv:     db,
		KvPath: conf.DBPath,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.Kv.Close(); err != nil {
		return nil
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Delete:
			engine_util.DeleteCF(s.Kv, m.Cf(), m.Key())
		case storage.Put:
			engine_util.PutCF(s.Kv, m.Cf(), m.Key(), m.Value())
		}
	}
	return nil
}
