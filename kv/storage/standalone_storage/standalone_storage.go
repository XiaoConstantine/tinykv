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
	// Nothing to do here
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
	return &standAloneStorageReader{s.Kv}, nil
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

type standAloneStorageReader struct {
	Kv *badger.DB
}

func (ssr *standAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(ssr.Kv, cf, key)
}

func (ssr *standAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := ssr.Kv.NewTransaction(true)
	return engine_util.NewCFIterator(cf, txn)
}

func (ssr *standAloneStorageReader) Close() {

}
