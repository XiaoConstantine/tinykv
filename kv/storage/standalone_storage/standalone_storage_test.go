package standalone_storage

import (
	"fmt"
	"os"
	"testing"
	/*
	 *"github.com/Connor1996/badger"
	 */
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func txnSet(t *testing.T, kv *StandAloneStorage, key []byte, val []byte, meta byte) {
	txn := kv.Kv.NewTransaction(true)
	require.NoError(t, txn.SetWithMeta(key, val, meta))
	require.NoError(t, txn.Commit())
}

func Set(s *StandAloneStorage, cf string, key []byte, value []byte) error {
	return s.Write(nil, []storage.Modify{
		{
			Data: storage.Put{
				Cf:    cf,
				Key:   key,
				Value: value,
			},
		},
	})
}

func Get(s *StandAloneStorage, cf string, key []byte) ([]byte, error) {
	reader, err := s.Reader(nil)
	if err != nil {
		return nil, err
	}
	return reader.GetCF(cf, key)
}

func Iter(s *StandAloneStorage, cf string) (engine_util.DBIterator, error) {
	reader, err := s.Reader(nil)
	if err != nil {
		return nil, err
	}
	return reader.IterCF(cf), nil
}

func cleanUpTestData(conf *config.Config) error {
	if conf != nil {
		return os.RemoveAll(conf.DBPath)
	}
	return nil
}

func TestNewStandAloneStorage(t *testing.T) {
	conf := config.NewTestConfig()
	s := NewStandAloneStorage(conf)
	log.Info(s.Kv, s.KvPath)
	// DB instance
	for i := 0; i < 100; i++ {
		txnSet(t, s, []byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)), 0x00)
	}
	add := storage.Put{[]byte("a"), []byte("a1"), "test"}
	del := storage.Delete{[]byte("a"), "test"}
	batch := []storage.Modify{{add}, {del}}

	require.NoError(t, s.Write(nil, batch))

	require.NoError(t, s.Stop())
}

func TestRawGet(t *testing.T) {
	conf := config.NewTestConfig()
	s := NewStandAloneStorage(conf)
	s.Start()
	defer cleanUpTestData(conf)
	defer s.Stop()

	cf := engine_util.CfDefault
	Set(s, cf, []byte{99}, []byte{42})

	val, _ := Get(s, cf, []byte{99})
	assert.Equal(t, []byte{42}, val)
}
