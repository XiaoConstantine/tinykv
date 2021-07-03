package standalone_storage

import (
	"fmt"
	"testing"
	/*
	 *"github.com/Connor1996/badger"
	 */
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/stretchr/testify/require"
)

func txnSet(t *testing.T, kv *StandAloneStorage, key []byte, val []byte, meta byte) {
	txn := kv.Kv.NewTransaction(true)
	require.NoError(t, txn.SetWithMeta(key, val, meta))
	require.NoError(t, txn.Commit())
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
