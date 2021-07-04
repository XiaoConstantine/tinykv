package standalone_storage

import (
	"os"
	"testing"
	/*
	 *"github.com/Connor1996/badger"
	 */
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/stretchr/testify/assert"
)

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

func Delete(s *StandAloneStorage, cf string, key []byte) error {
	return s.Write(nil, []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  cf,
				Key: key,
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

func TestRawGetNotFound(t *testing.T) {
	conf := config.NewTestConfig()
	s := NewStandAloneStorage(conf)
	s.Start()
	defer cleanUpTestData(conf)
	defer s.Stop()
	cf := engine_util.CfDefault
	val, _ := Get(s, cf, []byte{99})
	assert.Nil(t, val)
}

func TestRawDelete(t *testing.T) {
	conf := config.NewTestConfig()
	s := NewStandAloneStorage(conf)
	s.Start()
	defer cleanUpTestData(conf)
	defer s.Stop()
	cf := engine_util.CfDefault
	assert.Nil(t, Set(s, cf, []byte{99}, []byte{42}))

	err := Delete(s, cf, []byte{99})
	assert.Nil(t, err)

	val, _ := Get(s, cf, []byte{99})
	assert.Equal(t, []byte(nil), val)
}

func TestRawScan(t *testing.T) {
	conf := config.NewTestConfig()
	s := NewStandAloneStorage(conf)
	s.Start()
	defer cleanUpTestData(conf)
	defer s.Stop()
	cf := engine_util.CfDefault

	Set(s, cf, []byte{1}, []byte{233, 1})
	Set(s, cf, []byte{2}, []byte{233, 2})
	Set(s, cf, []byte{3}, []byte{233, 3})
	Set(s, cf, []byte{4}, []byte{233, 4})
	Set(s, cf, []byte{5}, []byte{233, 5})

	itr, err := Iter(s, cf)
	assert.Nil(t, err)
	expectedKeys := [][]byte{{1}, {2}, {3}, {4}, {5}}
	i := 0
	for itr.Seek([]byte{1}); itr.Valid(); itr.Next() {
		item := itr.Item()
		k := item.Key()
		assert.Equal(t, expectedKeys[i], k)
		i++
	}
}
