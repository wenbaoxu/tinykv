package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	path string
	db   *badger.DB
	// Your Data Here (1).
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	log.SetLevel(log.LOG_LEVEL_NONE)
	// Your Code Here (1).
	return &StandAloneStorage{
		path: conf.DBPath,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.path
	opts.ValueDir = s.path
	db, err := badger.Open(opts)
	if err != nil {
		log.Errorf("failed to open badger db: %v, path: %s", err, s.path)
		return err
	}

	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	if err != nil {
		log.Errorf("failed to close badger db: %v, path: %s", err, s.path)
	}

	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &standAloneReader{
		db: s.db,
		tx: s.db.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	s.db.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			switch m.Data.(type) {
			case storage.Put:
				log.Infof("update %s:%s", engine_util.KeyWithCF(m.Cf(), m.Key()), m.Value())
				if err := txn.Set(engine_util.KeyWithCF(m.Cf(), m.Key()), m.Value()); err != nil {
					log.Errorf("failed to update %s:%s, err %v", engine_util.KeyWithCF(m.Cf(), m.Key()), m.Value(), err)
					return err
				}
			case storage.Delete:
				log.Infof("delete %s", engine_util.KeyWithCF(m.Cf(), m.Key()))
				if err := txn.Delete(engine_util.KeyWithCF(m.Cf(), m.Key())); err != nil {
					log.Errorf("failed to delete %s, err %v", engine_util.KeyWithCF(m.Cf(), m.Key()), err)
					return err
				}
			default:
				log.Errorf("unknown op %v", m.Data)
			}
		}
		return nil
	})
	return nil
}

type standAloneReader struct {
	db *badger.DB
	tx *badger.Txn
}

func (sr *standAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	log.Infof("get %s", engine_util.KeyWithCF(cf, key))
	val, err := engine_util.GetCF(sr.db, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}
func (sr *standAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.tx)
}

func (sr *standAloneReader) Close() {
	log.Panic("not implemented")
}
