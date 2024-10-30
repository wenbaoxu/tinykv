package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(nil)
	if err != nil {
		log.Errorf("failed to get reader: %v", err)
		return nil, err
	}

	data, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawGetResponse{Value: data, NotFound: data == nil}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}

	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: put,
		},
	})

	if err != nil {
		log.Errorf("failed to write data: %v", err)
		return nil, err
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	delete := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}

	err := server.storage.Write(nil, []storage.Modify{
		{
			Data: delete,
		},
	})

	if err != nil {
		log.Errorf("failed to delete data: %v", err)
		return nil, err
	}

	return &kvrpcpb.RawDeleteResponse{}, nil

}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	reader, err := server.storage.Reader(nil)
	if err != nil {
		log.Errorf("failed to get reader: %v", err)
		return nil, err
	}

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	var found uint32

	var result []*kvrpcpb.KvPair
	for iter.Seek(req.StartKey); iter.Valid() && found < req.GetLimit(); iter.Next() {
		var pair kvrpcpb.KvPair
		item := iter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			log.Errorf("failed to copy key %s value, err: %v", string(pair.Key), err)
			return nil, err
		}
		pair.Key = item.KeyCopy(nil)
		pair.Value = value

		result = append(result, &pair)
		found++
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: result,
	}, nil
}
