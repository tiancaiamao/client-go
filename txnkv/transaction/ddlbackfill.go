package transaction

import (
	"bytes"
	"context"
	
	"github.com/pkg/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/config/retry"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func (txn *KVTxn) BackfillScan(startKey []byte, endKey []byte, batchSize int) (kvrpcpb.DDLBackfillScanResponse, error) {
	sender := locate.NewRegionRequestSender(txn.store.GetRegionCache(), txn.store.GetTiKVClient(), txn.store.GetOracle())
	var loc *locate.KeyLocation
	// var resolvingRecordToken *int
	var err error
	// the states in request need to keep when retry request.
	var readType string

	bo := retry.NewBackoffer(context.Background(), 20000)
	for {
		loc, err = txn.store.GetRegionCache().LocateKey(bo, startKey)
		if err != nil {
			return kvrpcpb.DDLBackfillScanResponse{}, err
		}

		if len(endKey) == 0 ||
			(len(loc.EndKey) > 0 && bytes.Compare(loc.EndKey, endKey) < 0) {
			endKey = loc.EndKey
		}

		sreq := &kvrpcpb.DDLBackfillScanRequest{
			StartKey:   startKey,
			EndKey:     endKey,
			Limit: uint32(batchSize),
			Version:    txn.startTS,
		}
		req := tikvrpc.NewRequest(tikvrpc.CmdDDLBackfillScan, sreq, kvrpcpb.Context{})
		if readType != "" {
			req.ReadType = readType
			req.IsRetryRequest = true
		}
		resp, _, err := sender.SendReq(bo, req, loc.Region, client.ReadTimeoutMedium)
		if err != nil {
			return kvrpcpb.DDLBackfillScanResponse{}, err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return kvrpcpb.DDLBackfillScanResponse{}, err
		}
		readType = req.ReadType
		if regionErr != nil {
			// Leave the region error for caller to retry.
			return kvrpcpb.DDLBackfillScanResponse{
				RegionError: regionErr,
			}, err
		}
		if resp.Resp == nil {
			return kvrpcpb.DDLBackfillScanResponse{}, errors.WithStack(tikverr.ErrBodyMissing)
		}
		err = txn.store.CheckVisibility(txn.startTS)
		if err != nil {
			return err
		}

		// var keyErr *kvrpcpb.KeyError
		// var kvPairs []*kvrpcpb.KvPair
		cmdResp := resp.Resp.(*kvrpcpb.DDLBackfillScanResponse)
		// keyErr := cmdResp.GetError()
		kvPairs := cmdResp.Pairs

		// Check if kvPair contains error, it should be a Lock.
		// for _, pair := range kvPairs {
			// if keyErr := pair.GetError(); keyErr != nil && len(pair.Key) == 0 {
			// 	lock, err := txnlock.ExtractLockFromKeyErr(keyErr)
			// 	if err != nil {
			// 		return kvrpcpb.DDLBackfillScanResponse{}, err
			// 	}
			// 	pair.Key = lock.Key
			// }
		// }

		// s.cache, s.idx = kvPairs, 0

		if len(kvPairs) < batchSize {
			// No more data in current Region. Next getData() starts
			// from current Region's endKey.
			if !s.reverse {
				s.nextStartKey = loc.EndKey
			} else {
				s.nextEndKey = reqStartKey
			}
			if (!s.reverse && (len(loc.EndKey) == 0 || (len(s.endKey) > 0 && kv.CmpKey(s.nextStartKey, s.endKey) >= 0))) ||
				(s.reverse && (len(loc.StartKey) == 0 || (len(s.nextStartKey) > 0 && kv.CmpKey(s.nextStartKey, s.nextEndKey) >= 0))) {
				// Current Region is the last one.
				s.eof = true
			}
			return kvrpcpb.DDLBackfillScanResponse{}, err
		}

		// next getData() starts from the last key in kvPairs (but skip
		// it by appending a '\x00' to the key). Note that next getData()
		// may get an empty response if the Region in fact does not have
		// more data.
		// lastKey := kvPairs[len(kvPairs)-1].GetKey()
		// if !s.reverse {
		// 	s.nextStartKey = kv.NextKey(lastKey)
		// } else {
		// 	s.nextEndKey = lastKey
		// }
		return kvrpcpb.DDLBackfillScanResponse{}, err
	}
}

func (txn *KVTxn) BackfillCommit() (kvrpcpb.DDLBackfillCommitResponse, error) {
	return kvrpcpb.DDLBackfillCommitResponse{}, nil
}
