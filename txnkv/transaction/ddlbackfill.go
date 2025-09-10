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
	var reqEndKey []byte
	// var reqStartKey []byte
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

		reqEndKey = endKey
		if len(reqEndKey) == 0 ||
			(len(loc.EndKey) > 0 && bytes.Compare(loc.EndKey, reqEndKey) < 0) {
			reqEndKey = loc.EndKey
		}

		var reqType tikvrpc.CmdType
		var sreq any
		reqType = tikvrpc.CmdDDLBackfillScan
		sreq = &kvrpcpb.DDLBackfillScanRequest{
			StartKey:   startKey,
			EndKey:     reqEndKey,
			Version:    txn.startTS,
		}


		req := tikvrpc.NewRequest(reqType, sreq, kvrpcpb.Context{})
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
			// logutil.BgLogger().Debug("scanner getData failed",
			// 	zap.Stringer("regionErr", regionErr))
			// if err = retry.MayBackoffForRegionError(regionErr, bo); err != nil {
			// 	return err
			// }
			// continue
			return kvrpcpb.DDLBackfillScanResponse{}, err
		}
		if resp.Resp == nil {
			return kvrpcpb.DDLBackfillScanResponse{}, errors.WithStack(tikverr.ErrBodyMissing)
		}

		// var keyErr *kvrpcpb.KeyError
		// var kvPairs []*kvrpcpb.KvPair
		// cmdScanResp := resp.Resp.(*kvrpcpb.DDLBackfillScanResponse)
			// keyErr = cmdScanResp.GetError()
		// kvPairs = cmdScanResp.Pairs

		// err = txn.store.CheckVisibility(txn.startTS)
		// if err != nil {
		// 	return err
		// }

		// When there is a response-level key error, the returned pairs are incomplete.
		// We should resolve the lock first and then retry the same request.
		// if keyErr != nil {
		// 	lock, err := txnlock.ExtractLockFromKeyErr(keyErr)
		// 	if err != nil {
		// 		return err
		// 	}
		// 	locks := []*txnlock.Lock{lock}
		// 	if resolvingRecordToken == nil {
		// 		token := s.snapshot.store.GetLockResolver().RecordResolvingLocks(locks, s.snapshot.version)
		// 		resolvingRecordToken = &token
		// 		defer s.snapshot.store.GetLockResolver().ResolveLocksDone(s.snapshot.version, *resolvingRecordToken)
		// 	} else {
		// 		s.snapshot.store.GetLockResolver().UpdateResolvingLocks(locks, s.snapshot.version, *resolvingRecordToken)
		// 	}
		// 	msBeforeExpired, err := s.snapshot.store.GetLockResolver().ResolveLocks(bo, s.snapshot.version, locks)
		// 	if err != nil {
		// 		return err
		// 	}
		// 	if msBeforeExpired > 0 {
		// 		err = bo.BackoffWithMaxSleepTxnLockFast(int(msBeforeExpired), errors.Errorf("key is locked during scanning"))
		// 		if err != nil {
		// 			return err
		// 		}
		// 	}
		// 	continue
		// }

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
		// if len(kvPairs) < s.batchSize {
		// 	// No more data in current Region. Next getData() starts
		// 	// from current Region's endKey.
		// 	if !s.reverse {
		// 		s.nextStartKey = loc.EndKey
		// 	} else {
		// 		s.nextEndKey = reqStartKey
		// 	}
		// 	if (!s.reverse && (len(loc.EndKey) == 0 || (len(s.endKey) > 0 && kv.CmpKey(s.nextStartKey, s.endKey) >= 0))) ||
		// 		(s.reverse && (len(loc.StartKey) == 0 || (len(s.nextStartKey) > 0 && kv.CmpKey(s.nextStartKey, s.nextEndKey) >= 0))) {
		// 		// Current Region is the last one.
		// 		s.eof = true
		// 	}
		// 	return kvrpcpb.DDLBackfillScanResponse{}, err
		// }
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
