package store

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/actors/builtin"
	"github.com/filecoin-project/lotus/chain/types"
	lru "github.com/hashicorp/golang-lru"
	"golang.org/x/xerrors"
	"os"
	"strconv"
	"time"
)

var DefaultChainIndexCacheSize = 32 << 10

func init() {
	if s := os.Getenv("LOTUS_CHAIN_INDEX_CACHE"); s != "" {
		lcic, err := strconv.Atoi(s)
		if err != nil {
			log.Errorf("failed to parse 'LOTUS_CHAIN_INDEX_CACHE' env var: %s", err)
		}
		DefaultChainIndexCacheSize = lcic
	}

}

type ChainIndex struct {
	skipCache *lru.ARCCache

	loadTipSet loadTipSetFunc

	skipLength abi.ChainEpoch
}
type loadTipSetFunc func(context.Context, types.TipSetKey) (*types.TipSet, error)

func NewChainIndex(lts loadTipSetFunc) *ChainIndex {
	sc, _ := lru.NewARC(DefaultChainIndexCacheSize)
	return &ChainIndex{
		skipCache:  sc,
		loadTipSet: lts,
		skipLength: 20,
	}
}

type lbEntry struct {
	ts           *types.TipSet
	parentHeight abi.ChainEpoch
	targetHeight abi.ChainEpoch
	target       types.TipSetKey
}
type LbEntry struct {
	Ts           *types.TipSet
	ParentHeight abi.ChainEpoch
	TargetHeight abi.ChainEpoch
	Target       types.TipSetKey
}

func (ci *ChainIndex) GetTipsetByHeight(ctx context.Context, from *types.TipSet, to abi.ChainEpoch) (*types.TipSet, error) {
	if from.Height()-to <= ci.skipLength {
		return ci.walkBack(ctx, from, to)
	}

	rounded, err := ci.roundDown(ctx, from)
	if err != nil {
		return nil, err
	}

	var ret LbEntry

	found, err := Redis.GetValue(context.TODO(), fmt.Sprintf("lotus.height.%d", to), &ret)
	if to < 800000 {
		if found && ret.Ts != nil{
			log.Warnf("use redis:%v, to=%d found=%v, ts is nil: %v, height=%d",
				useRedis, to, found, ret.Ts == nil, ret.Ts.Height())
		}else{
			log.Warnf("use redis:%v, to=%d found=%v, ts is nil, target height=%d",
				useRedis, to, found, ret.TargetHeight)
		}
	}
	if found && err == nil && ret.Ts != nil && ret.Ts.Height() ==to {
		return ret.Ts, nil
	}

	cur := rounded.Key()
	head := ci.headHeight()

	for {
		cval, ok := ci.skipCache.Get(cur)
		if !ok {
			fc, err := ci.fillCache(ctx, cur, to, head)
			if err != nil {
				return nil, err
			}
			cval = fc
		}

		lbe := cval.(*lbEntry)
		if lbe.ts.Height() == to || lbe.parentHeight < to {
			return lbe.ts, nil
		} else if to > lbe.targetHeight {
			return ci.walkBack(ctx, lbe.ts, to)
		}

		cur = lbe.target
	}
}

func (ci *ChainIndex)headHeight()abi.ChainEpoch{
	if address.CurrentNetwork == 0{
		return abi.ChainEpoch((time.Now().Local().Unix()-1602773040)/30+148888)
	}
	return abi.ChainEpoch((time.Now().Local().Unix()-1624060830)/30+1)
}

func (ci *ChainIndex) GetTipsetByHeightWithoutCache(ctx context.Context, from *types.TipSet, to abi.ChainEpoch) (*types.TipSet, error) {
	return ci.walkBack(ctx, from, to)
}

func (ci *ChainIndex) fillCache(ctx context.Context, tsk types.TipSetKey,to, head abi.ChainEpoch) (*lbEntry, error) {
	ts, err := ci.loadTipSet(ctx, tsk)
	if err != nil {
		return nil, err
	}

	if ts.Height() == 0 {
		return &lbEntry{
			ts:           ts,
			parentHeight: 0,
		}, nil
	}

	// will either be equal to ts.Height, or at least > ts.Parent.Height()
	rheight := ci.roundHeight(ts.Height())

	parent, err := ci.loadTipSet(ctx, ts.Parents())
	if err != nil {
		return nil, err
	}

	rheight -= 1
	if to >head-builtin.EpochsInDay*7{
		rheight -= ci.skipLength
	}
	if rheight < 0 {
		rheight = 0
	}

	var skipTarget *types.TipSet
	if parent.Height() < rheight {
		skipTarget = parent
	} else {
		skipTarget, err = ci.walkBack(ctx, parent, rheight)
		if err != nil {
			return nil, xerrors.Errorf("fillCache walkback: %w", err)
		}
	}

	lbe := &lbEntry{
		ts:           ts,
		parentHeight: parent.Height(),
		targetHeight: skipTarget.Height(),
		target:       skipTarget.Key(),
	}

	if ts.Height() > head-builtin.EpochsInDay*7 {
		ci.skipCache.Add(tsk, lbe)
	} else{
		lbe2 := &LbEntry{
			Ts:           lbe.ts,
			ParentHeight: lbe.parentHeight,
			TargetHeight: lbe.targetHeight,
			Target:       lbe.target,
		}
		err = Redis.SetValue(context.TODO(), fmt.Sprintf("lotus.height.%d", ts.Height()), lbe2, 0)
		if err != nil {
			log.Errorf("store index: set ts=%d to redis err: %s", ts.Height(), err.Error())
		}
	}

	return lbe, nil
}

// floors to nearest skipLength multiple
func (ci *ChainIndex) roundHeight(h abi.ChainEpoch) abi.ChainEpoch {
	return (h / ci.skipLength) * ci.skipLength
}

func (ci *ChainIndex) roundDown(ctx context.Context, ts *types.TipSet) (*types.TipSet, error) {
	target := ci.roundHeight(ts.Height())

	rounded, err := ci.walkBack(ctx, ts, target)
	if err != nil {
		return nil, err
	}

	return rounded, nil
}

func (ci *ChainIndex) walkBack(ctx context.Context, from *types.TipSet, to abi.ChainEpoch) (*types.TipSet, error) {
	if to > from.Height() {
		return nil, xerrors.Errorf("looking for tipset with height greater than start point")
	}

	if to == from.Height() {
		return from, nil
	}

	ts := from

	for {
		pts, err := ci.loadTipSet(ctx, ts.Parents())
		if err != nil {
			return nil, err
		}

		if to > pts.Height() {
			// in case pts is lower than the epoch we're looking for (null blocks)
			// return a tipset above that height
			return ts, nil
		}
		if to == pts.Height() {
			return pts, nil
		}

		ts = pts
	}
}
