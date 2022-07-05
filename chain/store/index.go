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

func (ci *ChainIndex) GetTipsetByHeight(ctx context.Context, from *types.TipSet, to abi.ChainEpoch) (*types.TipSet, error) {
	if from.Height()-to <= ci.skipLength {
		return ci.walkBack(ctx, from, to)
	}

	rounded, err := ci.roundDown(ctx, from)
	if err != nil {
		return nil, err
	}

	var ret types.TipSet

	head := ci.headHeight()
	found, err := Redis.GetValue(context.TODO(), fmt.Sprintf("lotus.epoch.%d", to), &ret)
	if found && err == nil && ret.Height() == to {
		return &ret, nil
	}

	cur := rounded.Key()

	for {
		cval, ok := ci.skipCache.Get(cur)
		if !ok {
			fc, err := ci.fillCache(ctx, cur, head)
			if err != nil {
				return nil, err
			}
			cval = fc
		}

		lbe := cval.(*lbEntry)
		if lbe.ts.Height() == to || lbe.parentHeight < to {
			return lbe.ts, nil
		} else if to > lbe.targetHeight {
			return ci.walkBackToCache(ctx, lbe.ts, to, head)
		}

		cur = lbe.target
	}
}

func (ci *ChainIndex) headHeight() abi.ChainEpoch {
	if address.CurrentNetwork == 0 {
		return abi.ChainEpoch((time.Now().Local().Unix()-1602773040)/30 + 148888)
	}
	return abi.ChainEpoch((time.Now().Local().Unix()-1624060830)/30 + 1)
}

func (ci *ChainIndex) GetTipsetByHeightWithoutCache(ctx context.Context, from *types.TipSet, to abi.ChainEpoch) (*types.TipSet, error) {
	return ci.walkBack(ctx, from, to)
}

func (ci *ChainIndex) fillCache(ctx context.Context, tsk types.TipSetKey, head abi.ChainEpoch) (*lbEntry, error) {
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

	rheight -= ci.skipLength
	if rheight < 0 {
		rheight = 0
	}

	var skipTarget *types.TipSet
	if parent.Height() < rheight {
		skipTarget = parent
	} else {
		skipTarget, err = ci.walkBackToCache(ctx, parent, rheight, head)
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

	ci.skipCache.Add(tsk, lbe)

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

func (ci *ChainIndex) walkBackToCache(ctx context.Context, from *types.TipSet, to, head abi.ChainEpoch) (*types.TipSet, error) {
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
		if pts.Height() < head-builtin.EpochsInDay*7 {
			err = Redis.SetValue(context.TODO(), fmt.Sprintf("lotus.epoch.%d", pts.Height()), pts, 0)
			if err != nil {
				log.Errorf("store index: set ts=%d to redis err: %s", pts.Height(), err.Error())
			}
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
