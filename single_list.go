package redis_warpper

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"strconv"
)

var ErrIteratorEOF = errors.New("end of list")

type SingleList struct {
	rds *redis.Client
	key string
}

type ListIterator struct {
	rds      *redis.Client
	key      string
	min      string
	max      string
	offset   int64
	count    int64
	allCount int64
	reverse  bool
}

func NewSingleList(rds *redis.Client, key string) *SingleList {
	return &SingleList{
		rds: rds,
		key: key,
	}
}

func (l *SingleList) Set(data string, score int64) error {
	z := &redis.Z{
		Score:  float64(score),
		Member: data,
	}
	ctx := context.Background()
	_, err := l.rds.ZAdd(ctx, l.key, z).Result()
	if err != nil {
		return err
	}
	return nil
}

func (l *SingleList) Del(data ...string) error {
	_, err := l.rds.ZRem(context.Background(), l.key, data).Result()
	return err
}

func (l *SingleList) Contains(data string) (bool, error) {
	_, err := l.rds.ZRank(context.Background(), l.key, data).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, err
	}
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	return true, nil
}

// BatchContains 批量判断是否存在
func (l *SingleList) BatchContains(data ...string) ([]string, error) {
	ctx := context.Background()
	p := l.rds.Pipeline()
	existMap := map[string]*redis.IntCmd{}

	for _, v := range data {
		existMap[v] = p.ZRank(ctx, l.key, v)
	}

	_, err := p.Exec(ctx)
	if err != nil && !errors.Is(redis.Nil, err) {
		return nil, err
	}
	var ret []string
	for k, v := range existMap {
		if errors.Is(v.Err(), redis.Nil) {
			continue
		}
		ret = append(ret, k)
	}
	return ret, nil
}

// RangeWithScore reverse = false 从小到大
// true 从大到小
func (l *SingleList) RangeWithScore(min, max, offset, count uint64, reverse bool) (*ListIterator, error) {
	it := &ListIterator{
		rds:     l.rds,
		key:     l.key,
		offset:  int64(offset),
		reverse: reverse,
		count:   int64(count), // 特别注意这个参数，必须设置，否则返回不正常
	}
	if min == 0 {
		it.min = "-inf"
	} else {
		it.min = strconv.FormatUint(min, 10)
	}
	if max == 0 {
		it.max = "+inf"
	} else {
		it.max = strconv.FormatUint(max, 10)
	}
	ctx := context.Background()
	zcount, err := l.rds.ZCount(ctx, l.key, it.min, it.max).Result()
	if err != nil {
		return nil, err
	}
	it.allCount = zcount
	return it, nil
}

func (it *ListIterator) Count() int64 {
	return it.allCount
}

func (it *ListIterator) Next() ([]redis.Z, error) {
	if it.offset >= it.allCount {
		return nil, ErrIteratorEOF
	}
	ctx := context.Background()
	opt := &redis.ZRangeBy{
		Min:    it.min,
		Max:    it.max,
		Offset: it.offset,
		Count:  it.count,
	}
	var ret []redis.Z
	var err error
	if !it.reverse {
		ret, err = it.rds.ZRangeByScoreWithScores(ctx, it.key, opt).Result()
	} else {
		ret, err = it.rds.ZRevRangeByScoreWithScores(ctx, it.key, opt).Result()
	}

	if err != nil {
		return nil, err
	}
	it.offset += it.count
	return ret, nil
}
