package redis_warpper

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	lru "github.com/hashicorp/golang-lru"
)

//通过Hash自动分桶
//1. 通过Score顺序读取数据，由于有删除操作，记录可能不连续

const (
	bucketMaxCount = 5
	bucketMinCount = 2
)

var ErrDataMustStruct = errors.New("data must struct")
var ErrDataModalNotMatch = errors.New("data and modal not match")
var ErrKeyNotExist = errors.New("key not exist")
var ErrKeyExist = errors.New("key already exist")
var ErrKeyBucketMaxLimit = errors.New("key bucket <=5")
var ErrKeyBucketMinLimit = errors.New("key bucket >=2")

type bucket struct {
	rds *redis.Client
}

var globalMetaCache *lru.Cache

type BucketMeta struct {
	Name        string
	BucketCount uint `redis:"bucket_count"`
	Count       uint `redis:"count"`
}

func init() {
	var err error
	globalMetaCache, err = lru.New(1024)
	if err != nil {
		log.Fatal("redis_warpper: meta cache create failure")
	}
}

func (l *bucket) getIndexKey(name string) string {
	return fmt.Sprintf("%s#bucket_index", name)
}

func (l *bucket) lock(name string) error {
	lkey := l.getIndexKey(name) + "#WLOCK"
	b, err := l.rds.Exists(context.Background(), lkey).Result()
	if err != nil {
		return err
	}
	if b > 0 {
		return errors.New("list locked")
	}
	sr, err := l.rds.SetNX(context.Background(), lkey, 1, time.Second*5).Result()
	if err != nil {
		return err
	}
	if !sr {
		return errors.New("list locked")
	}
	return nil
}

func (l *bucket) unlock(name string) error {
	lkey := l.getIndexKey(name) + "#WLOCK"
	l.rds.Del(context.Background(), lkey).Result()
	return nil
}

func (l *bucket) getBucketKey(data string, idata *BucketMeta) string {
	return fmt.Sprintf("%s#%d", idata.Name, crc32.ChecksumIEEE([]byte(data))%uint32(idata.BucketCount))
}

func (l *bucket) Meta(name string, refresh bool) (*BucketMeta, error) {
	if !refresh {
		v, ok := globalMetaCache.Get(name)
		if ok {
			return v.(*BucketMeta), nil
		}
	}

	b, err := l.Exists(name)
	if err != nil {
		return nil, err
	}
	if !b {
		return nil, ErrKeyNotExist
	}
	ret := &BucketMeta{
		Name:        name,
		BucketCount: 0,
		Count:       0,
	}
	err = l.rds.HGetAll(context.Background(), l.getIndexKey(name)).Scan(ret)
	if err != nil {
		return nil, err
	}
	globalMetaCache.Add(name, ret)
	return ret, nil
}

func (l *bucket) Create(name string, bucketCount uint) error {
	b, err := l.Exists(name)
	if err != nil {
		return err
	}
	if b {
		return ErrKeyExist
	}
	if bucketCount > bucketMaxCount {
		return ErrKeyBucketMaxLimit
	}
	if bucketCount < bucketMinCount {
		return ErrKeyBucketMinLimit
	}
	listKey := l.getIndexKey(name)
	_, err = l.rds.HMSet(context.Background(), listKey, "bucket_count", bucketCount, "count", 0).Result()
	if err != nil {
		return err
	}
	globalMetaCache.Add(name, &BucketMeta{
		Name:        name,
		BucketCount: bucketCount,
		Count:       0,
	})
	return nil
}

func (l *bucket) Size(name string) (int64, error) {
	idata, err := l.Meta(name, true)
	if err != nil {
		return 0, err
	}
	return int64(idata.Count), nil
}

func (l *bucket) Exists(name string) (bool, error) {
	nkey := l.getIndexKey(name)
	e, err := l.rds.Exists(context.Background(), nkey).Result()
	if err != nil {
		return false, err
	}
	return e > 0, nil
}
