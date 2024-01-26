package redis_warpper

import (
	"context"
	"errors"

	"github.com/go-redis/redis/v8"
)

//Hash 存储多个对象，Hash本身有个计数字段，key+$$index
//每个对象，有自己的一个Record字段，其它字段该咋存就咋存

const (
	hashIndexSuffix  = "$$index"
	hashObjectSuffix = "$$object"
)

type SingleHash struct {
	rds *redis.Client
	key string
}

func NewSingleHash(rds *redis.Client, key string) *SingleHash {
	return &SingleHash{
		rds: rds,
		key: key,
	}
}

func (l *SingleHash) getHashIndexKey() string {
	return l.key + hashIndexSuffix
}

func (l *SingleHash) getObjectIndexKey(id string) string {
	return id + hashObjectSuffix
}
func (l *SingleHash) getObjectFieldKey(id, field string) string {
	return id + "#" + field
}

func (l *SingleHash) Size() (int64, error) {
	r, err := l.rds.HGet(context.Background(), l.key, l.getHashIndexKey()).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, err
	}
	if errors.Is(redis.Nil, err) {
		return 0, nil
	}
	return r, nil

}

func (l *SingleHash) Del(obj interface{}, id ...string) error {
	ctx := context.Background()
	for _, d := range id {
		fieldNames := getStructFieldName(obj)
		fullField := make([]string, len(fieldNames)+1)
		for idx, n := range fieldNames {
			fullField[idx] = l.getObjectFieldKey(d, n)
		}
		//最后加入对象的Index字段
		fullField[len(fullField)-1] = l.getObjectIndexKey(d)
		//原子操作，确保削减数量
		// _, err := l.rds.HDel(ctx, l.key, l.getHashIndexKey()).Result()
		_, err := l.rds.HIncrBy(ctx, l.key, l.getHashIndexKey(), -1).Result()
		if err != nil {
			return err
		}
		l.rds.HDel(ctx, l.key, fullField...)
	}
	return nil
}

func (l *SingleHash) Set(id string, data interface{}) error {
	if !requiredStruct(data) {
		return ErrDataMustStruct
	}
	ctx := context.Background()
	exist, err := l.rds.HExists(ctx, l.key, l.getObjectIndexKey(id)).Result()
	if err != nil {
		return err
	}
	//重新整理field
	smap, err := structToMap(data)
	dmap := map[string]interface{}{}
	if err != nil {
		return nil
	}
	dmap[l.getObjectIndexKey(id)] = id
	for k, v := range smap {
		dmap[l.getObjectFieldKey(id, k)] = v
	}
	//没有采用HSet批量设置，是要兼容Pika。HMSet 主流会被丢弃，Pika不支持HSet设置多个值
	pipe := l.rds.Pipeline()
	for k, v := range dmap {
		pipe.HSet(ctx, l.key, k, v)
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}
	if !exist {
		l.rds.HIncrBy(ctx, l.key, l.getHashIndexKey(), 1)
	}

	return err
}

func (l *SingleHash) SetField(id, field string, data interface{}) error {

	ctx := context.Background()
	dmap := map[string]interface{}{}
	dmap[l.getObjectFieldKey(id, field)] = data
	dmap[l.getObjectIndexKey(id)] = id //增加固定field
	//没有采用HSet批量设置，是要兼容Pika。HMSet 主流会被丢弃，Pika不支持HSet设置多个值
	pipe := l.rds.Pipeline()
	for k, v := range dmap {
		pipe.HSet(ctx, l.key, k, v)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}
	return err
}

func (l *SingleHash) Incr(id, field string, incr int64) error {
	ctx := context.Background()
	pipe := l.rds.Pipeline()
	//确保ID的记录存在
	pipe.HSet(ctx, l.key, l.getObjectIndexKey(id), id)
	pipe.HIncrBy(ctx, l.key, l.getObjectFieldKey(id, field), incr).Result()
	_, err := pipe.Exec(ctx)
	return err
}

func (l *SingleHash) GetField(id, field string) (string, error) {
	ctx := context.Background()
	ex, err := l.Contains(id)
	if err != nil {
		return "", err
	}
	if !ex {
		return "", ErrKeyNotExist
	}
	r, err := l.rds.HGet(ctx, l.key, l.getObjectFieldKey(id, field)).Result()
	if err != nil {
		return "", err
	}
	return r, nil
}

func (l *SingleHash) Contains(id string) (bool, error) {
	ctx := context.Background()
	r, err := l.rds.HExists(ctx, l.key, l.getObjectIndexKey(id)).Result()
	return r, err
}

func (l *SingleHash) Scan(modal interface{}, id string) error {
	if !requiredStruct(modal) {
		return ErrDataMustStruct
	}
	ctx := context.Background()
	s, err := l.rds.HExists(ctx, l.key, l.getObjectIndexKey(id)).Result()
	if err != nil {
		return err
	}
	if !s {
		//指定ID的数据不存在
		return ErrKeyNotExist
	}
	fieldNames := getStructFieldName(modal)
	//重新整理字段名称为存储用到的完整字段名(id+fieldname)
	hashFields := make([]string, len(fieldNames))
	for i := 0; i < len(fieldNames); i++ {
		hashFields[i] = l.getObjectFieldKey(id, fieldNames[i])
	}
	vals, err := l.rds.HMGet(ctx, l.key, hashFields...).Result()
	if err != nil {
		return err
	}
	dmap := map[string]interface{}{}
	for i := 0; i < len(fieldNames); i++ {
		dmap[fieldNames[i]] = vals[i]
	}
	err = scanMapToStruct(modal, dmap)
	if err != nil {
		return err
	}
	return nil
}

func (l *SingleHash) Range(modal interface{}, id ...string) (map[string]interface{}, error) {
	if !requiredStruct(modal) {
		return nil, ErrDataMustStruct
	}
	ctx := context.Background()
	ret := map[string]interface{}{}
	for _, d := range id {
		s, err := l.rds.HExists(ctx, l.key, l.getObjectIndexKey(d)).Result()
		if err != nil {
			return nil, err
		}
		if !s {
			//指定ID的数据不存在
			continue
		}
		inst := newModalInst(modal)
		//解析出字段值，根据字段值查找Field，再赋值
		fieldNames := getStructFieldName(inst)
		//重新整理字段名称为存储用到的完整字段名(id+fieldname)
		hashFields := make([]string, len(fieldNames))
		for i := 0; i < len(fieldNames); i++ {
			hashFields[i] = l.getObjectFieldKey(d, fieldNames[i])
		}
		vals, err := l.rds.HMGet(ctx, l.key, hashFields...).Result()
		if err != nil {
			return nil, err
		}
		dmap := map[string]interface{}{}
		for i := 0; i < len(fieldNames); i++ {
			dmap[fieldNames[i]] = vals[i]
		}
		err = scanMapToStruct(inst, dmap)
		if err != nil {
			return nil, err
		}
		ret[d] = inst
	}
	return ret, nil
}
