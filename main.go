package redis_warpper

import (
	"context"

	"github.com/duke-git/lancet/convertor"
	"github.com/go-redis/redis/v8"
)

type RedisWarp redis.Client

func (r *RedisWarp) GetStruct(key string, dest interface{}) error {
	if !requiredStruct(dest) {
		return ErrDataMustStruct
	}
	e, err := r.Exists(context.Background(), key).Result()
	if err != nil {
		return err
	}
	if e == 0 {
		return redis.Nil
	}

	values, err := r.HGetAll(context.Background(), key).Result()
	if err != nil {
		return err
	}
	dmap := map[string]interface{}{}
	for k, v := range values {
		dmap[k] = v
	}
	return scanMapToStruct(dest, dmap)
}

func (r *RedisWarp) SetStruct(key string, src interface{}) error {
	m, err := convertor.StructToMap(src)
	if err != nil {
		return err
	}
	_, err = r.HSet(context.Background(), key, m).Result()
	if err != nil {
		return err
	}
	return nil
}

func (r *RedisWarp) ExistExt(key string) (bool, error) {
	e, err := r.Exists(context.Background(), key).Result()
	if err != nil {
		return false, err
	}
	return e > 0, nil
}
