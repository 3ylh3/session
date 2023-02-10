package redis

import (
	"context"
	"github.com/3ylh3/session"
	"github.com/redis/go-redis/v9"
	"sync"
	"time"
)

type RedisStorageServer struct {
	client *redis.Client
}

type RedisLocker struct {
	client *redis.Client
	key    string
}

func (r *RedisLocker) Lock() {
	for {
		rsp, err := r.client.SetNX(context.Background(), "lock_"+r.key, 1, 0).Result()
		if err != nil && rsp {
			return
		} else {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (r *RedisLocker) Unlock() {
	err := r.client.Del(context.Background(), "lock_"+r.key).Err()
	if err != nil {
		panic(err)
	}
}

func (r *RedisStorageServer) InitServer(url string, password string) error {
	r.client = redis.NewClient(&redis.Options{
		Addr:     url,
		Password: password,
		DB:       0,
		PoolSize: 20,
	})
	return nil
}

func (r *RedisStorageServer) Close() error {
	return r.client.Close()
}

func (r *RedisStorageServer) Put(key string, data string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return r.client.Set(ctx, key, data, 0).Err()
}

func (r *RedisStorageServer) Get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	val, err := r.client.Get(ctx, key).Result()
	return val, err
}

func (r *RedisStorageServer) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return r.client.Del(ctx, key).Err()
}

func (r *RedisStorageServer) ListKeysByPrefix(prefix string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var result []string
	iter := r.client.Scan(ctx, 0, "prefix:"+prefix, 0).Iterator()
	for iter.Next(ctx) {
		result = append(result, iter.Val())
	}
	return result, iter.Err()
}

func (r *RedisStorageServer) NewLocker(key string) (sync.Locker, error) {
	return &RedisLocker{
		client: r.client,
		key:    key,
	}, nil
}

func init() {
	session.Register("redis", &RedisStorageServer{})
}
