package etcd

import (
	"context"
	"fmt"
	"github.com/3ylh3/session"
	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"sync"
	"time"
)

type EtcdStorageServer struct {
	client *clientv3.Client
}

func (e *EtcdStorageServer) InitServer(url string, password string) error {
	// 初始化etcd client
	var config = clientv3.Config{
		Endpoints:   []string{url},
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(config)
	if err != nil {
		return err
	}
	e.client = client
	return nil
}

func (e *EtcdStorageServer) Close() error {
	return e.client.Close()
}

func (e *EtcdStorageServer) Put(key string, data string) error {
	kv := clientv3.NewKV(e.client)
	_, err := kv.Put(context.TODO(), key, data, clientv3.WithPrevKV())
	return err
}

func (e *EtcdStorageServer) Get(key string) (string, error) {
	kv := clientv3.NewKV(e.client)
	resp, err := kv.Get(context.TODO(), key)
	if err != nil {
		return "", err
	}
	if 0 == resp.Count {
		return "", fmt.Errorf("not find key:%s\n", key)
	}
	return string(resp.Kvs[0].Value), nil
}

func (e *EtcdStorageServer) Delete(key string) error {
	kv := clientv3.NewKV(e.client)
	_, err := kv.Delete(context.TODO(), key)
	return err
}

func (e *EtcdStorageServer) ListKeysByPrefix(prefix string) ([]string, error) {
	kv := clientv3.NewKV(e.client)
	resp, err := kv.Get(context.TODO(), prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var result []string
	for _, value := range resp.Kvs {
		result = append(result, string(value.Key))
	}
	return result, nil
}

func (e *EtcdStorageServer) NewLocker(key string) (sync.Locker, error) {
	s, err := concurrency.NewSession(e.client, concurrency.WithTTL(1))
	if err != nil {
		return nil, err
	}
	lock := concurrency.NewLocker(s, key)
	return lock, nil
}

func init() {
	session.Register("etcd", &EtcdStorageServer{})
}
