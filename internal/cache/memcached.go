package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	netUrl "net/url"
	"os"
	"sync"
	"time"

	"github.com/IliaW/page-crawler/config"
	"github.com/IliaW/page-crawler/internal"
	"github.com/IliaW/page-crawler/internal/model"
	"github.com/bradfitz/gomemcache/memcache"
)

type CachedClient interface {
	SaveCrawlInfo(string, *model.ProcessorTask)
	DecrementThreshold(string)
	Close()
}

type MemcachedClient struct {
	client *memcache.Client
	cfg    *config.CacheConfig
	mu     sync.Mutex
}

func NewMemcachedClient(cacheConfig *config.CacheConfig) *MemcachedClient {
	slog.Info("connecting to memcached...")
	ss := new(memcache.ServerList)
	err := ss.SetServers(cacheConfig.Servers...)
	if err != nil {
		slog.Error("failed to set memcached servers.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	c := &MemcachedClient{
		client: memcache.NewFromSelector(ss),
		cfg:    cacheConfig,
	}
	slog.Info("pinging the memcached.")
	err = c.client.Ping()
	if err != nil {
		slog.Error("connection to the memcached is failed.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	slog.Info("connected to memcached!")

	return c
}

func (mc *MemcachedClient) SaveCrawlInfo(url string, task *model.ProcessorTask) {
	ttl := mc.cfg.TtlForPage
	if task.Force {
		ttl = time.Minute
	}

	key := internal.HashURL(url)
	if err := mc.set(key, "1", int32((ttl).Seconds())); err != nil {
		slog.Error("failed to save s3 key to cache.", slog.String("key", key),
			slog.String("err", err.Error()))
	}
	slog.Debug("s3 key saved to cache.", slog.String("key", key), slog.Any("url", url))
}

func (mc *MemcachedClient) DecrementThreshold(url string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	slog.Debug("decrementing the threshold.")
	key := mc.generateDomainHash(url)
	_, err := mc.client.Decrement(key, 1)
	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			slog.Debug("cache expired.", slog.String("key", key))
		} else {
			slog.Warn("failed to decrement the threshold.", slog.String("key", key),
				slog.String("err", err.Error()))
		}
	}
}

func (mc *MemcachedClient) Close() {
	slog.Info("closing memcached connection.")
	err := mc.client.Close()
	if err != nil {
		slog.Error("failed to close memcached connection.", slog.String("err", err.Error()))
	}
}

func (mc *MemcachedClient) set(key string, value any, expiration int32) error {
	byteValue, err := json.Marshal(value)
	if err != nil {
		return err
	}
	item := &memcache.Item{
		Key:        key,
		Value:      byteValue,
		Expiration: expiration,
	}

	return mc.client.Set(item)
}

func (mc *MemcachedClient) generateDomainHash(url string) string {
	u, err := netUrl.Parse(url)
	var key string
	if err != nil {
		slog.Error("failed to parse url. Use full url as a key.", slog.String("url", url),
			slog.String("err", err.Error()))
		key = fmt.Sprintf("%s-1m-crawl", internal.HashURL(url))
	} else {
		key = fmt.Sprintf("%s-1m-crawl", internal.HashURL(u.Host))
		slog.Debug(url, slog.String("key:", key))
	}

	return key
}
