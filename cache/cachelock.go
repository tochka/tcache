package cache

import "sync"

type LockCache struct {
	InnerCache Cache
	mx         sync.RWMutex
}

func (c *LockCache) Remove(key string) {
	c.mx.Lock()
	c.InnerCache.Remove(key)
	c.mx.Unlock()
}

func (c *LockCache) Get(key string) (value Value, ok bool) {
	c.mx.RLock()
	value, ok = c.InnerCache.Get(key)
	c.mx.RUnlock()
	return
}

func (c *LockCache) Add(key string, value Value) {
	c.mx.Lock()
	c.InnerCache.Add(key, value)
	c.mx.Unlock()
}

func (c *LockCache) Keys() (keys []string) {
	c.mx.RLock()
	keys = c.InnerCache.Keys()
	c.mx.RUnlock()
	return
}
