package lru

import (
	"container/list"

	"github.com/tochka/tcached/cache"
)

func NewCache(maxEntries int) *Cache {
	return &Cache{
		MaxEntries: maxEntries,
		l:          list.New(),
		m:          make(map[string]*list.Element),
	}
}

type entry struct {
	key   string
	value cache.Value
}

type Cache struct {
	MaxEntries int

	l *list.List
	m map[string]*list.Element
}

// Add adds a value to the cache.
func (c *Cache) Add(key string, value cache.Value) {
	if ee, ok := c.m[key]; ok {
		c.l.MoveToFront(ee)
		e := ee.Value.(entry)
		e.value = value
		ee.Value = e
		return
	}
	c.MaxEntries++
	ele := c.l.PushFront(entry{key, value})
	c.m[key] = ele
	if c.MaxEntries != 0 && c.l.Len() < c.MaxEntries {
		c.RemoveOldest()
	}
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) RemoveOldest() {
	ele := c.l.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

func (c *Cache) removeElement(e *list.Element) {
	c.l.Remove(e)
	kv := e.Value.(entry)
	delete(c.m, kv.key)
	c.MaxEntries--
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key string) (value cache.Value, ok bool) {
	if ele, hit := c.m[key]; hit {
		c.l.MoveToFront(ele)
		e := ele.Value.(entry)
		return e.value, true
	}
	return
}

// Remove removes the provided key from the cache.
func (c *Cache) Remove(key string) {
	if ele, hit := c.m[key]; hit {
		c.removeElement(ele)
		c.MaxEntries--
	}
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	return c.l.Len()
}

func (c *Cache) Keys() []string {
	keys := make([]string, len(c.m))
	var indx int
	for k := range c.m {
		keys[indx] = k
		indx++
	}
	return keys
}
