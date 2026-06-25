package cache

import (
	"encoding/gob"
	"fmt"
	"io"
	"sync"
	"time"
)

type Item[V any] struct {
	Object     V
	Expiration int64
}

func (item Item[V]) Expired() bool {
	if item.Expiration == 0 {
		return false
	}
	return time.Now().UnixNano() > item.Expiration
}

const (
	NoExpiration      time.Duration = -1
	DefaultExpiration time.Duration = 0
)

type Cache[K comparable, V any] struct {
	*cache[K, V]
}

type cache[K comparable, V any] struct {
	defaultExpiration time.Duration
	items             map[K]Item[V]
	mu                sync.RWMutex
	onEvicted         func(K, V)
	janitor           *janitor[K, V]
}

func (c *cache[K, V]) Set(k K, x V, d time.Duration) {
	var e int64
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}
	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}
	c.mu.Lock()
	c.items[k] = Item[V]{Object: x, Expiration: e}
	c.mu.Unlock()
}

func (c *cache[K, V]) set(k K, x V, d time.Duration) {
	var e int64
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}
	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}
	c.items[k] = Item[V]{Object: x, Expiration: e}
}

func (c *cache[K, V]) SetDefault(k K, x V) {
	c.Set(k, x, DefaultExpiration)
}

func (c *cache[K, V]) Add(k K, x V, d time.Duration) error {
	c.mu.Lock()
	_, found := c.get(k)
	if found {
		c.mu.Unlock()
		return fmt.Errorf("item already exists")
	}
	c.set(k, x, d)
	c.mu.Unlock()
	return nil
}

func (c *cache[K, V]) Replace(k K, x V, d time.Duration) error {
	c.mu.Lock()
	_, found := c.get(k)
	if !found {
		c.mu.Unlock()
		return fmt.Errorf("item doesn't exist")
	}
	c.set(k, x, d)
	c.mu.Unlock()
	return nil
}

func (c *cache[K, V]) UpdateExpiration(k K, d time.Duration) error {
	c.mu.Lock()
	item, found := c.items[k]
	if !found || item.Expired() {
		c.mu.Unlock()
		return fmt.Errorf("item doesn't exist")
	}
	item.Expiration = time.Now().Add(d).UnixNano()
	c.mu.Unlock()
	return nil
}

func (c *cache[K, V]) UpdateExpirationDefault(k K) error {
	return c.UpdateExpiration(k, DefaultExpiration)
}

func (c *cache[K, V]) Get(k K) (V, bool) {
	c.mu.RLock()
	item, found := c.items[k]
	if !found || item.Expired() {
		c.mu.RUnlock()
		var zero V
		return zero, false
	}
	c.mu.RUnlock()
	return item.Object, true
}

func (c *cache[K, V]) GetWithExpiration(k K) (V, time.Time, bool) {
	c.mu.RLock()
	item, found := c.items[k]
	if !found || item.Expired() {
		c.mu.RUnlock()
		var zero V
		return zero, time.Time{}, false
	}
	if item.Expiration > 0 {
		c.mu.RUnlock()
		return item.Object, time.Unix(0, item.Expiration), true
	}
	c.mu.RUnlock()
	return item.Object, time.Time{}, true
}

func (c *cache[K, V]) GetWithUpdateExpiration(k K, d time.Duration) (V, bool) {
	c.mu.Lock()
	item, found := c.items[k]
	if !found || item.Expired() {
		c.mu.Unlock()
		var zero V
		return zero, false
	}
	item.Expiration = time.Now().Add(d).UnixNano()
	c.mu.Unlock()
	return item.Object, true
}

func (c *cache[K, V]) GetWithUpdateExpirationDefault(k K) (V, bool) {
	return c.GetWithUpdateExpiration(k, DefaultExpiration)
}

func (c *cache[K, V]) Exec(k K, e func(V) V) error {
	c.mu.Lock()
	v, found := c.items[k]
	if !found || v.Expired() {
		c.mu.Unlock()
		return fmt.Errorf("item doesn't exist")
	}
	nv := e(v.Object)
	v.Object = nv
	c.items[k] = v
	c.mu.Unlock()
	return nil
}

func (c *cache[K, V]) get(k K) (V, bool) {
	item, found := c.items[k]
	if !found || item.Expired() {
		var zero V
		return zero, false
	}
	return item.Object, true
}

func (c *cache[K, V]) Delete(k K) {
	c.mu.Lock()
	v, evicted := c.delete(k)
	c.mu.Unlock()
	if evicted && c.onEvicted != nil {
		c.onEvicted(k, v)
	}
}

func (c *cache[K, V]) delete(k K) (V, bool) {
	if v, found := c.items[k]; found {
		delete(c.items, k)
		return v.Object, true
	}
	var zero V
	return zero, false
}

type keyAndValue[K comparable, V any] struct {
	key   K
	value V
}

func (c *cache[K, V]) DeleteExpired() {
	var evictedItems []keyAndValue[K, V]
	now := time.Now().UnixNano()
	c.mu.Lock()
	for k, v := range c.items {
		if v.Expiration > 0 && now > v.Expiration {
			ov, evicted := c.delete(k)
			if evicted {
				evictedItems = append(evictedItems, keyAndValue[K, V]{k, ov})
			}
		}
	}
	c.mu.Unlock()
	if c.onEvicted != nil {
		for _, v := range evictedItems {
			c.onEvicted(v.key, v.value)
		}
	}
}

func (c *cache[K, V]) OnEvicted(f func(K, V)) {
	c.mu.Lock()
	c.onEvicted = f
	c.mu.Unlock()
}

func (c *cache[K, V]) Save(w io.Writer) (err error) {
	enc := gob.NewEncoder(w)
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("gob error")
		}
	}()
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, v := range c.items {
		gob.Register(v.Object)
	}
	return enc.Encode(&c.items)
}

func (c *cache[K, V]) Load(r io.Reader) error {
	dec := gob.NewDecoder(r)
	items := map[K]Item[V]{}
	if err := dec.Decode(&items); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range items {
		ov, found := c.items[k]
		if !found || ov.Expired() {
			c.items[k] = v
		}
	}
	return nil
}

func (c *cache[K, V]) Items() map[K]Item[V] {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m := make(map[K]Item[V], len(c.items))
	now := time.Now().UnixNano()
	for k, v := range c.items {
		if v.Expiration > 0 && now > v.Expiration {
			continue
		}
		m[k] = v
	}
	return m
}

func (c *cache[K, V]) ItemCount() int {
	c.mu.RLock()
	n := len(c.items)
	c.mu.RUnlock()
	return n
}

func (c *cache[K, V]) Flush() {
	c.mu.Lock()
	c.items = map[K]Item[V]{}
	c.mu.Unlock()
}

type janitor[K comparable, V any] struct {
	Interval time.Duration
	stop     chan bool
}

func (j *janitor[K, V]) Run(c *cache[K, V]) {
	ticker := time.NewTicker(j.Interval)
	for {
		select {
		case <-ticker.C:
			c.DeleteExpired()
		case <-j.stop:
			ticker.Stop()
			return
		}
	}
}

func runJanitor[K comparable, V any](c *cache[K, V], ci time.Duration) {
	j := &janitor[K, V]{
		Interval: ci,
		stop:     make(chan bool),
	}
	c.janitor = j
	go j.Run(c)
}

func newCache[K comparable, V any](de time.Duration, m map[K]Item[V]) *cache[K, V] {
	if de == 0 {
		de = -1
	}
	return &cache[K, V]{
		defaultExpiration: de,
		items:             m,
	}
}

func newCacheWithJanitor[K comparable, V any](de, ci time.Duration, m map[K]Item[V]) *Cache[K, V] {
	c := newCache[K, V](de, m)
	C := &Cache[K, V]{c}
	if ci > 0 {
		runJanitor(c, ci)
	}
	return C
}

func (c *cache[K, V]) Close() {
	if c.janitor != nil {
		close(c.janitor.stop)
		c.janitor = nil
	}
	var evictedItems []keyAndValue[K, V]
	c.mu.Lock()
	for k := range c.items {
		ov, evicted := c.delete(k)
		if evicted {
			evictedItems = append(evictedItems, keyAndValue[K, V]{k, ov})
		}
	}
	c.mu.Unlock()
	if c.onEvicted != nil {
		for _, v := range evictedItems {
			c.onEvicted(v.key, v.value)
		}
	}
}

func New[K comparable, V any](defaultExpiration, cleanupInterval time.Duration) *Cache[K, V] {
	return newCacheWithJanitor[K, V](
		defaultExpiration,
		cleanupInterval,
		make(map[K]Item[V]),
	)
}

func NewFrom[K comparable, V any](defaultExpiration, cleanupInterval time.Duration, items map[K]Item[V]) *Cache[K, V] {
	return newCacheWithJanitor[K, V](
		defaultExpiration,
		cleanupInterval,
		items,
	)
}
