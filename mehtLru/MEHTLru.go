package mehtLru

import (
	"MEHT/util"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	//"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/syndtr/goleveldb/leveldb"
)

type MCache[K comparable, V any] struct {
	lruCache *lru.Cache[K, V]
	db       *leveldb.DB
}

func New[K comparable, V any](size int, db *leveldb.DB) (*MCache[K, V], error) {
	return NewWithEvict[K, V](size, nil, db)
}

func NewWithEvict[K comparable, V any](size int, onEvicted func(key K, value V), db *leveldb.DB) (c *MCache[K, V], err error) {
	cc, _ := lru.NewWithEvict(size, onEvicted)
	c = &MCache[K, V]{
		lruCache: cc,
		db:       db,
	}
	return
}

func (c *MCache[K, V]) onEvicted(k K, v V) {
	k_, err := util.ToStringE(k)
	if err != nil {
		panic(err)
	}
	v_, err := util.ToStringE(v)
	if err != nil {
		panic(err)
	}
	fmt.Println("onEvicted: \n", k_, v_)
	//if err = c.db.Put([]byte(k_), []byte(v_), nil); err != nil {
	//	panic(err)
	//}
}

func (c *MCache[K, V]) Purge() {
	c.lruCache.Purge()
}

// Add adds a value to the cache. Returns true if an eviction occurred.
func (c *MCache[K, V]) Add(key K, value V) (evicted bool) {
	return c.lruCache.Add(key, value)
}

// Get looks up a key's value from the cache.
func (c *MCache[K, V]) Get(key K) (value V, ok bool) {
	return c.lruCache.Get(key)
}

// Contains checks if a key is in the cache, without updating the
// recent-ness or deleting it for being stale.
func (c *MCache[K, V]) Contains(key K) bool {
	return c.lruCache.Contains(key)
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *MCache[K, V]) Peek(key K) (value V, ok bool) {
	return c.lruCache.Peek(key)
}

// ContainsOrAdd checks if a key is in the cache without updating the
// recent-ness or deleting it for being stale, and if not, adds the value.
// Returns whether found and whether an eviction occurred.
func (c *MCache[K, V]) ContainsOrAdd(key K, value V) (ok, evicted bool) {
	return c.lruCache.ContainsOrAdd(key, value)
}

// PeekOrAdd checks if a key is in the cache without updating the
// recent-ness or deleting it for being stale, and if not, adds the value.
// Returns whether found and whether an eviction occurred.
func (c *MCache[K, V]) PeekOrAdd(key K, value V) (previous V, ok, evicted bool) {
	return c.lruCache.PeekOrAdd(key, value)
}

// Remove removes the provided key from the cache.
func (c *MCache[K, V]) Remove(key K) (present bool) {
	return c.lruCache.Remove(key)
}

// Resize changes the cache size.
func (c *MCache[K, V]) Resize(size int) (evicted int) {
	return c.lruCache.Resize(size)
}

// RemoveOldest removes the oldest item from the cache.
func (c *MCache[K, V]) RemoveOldest() (key K, value V, ok bool) {
	return c.lruCache.RemoveOldest()
}

// GetOldest returns the oldest entry
func (c *MCache[K, V]) GetOldest() (key K, value V, ok bool) {
	return c.lruCache.GetOldest()
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *MCache[K, V]) Keys() []K {
	return c.lruCache.Keys()
}

// Values returns a slice of the values in the cache, from oldest to newest.
func (c *MCache[K, V]) Values() []V {
	return c.lruCache.Values()
}

// Len returns the number of items in the cache.
func (c *MCache[K, V]) Len() int {
	return c.lruCache.Len()
}
