package cache

import (
	"log"
	"order_service/internal/model"
	"sync"
	"time"
)

type Cache struct {
	sync.RWMutex
	items map[string]Item
}

type Item struct {
	Value   model.OrderInfo
	Created time.Time
}

func New() *Cache {
	items := make(map[string]Item)
	cache := Cache{
		items: items,
	}
	return &cache
}

func (c *Cache) Set(key string, value model.OrderInfo) {
	c.Lock()
	defer c.Unlock()

	c.items[key] = Item{
		Value:   value,
		Created: time.Now(),
	}

	log.Println("success caching data with key: ", key)

}

func (c *Cache) Get(key string) (any, bool) {
	c.RLock()
	defer c.RUnlock()

	item, found := c.items[key]
	if item.Value.OrderUid != "" && !found {
		return nil, false
	}

	return item.Value, true
}
