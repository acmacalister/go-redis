package main

const (
	commandGet = "GET"
	commandSet = "SET"
)

func (store *store) Get(key string) *storeItem {
	store.lock.RLock()
	defer store.lock.RUnlock()

	return store.dict[key]
}

func (store *store) Set(key string, val interface{}) {
	store.lock.Lock()
	defer store.lock.Unlock()
	item := storeItem{val: val, redisType: typeString}
	store.dict[key] = &item
}
