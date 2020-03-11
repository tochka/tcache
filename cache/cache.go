package cache

type Cache interface {
	Remove(key string)
	Get(key string) (value Value, ok bool)
	Add(key string, value Value)
	Keys() []string
}
