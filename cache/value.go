package cache

type Value struct {
	Expired uint32
	Value   []byte
}
