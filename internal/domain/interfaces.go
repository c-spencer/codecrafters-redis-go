package domain

import "github.com/codecrafters-io/redis-starter-go/internal/rdb"

type State interface {
	// Return the value for the given key, if it exists.
	// Must guarantee that the value is not expired.
	Get(key string) (*rdb.ValueEntry, bool)
	// Set the value for the given key.
	Set(value *rdb.ValueEntry)
	// Return all keys matching the given pattern.
	Keys(pattern string) []string
	// Delete the value for the given key.
	Delete(key string)

	// Subscribe for changes among a set of keys.
	// The subscription will be automatically cancelled on the first result,
	// or when the timeout is reached. If the timeout is reached, the callback
	// will be called with a nil value.
	Subscribe(keys []string, timeout int, callback func(*rdb.ValueEntry))

	// Return the configuration or state of the server.
	Config() ROMap
	ReplicationInfo() ROMap
}

type ROMap interface {
	Get(key string) (string, bool)
	GetInt(key string) (int, bool)
}
