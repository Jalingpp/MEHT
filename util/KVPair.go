package util

import "fmt"

type KVPair struct {
	key   string
	value string
}

// NewKVPair creates a new KVPair object
func NewKVPair(key string, value string) *KVPair {
	return &KVPair{key, value}
}

// GetKey returns the key of the KVPair
func (kv *KVPair) GetKey() string {
	return kv.key
}

// GetValue returns the value of the KVPair
func (kv *KVPair) GetValue() string {
	return kv.value
}

// SetValue sets the value of the KVPair
func (kv *KVPair) SetValue(value string) {
	kv.value = value
}

// String returns a string representation of the KVPair
func (kv *KVPair) String() string {
	return fmt.Sprintf("%s:%s", kv.key, kv.value)
}

// Equals returns true if the KVPair is equal to the other KVPair
func (kv *KVPair) Equals(other *KVPair) bool {
	return kv.key == other.key && kv.value == other.value
}

// LessThan returns true if the KVPair is less than the other KVPair
func (kv *KVPair) LessThan(other *KVPair) bool {
	return kv.key < other.key
}

// GreaterThan returns true if the KVPair is greater than the other KVPair
func (kv *KVPair) GreaterThan(other *KVPair) bool {
	return kv.key > other.key
}
