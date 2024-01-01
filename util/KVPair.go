package util

import (
	"fmt"
	"strings"
)

// NewKVPair(key string, value string) *KVPair {} : creates a new KVPair object
// ReverseKVPair(kvPair *KVPair) *KVPair {}: 倒置KV
// GetKey() string{} : returns the key of the KVPair
// GetValue() string {}: returns the value of the KVPair
// SetValue(value string) {} : sets the value of the KVPair
// String() string {}: returns the string representation of the KVPair
// Equals(other *KVPair) bool {}: returns true if the KVPair is equal to the other KVPair
// LessThan(other *KVPair) bool {}: returns true if the KVPair is less than the other KVPair
//GreaterThan(other *KVPair) bool {}: returns true if the KVPair is greater than the other KVPair

type KVPair struct {
	key   string
	value string
}

// NewKVPair creates a new KVPair object
func NewKVPair(key string, value string) *KVPair {
	return &KVPair{key, value}
}

// ReverseKVPair 倒置KV
func ReverseKVPair(kvPair KVPair) KVPair {
	return KVPair{kvPair.value, kvPair.key}
}

// AddValue adds a new value to the KVPair, if value is changed, returns true
func (kv *KVPair) AddValue(newValue string) bool {
	if kv.value == "" {
		kv.value = newValue
		return true
	}
	//考虑原value中是否已经包含此newValue
	values := strings.Split(kv.value, ",")
	for i := 0; i < len(values); i++ {
		if values[i] == newValue {
			return false
		}
	}
	kv.value = kv.value + "," + newValue
	return true
}

func (kv *KVPair) DelValue(toDel string) bool {
	if kv.value == "" {
		return false
	}
	values := strings.Split(kv.value, ",")
	for i := 0; i < len(values); i++ {
		if values[i] == toDel {
			values = append(values[:i], values[i+1:]...)
			kv.value = strings.Join(values, ",")
			return true
		}
	}
	return false
}

func (kv *KVPair) DeleteValue(oldValue string) bool {
	values := strings.Split(kv.value, ",")
	//如果value中只有一个值
	if len(values) == 1 {
		if values[0] == oldValue {
			kv.value = ""
			return true
		} else {
			return false
		}
	} else {
		isDel := false
		var posDel int
		//如果value中有多个值
		for posDel = 0; posDel < len(values); posDel++ {
			if values[posDel] == oldValue {
				isDel = true
				break
			}
		}
		if isDel {
			newValues := make([]string, len(values))
			copy(newValues, values)
			newValues = append(newValues[:posDel], newValues[posDel+1:]...)
			kv.value = strings.Join(newValues, ",")
		}
		return isDel
	}
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

func (kv *KVPair) SetKey(key string) {
	kv.key = key
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

func (kv *KVPair) PrintKVPair() {
	fmt.Printf("key=%s , value=%s\n", kv.key, kv.value)
}

type SeKVPair struct {
	Key   string
	Value string
}

func (kv *SeKVPair) GetKey() string {
	return kv.Key
}

func (kv *SeKVPair) GetValue() string {
	return kv.Value
}
