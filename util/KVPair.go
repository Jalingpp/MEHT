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
	Key   string
	Value string
}

// NewKVPair creates a new KVPair object
func NewKVPair(key string, value string) *KVPair {
	return &KVPair{key, value}
}

// ReverseKVPair 倒置KV
func ReverseKVPair(kvPair KVPair) KVPair {
	return KVPair{Key: kvPair.Value, Value: kvPair.Key}
}

// AddValue adds a new value to the KVPair, if value is changed, returns true
func (kv *KVPair) AddValue(newValue string) bool {
	if kv.Value == "" {
		kv.Value = newValue
		return true
	}
	//考虑原value中是否已经包含此newValue
	values := strings.Split(kv.Value, ",")
	for i := 0; i < len(values); i++ {
		if values[i] == newValue {
			return false
		}
	}
	kv.Value = kv.Value + "," + newValue
	return true
}

func (kv *KVPair) DelValue(toDel string) bool {
	if kv.Value == "" {
		return false
	}
	values := strings.Split(kv.Value, ",")
	for i := 0; i < len(values); i++ {
		if values[i] == toDel {
			values = append(values[:i], values[i+1:]...)
			kv.Value = strings.Join(values, ",")
			return true
		}
	}
	return false
}

func (kv *KVPair) DeleteValue(oldValue string) bool {
	values := strings.Split(kv.Value, ",")
	//如果value中只有一个值
	if len(values) == 1 {
		if values[0] == oldValue {
			kv.Value = ""
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
			kv.Value = strings.Join(newValues, ",")
		}
		return isDel
	}
}

// GetKey returns the key of the KVPair
func (kv *KVPair) GetKey() string {
	return kv.Key
}

// GetValue returns the value of the KVPair
func (kv *KVPair) GetValue() string {
	return kv.Value
}

// SetValue sets the value of the KVPair
func (kv *KVPair) SetValue(value string) {
	kv.Value = value
}

func (kv *KVPair) SetKey(key string) {
	kv.Key = key
}

// String returns a string representation of the KVPair
func (kv *KVPair) String() string {
	return fmt.Sprintf("%s:%s", kv.Key, kv.Value)
}

// Equals returns true if the KVPair is equal to the other KVPair
func (kv *KVPair) Equals(other *KVPair) bool {
	return kv.Key == other.Key && kv.Value == other.Value
}

// LessThan returns true if the KVPair is less than the other KVPair
func (kv *KVPair) LessThan(other *KVPair) bool {
	return kv.Key < other.Key
}

// GreaterThan returns true if the KVPair is greater than the other KVPair
func (kv *KVPair) GreaterThan(other *KVPair) bool {
	return kv.Key > other.Key
}

func (kv *KVPair) PrintKVPair() {
	fmt.Printf("key=%s , value=%s\n", kv.Key, kv.Value)
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
