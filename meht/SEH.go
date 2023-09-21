package meht

// import (
// 	"fmt"
// )

type SEH struct {
	gd            int                // global depth
	ht            map[string]*Bucket // hash table of buckets
	bucketsNumber int                // number of buckets
}
