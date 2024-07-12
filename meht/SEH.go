package meht

import (
	"MEHT/mht"
	"MEHT/util"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"strings"
	"sync"
	"time"
)

type SEH struct {
	gd             int      // global depth, initial zero
	rdx            int      // rdx, initial  given
	bucketCapacity int      // capacity of the bucket, initial given
	bucketSegNum   int      // number of segment bits in the bucket, initial given
	ht             sync.Map // hash table of buckets
	bucketsNumber  int      // number of buckets, initial zero
	latch          sync.RWMutex
}

// NewSEH returns a new SEH
func NewSEH(rdx int, bc int, bs int) *SEH {
	return &SEH{0, rdx, bc, bs, sync.Map{}, 0, sync.RWMutex{}}
}

// UpdateSEHToDB 更新SEH到db
func (seh *SEH) UpdateSEHToDB(db *leveldb.DB) {
	seSEH := SerializeSEH(seh)
	if err := db.Put([]byte("seh"), seSEH, nil); err != nil {
		panic(err)
	}
}

// GetBucket 获取bucket，如果内存中没有，从db中读取
func (seh *SEH) GetBucket(bucketKey string, db *leveldb.DB, cache *[]interface{}) *Bucket {
	//任何跳转到此处的函数都已对seh.ht添加了读锁，因此此处不必加锁
	ret_, ok := seh.ht.Load(bucketKey)
	if !ok {
		if len(bucketKey) > 0 {
			return seh.GetBucket(bucketKey[util.ComputeStrideByBase(seh.rdx):], db, cache)
		} else {
			return nil
		}
	}
	ret := ret_.(*Bucket)
	if ret != dummyBucket {
		return ret
	}
	key_ := "bucket" + bucketKey
	if cache != nil {
		targetCache, _ := (*cache)[1].(*lru.Cache[string, *Bucket])
		ret, ok = targetCache.Get(key_)
	}
	if !ok {
		if bucketString, error_ := db.Get([]byte(key_), nil); error_ == nil {
			bucket, _ := DeserializeBucket(bucketString)
			ret = bucket
		}
	}
	seh.ht.Store(bucketKey, ret)
	return ret
}

func (seh *SEH) GetBucketByKeyCon(key string) string {
	stride := util.ComputeStrideByBase(seh.rdx)
	if len(key) < seh.gd {
		key = strings.Repeat("0", seh.gd*stride-len(key)) + key
	}
	in := make(chan string)
	for i := 0; i < len(key)/stride; i++ {
		go func(i int) {
			key_ := key[:i*stride]
			_, ok := seh.ht.Load(key_)
			if ok {
				in <- key_
			}
		}(i)
	}
	return <-in
}

// GetBucketByKey GetBucket returns the bucket with the given key
func (seh *SEH) GetBucketByKey(key string, db *leveldb.DB, cache *[]interface{}) *Bucket {
	//任何跳转到此处的函数都已对seh.ht添加了读锁，因此此处不必加锁
	if seh.gd == 0 {
		return seh.GetBucket("", db, cache)
	}
	var bKey string
	e := seh.gd * util.ComputeStrideByBase(seh.rdx)
	if len(key) >= e {
		bKey = key[len(key)-e:]
	} else {
		bKey = strings.Repeat("0", e-len(key)) + key
	}
	return seh.GetBucket(bKey, db, cache)
	//return seh.GetBucket(seh.GetBucketByKeyCon(key), db, cache)
}

// GetGD returns the global depth of the SEH
func (seh *SEH) GetGD() int {
	return seh.gd
}

// GetHT returns the hash table of the SEH
func (seh *SEH) GetHT() *sync.Map {
	return &seh.ht
}

// GetBucketsNumber returns the number of buckets in the SEH
func (seh *SEH) GetBucketsNumber() int {
	return seh.bucketsNumber
}

// GetValueByKey returns the value of the key-value pair with the given key
func (seh *SEH) GetValueByKey(key string, db *leveldb.DB, cache *[]interface{}) string {
	seh.latch.RLock()
	bucket := seh.GetBucketByKey(key, db, cache)
	seh.latch.RUnlock()
	if bucket == nil {
		return ""
	}
	return bucket.GetValue(key, db, cache)
}

// GetProof returns the proof of the key-value pair with the given key
func (seh *SEH) GetProof(key string, db *leveldb.DB, cache *[]interface{}) (string, [32]byte, *mht.MHTProof) {
	seh.latch.RLock()
	bucket := seh.GetBucketByKey(key, db, cache)
	seh.latch.RUnlock()
	value, segKey, isSegExist, index := bucket.GetValueByKey(key, db, cache, false)
	segRootHash, mhtProof := bucket.GetProof(segKey, isSegExist, index, db, cache)
	return value, segRootHash, mhtProof
}

// Insert inserts the key-value pair into the SEH,返回插入的bucket指针,插入的value,segRootHash,proof
func (seh *SEH) Insert(kvPair util.KVPair, db *leveldb.DB, cache *[]interface{}, isDelete bool) ([][]*Bucket, BucketDelegationCode, *int64, int64) {
	//判断是否为第一次插入
	if seh.bucketsNumber == 0 { //第一次插入一定不是删除操作，因此不用判断isDelete
		//创建新的bucket
		bucket := NewBucket(0, seh.rdx, seh.bucketCapacity, seh.bucketSegNum)
		bucket.Insert(kvPair, db, cache)
		seh.ht.Store("", bucket)
		//更新bucket到db
		bucket.UpdateBucketToDB(db, cache)
		seh.bucketsNumber++
		buckets := make([]*Bucket, 0)
		buckets = append(buckets, bucket)
		return [][]*Bucket{buckets}, DELEGATE, nil, 0
	}
	//不是第一次插入,根据key和GD找到待插入的bucket
	seh.latch.RLock()
	bucket := seh.GetBucketByKey(kvPair.GetKey(), db, cache)
	seh.latch.RUnlock()
	//即使要删除的值在辅助索引上还没有来得及插入，也不会影响删除操作，因为删除操作只需要找到桶并记录这一次删除操作即可
	//后续要删除的值也一定会先经过这个桶去插入，不会说延迟删除的桶和即将插入的桶不会有交集，但是桶分裂时要将删除标记传递下去，保证延后的需删除值在插入时一定会在待插入桶找到删除标记
	if bucket != nil && bucket.latch.TryLock() {
		seh.latch.RLock()
		bucket_ := seh.GetBucketByKey(kvPair.GetKey(), db, cache)
		seh.latch.RUnlock()
		if bucket_ != bucket {
			bucket.latch.Unlock()
			return nil, FAILED, nil, 0
		}
		bucket.PendingNum = 0
		var bucketSs [][]*Bucket
		// 成为被委托者，被委托者保证最多一次性将bucket更新满但不分裂，或者虽然引发桶分裂但不接受额外委托并只插入自己的
		bucket.DelegationLatch.Lock()
		if oldValKvp, ok := bucket.DelegationList[kvPair.GetKey()]; ok {
			newValKvp := util.NewKVPair(oldValKvp.GetKey(), oldValKvp.GetValue())
			if isDelete {
				if isChange := newValKvp.DelValue(kvPair.GetValue()); !isChange { // 删除失败，延迟删除
					if _, ok := bucket.toDelMap[kvPair.GetKey()]; !ok {
						bucket.toDelMap[kvPair.GetKey()] = make(map[string]int)
					}
					bucket.toDelMap[kvPair.GetKey()][kvPair.GetValue()]++
				} else { // 删除成功，更新桶
					bucket.DelegationList[kvPair.GetKey()] = *newValKvp
				}
			} else {
				if bucket.toDelMap[kvPair.GetKey()][kvPair.GetValue()] > 0 { // 如果要插入的值在延迟删除列表中，则延迟删除列表中的计数减一，并跳过插入
					bucket.toDelMap[kvPair.GetKey()][kvPair.GetValue()]--
				} else {
					if isChange := newValKvp.AddValue(kvPair.GetValue()); isChange { // 否则，正常的追加插入
						bucket.DelegationList[kvPair.GetKey()] = *newValKvp
					}
				}
			}
		} else {
			if isDelete {
				value, _, _, _ := bucket.GetValueByKey(kvPair.GetKey(), db, cache, true) //连带旧值一并更新
				newValKvp := util.NewKVPair(kvPair.GetKey(), value)
				if isChange := newValKvp.DelValue(kvPair.GetValue()); !isChange { // 删除失败，延迟删除
					if _, ok := bucket.toDelMap[kvPair.GetKey()]; !ok {
						bucket.toDelMap[kvPair.GetKey()] = make(map[string]int)
					}
					bucket.toDelMap[kvPair.GetKey()][kvPair.GetValue()]++
				} else { // 删除成功，更新桶
					bucket.DelegationList[kvPair.GetKey()] = *newValKvp
				}
			} else {
				if bucket.toDelMap[kvPair.GetKey()][kvPair.GetValue()] > 0 { // 如果要插入的值在延迟删除列表中，则延迟删除列表中的计数减一，并跳过插入
					bucket.toDelMap[kvPair.GetKey()][kvPair.GetValue()]--
				} else {
					value, _, _, _ := bucket.GetValueByKey(kvPair.GetKey(), db, cache, true) //连带旧值一并更新
					newValKvp := util.NewKVPair(kvPair.GetKey(), value)
					if isChange := newValKvp.AddValue(kvPair.GetValue()); isChange {
						bucket.DelegationList[kvPair.GetKey()] = *newValKvp
					}
				}
			}
		}
		bucket.latchTimestamp = time.Now().Unix()
		delegationListCopy := make([]util.KVPair, len(bucket.DelegationList))
		idx := 0
		for _, v := range bucket.DelegationList {
			delegationListCopy[idx] = v
			idx++
		}
		// 先计算PendingNum再清空DelegationList
		// 保证其他线程尝试委托数据时计算PendingNum+len(DelegationList)的数目只会多算不会少算
		// 这样就不会出现实际桶已经满了但是还有线程成功在清空后的DelegationList里委托数据的情况
		bucket.PendingNum = len(bucket.DelegationList)
		bucket.DelegationList = nil
		bucket.DelegationList = make(map[string]util.KVPair)
		bucket.DelegationLatch.Unlock() // 允许其他线程委托自己插入
		//bucket.DelegationLatch.Lock()        // 获得委托锁，正式拒绝所有其他线程的委托
		if bucket.number < bucket.capacity { // 由于插入数目一定不引起桶分裂，顶多插满，因此最后插完的桶就是当前桶
			//fmt.Printf("%p\t", bucket)
			//fmt.Println("No Split Occur when bucket.number < bucket.capacity: ", bucket.number, " ", bucket.PendingNum, " ", bucket.capacity)
			for _, kvp := range delegationListCopy {
				bucket.Insert(kvp, db, cache)
			}
			bucket.UpdateBucketToDB(db, cache) // 更新桶
			bucketSs = [][]*Bucket{{bucket}}
		} else { // 否则一定只插入了一个，如果不是更新则引发桶的分裂
			bucketSs = bucket.Insert(kvPair, db, cache)
			if len(bucketSs[0]) == 1 {
				bucketSs[0][0].UpdateBucketToDB(db, cache) // 更新桶
				bucketSs = [][]*Bucket{{bucket}}
				//fmt.Println("No Split Occur")
			} else {
				var newLd int
				ld1 := bucketSs[0][0].GetLD()
				ld2 := bucketSs[0][1].GetLD()
				if ld1 < ld2 {
					newLd = ld1 + len(bucketSs) - 1
				} else {
					newLd = ld2 + len(bucketSs) - 1
				}
				seh.latch.Lock()
				seh.bucketsNumber += len(bucketSs) * (bucket.rdx - 1) //除第一层外每一层都是rdx-1个桶，这是因为第一层以外的桶都是上一层的某一个桶分裂出来的，因此要减去1
				if seh.gd < newLd {
					seh.gd = newLd
				}
				seh.latch.Unlock() //不同桶对ht的修改不会产生交集，因此尝试将seh锁释放提前，让sync.map本身特性保证ht修改的并发安全
				//无论是否扩展,均需遍历buckets,更新ht,更新buckets到db
				for i, buckets := range bucketSs {
					var bKey string
					for j := range buckets {
						if i != 0 && j == 0 { // 第一层往后每一层的第一个桶都是上一层分裂的那个桶，而上一层甚至更上层已经加过了，因此跳过
							continue
						}
						bKey = util.IntArrayToString(buckets[j].GetBucketKey(), buckets[j].rdx)
						// 在更新ht之前需要先把桶都给锁上，因为在此之后mgt还需要grow
						// 如果不锁的话，mgt在grow完成前可能桶就已经通过ht被找到，然后进行桶更新，就会出问题
						if buckets[j] != bucket { //将新分裂出来的桶给锁上，bucket本身已经被锁上了，因此不需要重复上锁
							buckets[j].latch.Lock()
						}
						seh.ht.Store(bKey, buckets[j])
						buckets[j].UpdateBucketToDB(db, cache)
					}
					toDelKey := bKey[util.ComputeStrideByBase(buckets[0].rdx):]
					seh.ht.Delete(toDelKey)
				}
			}
		}
		//bucket.DelegationList = nil
		//bucket.DelegationList = make(map[string]util.KVPair)
		bucket.latchTimestamp = 0
		//bucket.DelegationLatch.Unlock()
		// 此处桶锁不释放，会在MGTGrow的地方释放，因为此处桶锁释放后，其他线程就可以插入了，而此时mgt若需要分裂则还没有更新，因此可能会出现桶插入了但是相应mgtNode不存在的问题
		return bucketSs, DELEGATE, nil, 0
	} else if bucket != nil {
		// 成为委托者
		//if len(bucket.DelegationList) == 0 { // 保证被委托者能第一时间拿到DelegationLatch并更新自己要插入的数据到DelegationList中
		//	return nil, FAILED, nil, 0
		//}
		// 当发现上一个被委托者已经执行完插入操作，只剩mgtNode还在更新时，说明已经快要有下一个被委托者了，因此重做，尝试成为下一个被委托者
		// 以此保证DelegationList一定会有一个线程去将里面的内容写入桶中
		if bucket.latchTimestamp == 0 {
			return nil, FAILED, nil, 0
		}
		for !bucket.latch.TryLock() { // 重复查看是否存在可以委托的对象
			if len(bucket.DelegationList)+bucket.PendingNum+bucket.number >= bucket.capacity-1 { // 多减去的1是留给新来的被委托人的
				// 发现可能无法再委托则退出函数并重做，直到这个桶因一个线程的插入而分裂，产生新的空间
				// 说不定重做以后这就是新的被委托者，毕竟桶已满就说明一定有一个获得了桶锁的线程在工作中
				// 而这个工作线程在不久的将来就会更新完桶并释放锁，说不定你就在上一个if代码块里工作了
				return nil, FAILED, nil, 0
			}
			// 等待一个委托线程准备好接受委托，准备好的意思就是它已经把自己要插入的数据加入到delegationList
			// 否则被委托者即是获得了桶锁，但是会和委托者互相抢DelegationLatch，导致委托者无法更新自己的数据到桶中
			if bucket.latchTimestamp == 0 {
				continue
			}
			if bucket.DelegationLatch.TryLock() {
				//seh.latch.RLock()
				//// 需要检查是否因为桶的分裂而导致自己的桶已经不是当前桶了，如果不是则重做
				//// 但是不需要对seh上读锁，因为能到这段代码就说明虽然桶在插入，但是明显不会插满
				//// 那么桶就不会分裂，不会分裂seh就不会变，因此不需要对seh上读锁
				bucket_ := seh.GetBucketByKey(kvPair.GetKey(), db, cache)
				if bucket_ != bucket || len(bucket.DelegationList)+bucket.PendingNum+bucket.number >= bucket.capacity-1 || bucket.latchTimestamp == 0 {
					// 重新检查是否可以插入，发现没位置了就只能等新一轮调整让桶分裂了
					bucket.DelegationLatch.Unlock()
					//seh.latch.RUnlock()
					return nil, FAILED, nil, 0
				}
				////seh.latch.RUnlock()
				if oldValKvp, ok := bucket.DelegationList[kvPair.GetKey()]; ok {
					newValKvp := util.NewKVPair(oldValKvp.GetKey(), oldValKvp.GetValue())
					if isDelete {
						if isChange := newValKvp.DelValue(kvPair.GetValue()); !isChange { // 删除失败，延迟删除
							if _, ok := bucket.toDelMap[kvPair.GetKey()]; !ok {
								bucket.toDelMap[kvPair.GetKey()] = make(map[string]int)
							}
							bucket.toDelMap[kvPair.GetKey()][kvPair.GetValue()]++
						} else { // 删除成功，更新桶
							bucket.DelegationList[kvPair.GetKey()] = *newValKvp
						}
					} else {
						if bucket.toDelMap[kvPair.GetKey()][kvPair.GetValue()] > 0 { // 如果要插入的值在延迟删除列表中，则延迟删除列表中的计数减一，并跳过插入
							bucket.toDelMap[kvPair.GetKey()][kvPair.GetValue()]--
						} else {
							if isChange := newValKvp.AddValue(kvPair.GetValue()); isChange {
								bucket.DelegationList[kvPair.GetKey()] = *newValKvp
							}
						}
					}
				} else {
					if isDelete { // 删除失败，延迟删除
						if _, ok := bucket.toDelMap[kvPair.GetKey()]; !ok {
							bucket.toDelMap[kvPair.GetKey()] = make(map[string]int)
						}
						bucket.toDelMap[kvPair.GetKey()][kvPair.GetValue()]++
					} else {
						if bucket.toDelMap[kvPair.GetKey()][kvPair.GetValue()] > 0 { // 如果要插入的值在延迟删除列表中，则延迟删除列表中的计数减一，并跳过插入
							bucket.toDelMap[kvPair.GetKey()][kvPair.GetValue()]--
						} else {
							value, _, _, _ := bucket.GetValueByKey(kvPair.GetKey(), db, cache, true) //连带旧值一并更新
							newValKvp := util.NewKVPair(kvPair.GetKey(), value)
							if isChange := newValKvp.AddValue(kvPair.GetValue()); isChange {
								bucket.DelegationList[kvPair.GetKey()] = *newValKvp
							}
						}
					}
				}
				//// 成功委托
				bucket.DelegationLatch.Unlock()
				return nil, CLIENT, &bucket.latchTimestamp, bucket.latchTimestamp
			}
		}
		// 发现没有可委托的人，重做，尝试成为被委托者
		bucket.latch.Unlock()
		return nil, FAILED, nil, 0
	}
	return nil, FAILED, nil, 0
}

// PrintSEH 打印SEH
func (seh *SEH) PrintSEH(db *leveldb.DB, cache *[]interface{}) {
	fmt.Printf("打印SEH-------------------------------------------------------------------------------------------\n")
	if seh == nil {
		return
	}
	fmt.Printf("SEH: gd=%d, rdx=%d, bucketCapacity=%d, bucketSegNum=%d, bucketsNumber=%d\n", seh.gd, seh.rdx, seh.bucketCapacity, seh.bucketSegNum, seh.bucketsNumber)
	seh.latch.RLock()
	seh.ht.Range(func(key, value interface{}) bool {
		fmt.Printf("bucketKey=%s\n", key.(string))
		seh.GetBucket(key.(string), db, cache).PrintBucket(db, cache)
		return true
	})
	seh.latch.RUnlock()
}

type SeSEH struct {
	Gd             int      // global depth, initial zero
	Rdx            int      // rdx, initial  given
	BucketCapacity int      // capacity of the bucket, initial given
	BucketSegNum   int      // number of segment bits in the bucket, initial given
	HashTableKeys  []string // hash table of buckets
	BucketsNumber  int      // number of buckets, initial zero
}

func SerializeSEH(seh *SEH) []byte {
	hashTableKeys := make([]string, 0)
	seh.ht.Range(func(key, value interface{}) bool {
		hashTableKeys = append(hashTableKeys, key.(string))
		return true
	})
	seSEH := &SeSEH{seh.gd, seh.rdx, seh.bucketCapacity, seh.bucketSegNum,
		hashTableKeys, seh.bucketsNumber}
	if jsonSEH, err := json.Marshal(seSEH); err != nil {
		fmt.Printf("SerializeSEH error: %v\n", err)
		return nil
	} else {
		return jsonSEH
	}
}

func DeserializeSEH(data []byte) (*SEH, error) {
	var seSEH SeSEH
	if err := json.Unmarshal(data, &seSEH); err != nil {
		fmt.Printf("DeserializeSEH error: %v\n", err)
		return nil, err
	}
	seh := &SEH{seSEH.Gd, seSEH.Rdx, seSEH.BucketCapacity, seSEH.BucketSegNum,
		sync.Map{}, seSEH.BucketsNumber, sync.RWMutex{}}
	for _, key := range seSEH.HashTableKeys {
		if key == "" && seSEH.Gd > 0 {
			continue
		} else {
			seh.ht.Store(key, dummyBucket)
		}
	}
	return seh, nil
}
