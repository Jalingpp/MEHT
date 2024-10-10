package meht

import (
	"math"

	"github.com/willf/bloom"
)

type BSFG struct {
	WindowSize int                  //滑动窗口大小
	Stride     int                  //滑动窗口步频，即相邻两个窗口间相差的bk长度
	BFSize     int                  //单个布隆过滤器所含位数
	BFHashNum  int                  //单个布隆过滤器哈希函数的个数
	Rdx        int                  //bk的进制数
	BFList     []*bloom.BloomFilter //布隆过滤器列表，每个bf的范围是[1+index*Stride,index*Stride+WindowSize]
	ElementMap map[int]int          //记录每个过滤器中的元素个数
	EMap       map[string][]int
}

func NewBSFG(ws int, strd int, bfsize int, bfhnum int, rdx int) *BSFG {
	bfList := make([]*bloom.BloomFilter, 0)
	// return &BSFG{ws, strd, bfsize, bfhnum, rdx, bfList, make(map[string][]int)}
	return &BSFG{ws, strd, bfsize, bfhnum, rdx, bfList, make(map[int]int), make(map[string][]int)}
}

func (bsfg *BSFG) InsertElement(bucketkey string) {
	length := len(bucketkey)
	if length <= 0 {
		return
	}
	stIndex := (length - bsfg.WindowSize) / bsfg.Stride
	// fmt.Println(length, bsfg.WindowSize, bsfg.Stride)
	if stIndex < 0 {
		stIndex = 0
	}
	if stIndex*bsfg.Stride+bsfg.WindowSize < length {
		stIndex = stIndex + 1
	}
	edIndex := (length - 1) / bsfg.Stride
	if edIndex < 0 {
		edIndex = 0
	}
	if length > edIndex*bsfg.Stride+bsfg.WindowSize {
		edIndex = edIndex + 1
	}
	for i := len(bsfg.BFList); i < stIndex; i++ {
		filter := bloom.New(uint(bsfg.BFSize), uint(bsfg.BFHashNum))
		bsfg.BFList = append(bsfg.BFList, filter)
	}
	for i := stIndex; i <= edIndex; i++ {
		if len(bsfg.BFList) == i {
			filter := bloom.New(uint(bsfg.BFSize), uint(bsfg.BFHashNum))
			bsfg.BFList = append(bsfg.BFList, filter)
		}
		// fmt.Println(len(bsfg.BFList), i)
		bsfg.BFList[i].Add([]byte(bucketkey))
		bsfg.ElementMap[i] = bsfg.ElementMap[i] + 1
		bsfg.EMap[bucketkey] = append(bsfg.EMap[bucketkey], i)
	}
}

func (bsfg *BSFG) IsExist(bucketkey string) bool {
	length := len(bucketkey)
	stIndex := (length - bsfg.WindowSize) / bsfg.Stride
	if stIndex < 0 {
		stIndex = 0
	}
	if stIndex*bsfg.Stride+bsfg.WindowSize < length {
		stIndex = stIndex + 1
	}
	edIndex := (length - 1) / bsfg.Stride
	if edIndex < 0 {
		edIndex = 0
	}
	if length > edIndex*bsfg.Stride+bsfg.WindowSize {
		edIndex = edIndex + 1
	}
	if len(bsfg.BFList) <= stIndex {
		return false
	}
	for i := stIndex; i <= edIndex; i++ {
		if len(bsfg.BFList) == i {

			return false
		}
		if !bsfg.BFList[i].Test([]byte(bucketkey)) {
			// fmt.Println("11111", bucketkey, bsfg.EMap)
			return false
		}
	}
	return true
}

// func (bsfg *BSFG) EstimateFPRate() float64 {
// 	//计算最新BF中的FPR
// 	st := 1 + len(bsfg.BFList)*bsfg.Stride
// 	ed := len(bsfg.BFList)*bsfg.Stride + bsfg.WindowSize
// 	//计算当前BF能容纳的元素上限
// 	eleNum := 0
// 	for j := st; j <= ed; j++ {
// 		eleNum = eleNum + int(math.Pow(float64(bsfg.Rdx), float64(j)))
// 	}
// 	rate := math.Pow(FPRate(bsfg.BFSize, bsfg.BFHashNum, eleNum), float64(bsfg.WindowSize)/float64(bsfg.Stride))
// 	return rate
// }

func (bsfg *BSFG) EstimateFPRate() float64 {
	maxFPR := float64(0)
	fpStride := bsfg.WindowSize / bsfg.Stride
	//计算每连续ws/stride个BF计算一个FPR，取最大的FPR
	for i := 0; i < len(bsfg.BFList); i += fpStride {
		_rate := float64(1)
		for j := i; j < i+fpStride; j++ {
			_rate = _rate * FPRate(bsfg.BFSize, bsfg.BFHashNum, bsfg.ElementMap[j])
		}
		if _rate > maxFPR {
			maxFPR = _rate
		}
	}
	return maxFPR
}

func FPRate(m int, k int, n int) float64 {
	return math.Pow(1.0-math.Exp(-float64(k)*float64(n)/float64(m)), float64(k))
}
