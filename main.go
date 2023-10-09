package main

import (
	"MEHT/sedb"
	"MEHT/util"
	// "encoding/hex"
	// "fmt"
)

func main() {
	//测试SEDB

	//参数设置
	filePath := "data/levelDB/config.txt" //存储seHash和dbPath的文件路径
	// siMode := "meht" //辅助索引类型，meht或mpt
	siMode := "mpt"
	rdx := 16 //meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	bc := 4   //meht中bucket的容量，即每个bucket中最多存储的KVPair数
	bs := 1   //meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
	seHash, dbPath := sedb.ReadSEDBInfoFromFile(filePath)
	// dbPath := "data/levelDB/testSEDB" //初始化的底层存储文件路径

	kvdataPath := "data/testdata.txt"

	//创建一个SEDB
	// sedb := sedb.NewSEDB(nil, siMode, rdx, bc, bs, dbPath)
	sedb := sedb.NewSEDB(seHash, siMode, rdx, bc, bs, dbPath)

	//读文件创建一个KVPair数组
	kvPairs := util.ReadKVPairFromFile(kvdataPath)

	//插入KVPair数组
	for i := 0; i < 6; i++ {
		//把KV转化为十六进制
		kvPairs[i].SetKey(kvPairs[i].GetKey())
		kvPairs[i].SetValue(util.StringToHex(kvPairs[i].GetValue()))
		//插入SEDB
		sedb.InsertKVPair(kvPairs[i])
	}

	//打印SEDB
	sedb.PrintSEDB()

	//写seHash和dbPath到文件
	sedb.WriteSEDBInfoToFile(filePath)

	// s := "000c4"
	// hexValue := hex.EncodeToString([]byte(s))
	// fmt.Println(hexValue)
	// fmt.Printf("%x\n", []byte(hexValue))
}
