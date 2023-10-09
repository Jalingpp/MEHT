package sedb

import (
	"MEHT/util"
	"encoding/hex"
	"fmt"
	"log"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
)

type SEDB struct {
	se     *StorageEngine //搜索引擎的指针
	seHash []byte         //搜索引擎序列化后的哈希值
	db     *leveldb.DB    //底层存储的指针
	dbPath string         //底层存储的文件路径
}

// NewSEDB() *SEDB: 返回一个新的SEDB
func NewSEDB(seh []byte, siMode string, rdx int, bc int, bs int, dbPath string) *SEDB {
	//打开或创建数据库
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		log.Fatal(err)
	}
	storengine := NewStorageEngine(seh, nil, siMode, nil, rdx, bc, bs, db)
	return &SEDB{storengine, seh, db, dbPath}
}

// 向SEDB中插入一条记录,返回插入证明
func (sedb *SEDB) InsertKVPair(kvpair *util.KVPair) *SEDBProof {
	//向StorageEngine中插入一条记录
	newSEHash, primaryProof, secondaryMPTProof, secondaryMEHTProof := sedb.se.Insert(kvpair, sedb.db)
	//更新seHash
	sedb.seHash = newSEHash
	//构造SEDBProof
	sedbProof := NewSEDBProof(primaryProof, secondaryMPTProof, secondaryMEHTProof)
	//返回插入结果
	return sedbProof
}

// 写seHash和dbPath到文件
func (sedb *SEDB) WriteSEDBInfoToFile(filePath string) {
	data := hex.EncodeToString(sedb.seHash) + "," + sedb.dbPath + "\n"
	util.WriteStringToFile(filePath, data)
}

// 从文件中读取seHash和dbPath
func ReadSEDBInfoFromFile(filePath string) ([]byte, string) {
	data, _ := util.ReadStringFromFile(filePath)
	seh_dbPath := strings.Split(data, ",")
	if len(seh_dbPath) != 2 {
		fmt.Println("seHash and dbPath don't exist!")
		return nil, ""
	}
	seh, _ := hex.DecodeString(seh_dbPath[0])
	dbPath := seh_dbPath[1]
	return seh, dbPath
}

// 打印SEDB
func (sedb *SEDB) PrintSEDB() {
	fmt.Println("打印SEDB-----------------------------------------------------------------------")
	fmt.Println("seHash:", sedb.seHash)
	fmt.Println("dbPath:", sedb.dbPath)
	sedb.se.PrintStorageEngine()
}
