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

	siMode string //se的参数，辅助索引类型，meht或mpt
	rdx    int    //se的参数，meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	bc     int    //se的参数，meht中bucket的容量，即每个bucket中最多存储的KVPair数
	bs     int    //se的参数，meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
}

// NewSEDB() *SEDB: 返回一个新的SEDB
func NewSEDB(seh []byte, dbPath string, siMode string, rdx int, bc int, bs int) *SEDB {
	//打开或创建数据库
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		log.Fatal(err)
	}
	return &SEDB{nil, seh, db, dbPath, siMode, rdx, bc, bs}
}

func (sedb *SEDB) GetStorageEngine() *StorageEngine {
	//如果se为空，从db中读取se
	if sedb.se == nil {
		seString, error := sedb.db.Get(sedb.seHash, nil)
		if error == nil {
			se, _ := DeserializeStorageEngine(seString)
			sedb.se = se
		}
	}
	return sedb.se
}

// 向SEDB中插入一条记录,返回插入证明
func (sedb *SEDB) InsertKVPair(kvpair *util.KVPair) *SEDBProof {
	//如果是第一次插入
	if sedb.GetStorageEngine() == nil {
		//创建一个新的StorageEngine
		sedb.se = NewStorageEngine(sedb.siMode, sedb.rdx, sedb.bc, sedb.bs)
	}
	//向StorageEngine中插入一条记录
	newSEHash, primaryProof, secondaryMPTProof, secondaryMEHTProof := sedb.GetStorageEngine().Insert(kvpair, sedb.db)
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
	fmt.Printf("seHash:%s\n", hex.EncodeToString(sedb.seHash))
	fmt.Println("dbPath:", sedb.dbPath)
	sedb.GetStorageEngine().PrintStorageEngine(sedb.db)
}
