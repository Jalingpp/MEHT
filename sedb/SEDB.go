package sedb

import (
	"MEHT/meht"
	"MEHT/mpt"
	"MEHT/util"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

//func NewSEDB(seh []byte, dbPath string, siMode string, rdx int, bc int, bs int) *SEDB {}：新建一个SEDB
//func (sedb *SEDB) GetStorageEngine() *StorageEngine {}： 获取SEDB中的StorageEngine，如果为空，从db中读取se
//func (sedb *SEDB) InsertKVPair(kvpair *util.KVPair) *SEDBProof {}： 向SEDB中插入一条记录,返回插入证明
//func (sedb *SEDB) QueryKVPairsByHexKeyword(Hexkeyword string) (string, []*util.KVPair, *SEDBProof) {}: 根据十六进制的非主键Hexkeyword查询完整的kvpair
//func (sedb *SEDB) PrintKVPairsQueryResult(qkey string, qvalue string, qresult []*util.KVPair, qproof *SEDBProof) {}: 打印非主键查询结果
//func (sedb *SEDB) VerifyQueryResult(pk string, result []*util.KVPair, sedbProof *SEDBProof) bool {}: 验证查询结果
//func (sedb *SEDB) WriteSEDBInfoToFile(filePath string) {}： 写seHash和dbPath到文件
//func ReadSEDBInfoFromFile(filePath string) ([]byte, string) {}： 从文件中读取seHash和dbPath
//func (sedb *SEDB) PrintSEDB() {}： 打印SEDB

type SEDB struct {
	se     *StorageEngine //搜索引擎的指针
	seHash []byte         //搜索引擎序列化后的哈希值
	db     *leveldb.DB    //底层存储的指针
	dbPath string         //底层存储的文件路径

	siMode   string //se的参数，辅助索引类型，meht或mpt
	mehtName string //se的参数，meht的名字
	rdx      int    //se的参数，meht中mgt的分叉数，与key的基数相关，通常设为16，即十六进制数
	bc       int    //se的参数，meht中bucket的容量，即每个bucket中最多存储的KVPair数
	bs       int    //se的参数，meht中bucket中标识segment的位数，1位则可以标识0和1两个segment
}

// NewSEDB() *SEDB: 返回一个新的SEDB
func NewSEDB(seh []byte, dbPath string, siMode string, mehtName string, rdx int, bc int, bs int) *SEDB {
	//打开或创建数据库
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		log.Fatal(err)
	}
	return &SEDB{nil, seh, db, dbPath, siMode, mehtName, rdx, bc, bs}
}

// 获取SEDB中的StorageEngine，如果为空，从db中读取se
func (sedb *SEDB) GetStorageEngine() *StorageEngine {
	//如果se为空，从db中读取se
	if sedb.se == nil && sedb.seHash != nil {
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
		sedb.se = NewStorageEngine(sedb.siMode, sedb.mehtName, sedb.rdx, sedb.bc, sedb.bs)
	}
	//向StorageEngine中插入一条记录
	primaryProof, secondaryMPTProof, secondaryMEHTProof := sedb.GetStorageEngine().Insert(kvpair, sedb.db)
	//更新seHash，并将se更新至db
	sedb.seHash = sedb.GetStorageEngine().UpdateStorageEngineToDB(sedb.db)
	var pProof []*mpt.MPTProof
	pProof = append(pProof, primaryProof)
	sedbProof := NewSEDBProof(pProof, secondaryMPTProof, secondaryMEHTProof)
	//返回插入结果
	return sedbProof
}

// 根据十六进制的非主键Hexkeyword查询完整的kvpair
func (sedb *SEDB) QueryKVPairsByHexKeyword(Hexkeyword string) (string, []*util.KVPair, *SEDBProof) {
	if sedb.GetStorageEngine() == nil {
		fmt.Println("SEDB is empty!")
		return "", nil, nil
	}
	//根据Hexkeyword在非主键索引中查询
	var primaryKey string
	var secondaryMPTProof *mpt.MPTProof
	var secondaryMEHTProof *meht.MEHTProof
	var primaryProof []*mpt.MPTProof
	var queryResult []*util.KVPair
	var lock sync.Mutex
	var wg sync.WaitGroup
	if sedb.siMode == "mpt" {
		primaryKey, secondaryMPTProof = sedb.GetStorageEngine().GetSecondaryIndex_mpt(sedb.db).QueryByKey(Hexkeyword, sedb.db)
		secondaryMEHTProof = nil
		//根据primaryKey在主键索引中查询
		if primaryKey == "" {
			fmt.Println("No such key!")
			return "", nil, NewSEDBProof(nil, secondaryMPTProof, secondaryMEHTProof)
		}
		primarykeys := strings.Split(primaryKey, ",")
		wg.Add(len(primarykeys))
		for i := 0; i < len(primarykeys); i++ {
			go func(primarykey string, mutex *sync.Mutex) {
				qV, pProof := sedb.GetStorageEngine().GetPrimaryIndex(sedb.db).QueryByKey(primarykey, sedb.db)
				//用qV和primarykeys[i]构造一个kvpair
				kvpair := util.NewKVPair(primarykey, qV)
				lock.Lock()
				//把kvpair加入queryResult
				queryResult = append(queryResult, kvpair)
				//把pProof加入primaryProof
				primaryProof = append(primaryProof, pProof)
				lock.Unlock()
				wg.Done()
			}(primarykeys[i], &lock)
		}
		wg.Wait()
		return primaryKey, queryResult, NewSEDBProof(primaryProof, secondaryMPTProof, secondaryMEHTProof)
	} else if sedb.siMode == "meht" {
		secondaryMPTProof = nil
		pKey, qbucket, segkey, isSegExist, index := sedb.GetStorageEngine().GetSecondaryIndex_meht(sedb.db).QueryValueByKey(Hexkeyword, sedb.db)
		primaryKey = pKey
		//根据primaryKey在主键索引中查询，同时构建MEHT的查询证明
		ch := make(chan *meht.MEHTProof)
		go func(ch chan *meht.MEHTProof) {
			seMEHTProof := sedb.GetStorageEngine().GetSecondaryIndex_meht(sedb.db).GetQueryProof(qbucket, segkey, isSegExist, index, sedb.db)
			ch <- seMEHTProof
		}(ch)
		//根据primaryKey在主键索引中查询
		if primaryKey == "" {
			fmt.Println("No such key!")
		} else {
			primarykeys := strings.Split(primaryKey, ",")
			wg.Add(len(primarykeys))
			for i := 0; i < len(primarykeys); i++ {
				go func(primarykey string, mutex *sync.Mutex) {
					qV, pProof := sedb.GetStorageEngine().GetPrimaryIndex(sedb.db).QueryByKey(primarykey, sedb.db)
					//用qV和primarykeys[i]构造一个kvpair
					kvpair := util.NewKVPair(primarykey, qV)
					lock.Lock()
					//把kvpair加入queryResult
					queryResult = append(queryResult, kvpair)
					//把pProof加入primaryProof
					primaryProof = append(primaryProof, pProof)
					lock.Unlock()
					wg.Done()
				}(primarykeys[i], &lock)
			}
			wg.Wait()
		}
		secondaryMEHTProof = <-ch
		return primaryKey, queryResult, NewSEDBProof(primaryProof, secondaryMPTProof, secondaryMEHTProof)
	} else {
		fmt.Println("siMode is wrong!")
		return "", nil, nil
	}
}

// 打印非主键查询结果
func (sedb *SEDB) PrintKVPairsQueryResult(qkey string, qvalue string, qresult []*util.KVPair, qproof *SEDBProof) {
	fmt.Printf("打印查询结果-------------------------------------------------------------------------------------------\n")
	fmt.Printf("查询关键字为%s的主键为:%s\n", qkey, qvalue)
	for i := 0; i < len(qresult); i++ {
		fmt.Printf("查询结果[%d]:key=%s,value=%s\n", i, qresult[i].GetKey(), util.HexToString(qresult[i].GetValue()))
	}
	fmt.Printf("查询证明为:\n")
	if qproof != nil {
		qproof.PrintSEDBProof()
	} else {
		fmt.Printf("查询证明为空\n")
	}
}

// 验证查询结果
func (sedb *SEDB) VerifyQueryResult(pk string, result []*util.KVPair, sedbProof *SEDBProof) bool {
	r := false
	rCh := make(chan bool)
	fmt.Printf("验证查询结果-------------------------------------------------------------------------------------------\n")
	//验证非主键查询结果
	fmt.Printf("验证非主键查询结果:")
	if sedb.siMode == "mpt" {
		r = sedb.se.GetSecondaryIndex_mpt(sedb.db).VerifyQueryResult(pk, sedbProof.GetSecondaryMPTIndexProof())
	} else if sedb.siMode == "meht" {
		r = meht.VerifyQueryResult(pk, sedbProof.GetSecondaryMEHTIndexProof())
	} else {
		fmt.Println("siMode is wrong!")
		return false
	}
	if !r {
		fmt.Println("非主键查询结果验证失败！")
		return false
	}
	//验证主键查询结果
	if pk == "" {
		fmt.Println("非主键查询结果为空！")
		return false
	}
	fmt.Printf("验证主键查询结果:\n")
	primaryIndexProof := sedbProof.GetPrimaryIndexProof()
	for i := 0; i < len(result); i++ {
		go func(result string, proof *mpt.MPTProof) {
			rCh <- sedb.se.GetPrimaryIndex(sedb.db).VerifyQueryResult(result, proof)
		}(result[i].GetValue(), primaryIndexProof[i])
	}
	for i := 0; i < len(result); i++ {
		r = <-rCh
		if !r {
			fmt.Println("主键查询结果验证失败!")
			return r
		}
	}
	return r
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
	dbPath := util.Strip(seh_dbPath[1], "\n\t\r")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		err = os.MkdirAll(dbPath, 0750)
		if err != nil {
			panic(err)
		}
	}
	return seh, dbPath
}

// 打印SEDB
func (sedb *SEDB) PrintSEDB() {
	fmt.Println("打印SEDB-----------------------------------------------------------------------")
	fmt.Printf("seHash:%s\n", hex.EncodeToString(sedb.seHash))
	fmt.Println("dbPath:", sedb.dbPath)
	sedb.GetStorageEngine().PrintStorageEngine(sedb.db)
}

// 获取db
func (sedb *SEDB) GetDB() *leveldb.DB {
	return sedb.db
}
