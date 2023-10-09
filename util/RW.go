package util

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func ReadKVPairFromFile(filepath string) []*KVPair {
	//打开文本文件
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println("Open file error!")
		return nil
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(file)
	//创建KVPair数组
	var kvPairs []*KVPair
	//读取文件内容
	readr := bufio.NewReader(file)
	for {
		//读取一行
		line, err := readr.ReadString('\n')
		if err != nil {
			break
		}
		//将一行分割为key和value
		line = strings.TrimRight(line, "\n")
		kv := strings.Split(line, ",")
		//创建一个KVPair
		kvPair := NewKVPair(kv[0], kv[1])
		//将KVPair加入数组
		kvPairs = append(kvPairs, kvPair)
	}
	return kvPairs
}
