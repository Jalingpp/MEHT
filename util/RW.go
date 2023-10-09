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
	defer file.Close()
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

func WriteStringToFile(filePath string, data string) {
	//打开文件
	file, err := os.Create(filePath)
	if err != nil {
		fmt.Println("Create file error!")
		return
	}
	defer file.Close()
	//写字符串
	file.WriteString(data)
}

func ReadStringFromFile(filePath string) (string, error) {
	//打开文件
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Open file error!")
		return "", err
	}
	defer file.Close()
	//读取文件内容
	readr := bufio.NewReader(file)
	line, err := readr.ReadString('\n')
	if err != nil {
		return "", err
	}
	//去掉换行符
	line = strings.TrimRight(line, "\n")
	return line, nil
}
