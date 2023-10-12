package util

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func ReadKVPairFromJsonFile(filepath string) []*KVPair {
	file, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)
	var kvPairs []*KVPair
	content, err := os.ReadFile(filepath)
	if err != nil {
		panic(err)
	}
	var content_ interface{}
	if err := json.Unmarshal(content, &content_); err != nil {
		panic(err)
	}
	if content_, ok := content_.(map[string]interface{}); ok {
		for k1, v1 := range content_ {
			v_ := ""
			if traits, ok := v1.(map[string]interface{}); ok {
				for k2, v2 := range traits {
					v_ += k2 + ":" + v2.(string) + ","
				}
				v_ = v_[:len(v_)-1]
			}
			kvPair := NewKVPair(v_, k1)
			fmt.Println(v_, k1)
			kvPairs = append(kvPairs, kvPair)
		}
	}
	return kvPairs
}

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
		kvPair := NewKVPair(Strip(kv[0], "\n\t\r"), Strip(kv[1], "\n\t\r"))
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
