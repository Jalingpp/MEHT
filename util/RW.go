package util

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

// GetDirAllFilePathsFollowSymlink gets all the file paths in the specified directory recursively.
func GetDirAllFilePathsFollowSymlink(dirname string) ([]string, error) {
	// Remove the trailing path separator if dirname has.
	dirname = strings.TrimSuffix(dirname, string(os.PathSeparator))
	infos, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(infos))
	for _, info := range infos {
		path := dirname + string(os.PathSeparator) + info.Name()
		realInfo, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if realInfo.IsDir() {
			tmp, err := GetDirAllFilePathsFollowSymlink(path)
			if err != nil {
				return nil, err
			}
			paths = append(paths, tmp...)
			continue
		}
		paths = append(paths, path)
	}
	return paths, nil
}

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
	address_ := strings.Split(filepath, string(os.PathSeparator))
	address := strings.Split(address_[len(address_)-1], ".")[0]
	if err := json.Unmarshal(content, &content_); err != nil {
		panic(err)
	}
	if content_, ok := content_.(map[string]interface{}); ok {
		for k1, v1 := range content_ {
			var v_ []string
			if traits, ok := v1.(map[string]interface{}); ok {
				v_ = make([]string, 0)
				k2List := make([]string, 0)
				for k2, _ := range traits {
					k2List = append(k2List, k2)
				}
				sort.Strings(k2List)
				for _, v2 := range k2List {
					v_ = append(v_, traits[v2].(string))
				}
			}
			kvPair := NewKVPair(address+string(os.PathSeparator)+k1, strings.Join(v_, ","))
			//fmt.Println(kvPair.GetKey())
			//fmt.Println(kvPair.GetValue())
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
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	}(file)
	//写字符串
	_, err = file.WriteString(data)
	if err != nil {
		panic(err)
	}
}

func ReadStringFromFile(filePath string) (string, error) {
	//读取文件内容
	content, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Println("Open file error!")
		return "", err
	}
	return strings.Split(string(content), "\n")[0], nil
}
