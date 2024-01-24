package util

import (
	"fmt"
	"os"
	"strings"
)

//// GetDirAllFilePathsFollowSymlink gets all the file paths in the specified directory recursively.
//func GetDirAllFilePathsFollowSymlink(dirname string) ([]string, error) {
//	// Remove the trailing path separator if dirname has.
//	dirname = strings.TrimSuffix(dirname, string(os.PathSeparator))
//	infos, err := os.ReadDir(dirname)
//	if err != nil {
//		return nil, err
//	}
//	paths := make([]string, 0, len(infos))
//	for _, info := range infos {
//		path := dirname + string(os.PathSeparator) + info.Name()
//		realInfo, err := os.Stat(path)
//		if err != nil {
//			return nil, err
//		}
//		if realInfo.IsDir() {
//			tmp, err := GetDirAllFilePathsFollowSymlink(path)
//			if err != nil {
//				return nil, err
//			}
//			paths = append(paths, tmp...)
//			continue
//		}
//		paths = append(paths, path)
//	}
//	return paths, nil
//}
func ReadLinesFromFile(filepath string) (lines []string) {
	content, err := os.ReadFile(filepath)
	if err != nil {
		panic(err)
	}
	lines = strings.Split(string(content), "\n")
	return lines
}

func ReadNFTOwnerFromFile(filepath string, num int) (kvPairs []KVPair) {
	content, err := os.ReadFile(filepath)
	if err != nil {
		panic(err)
	}
	lines := strings.Split(string(content), "\n")
	for i, line := range lines {
		if len(line) == 0 || i == num {
			break
		}
		line_ := Strip(line, "\r")
		kvs := strings.Split(line_, ",")
		kvPairs = append(kvPairs, KVPair{kvs[1], kvs[2]})
	}
	return
}

//func ReadQueryOwnerFromFile(filepath string, num int) (ret []string) {
//	content, err := os.ReadFile(filepath)
//	if err != nil {
//		panic(err)
//	}
//	lines := strings.Split(string(content), "\n")
//	for i, line := range lines {
//		if len(line) == 0 || i == num {
//			break
//		}
//		ret = append(ret, Strip(line, "\r"))
//	}
//	return
//}

func ReadQueryFromFile(dirPath string, num int) (ret []string) {
	//if dirPath[len(dirPath)-1] != '/' || dirPath[len(dirPath)-1] != '\\' {
	//	dirPath = (" " + dirPath[:len(dirPath)-1])[1:] + string(os.PathSeparator)
	//}
	//content, err := os.ReadFile(dirPath + "query-" + strconv.Itoa(num/10000) + "W")
	content, err := os.ReadFile(dirPath)
	//content, err := os.ReadFile(dirPath + "query-1")
	if err != nil {
		panic(err)
	}
	lines := strings.Split(string(content), "\n")
	for i, line := range lines {
		if len(line) == 0 || i == num {
			break
		}
		ret = append(ret, Strip(line, "\r"))
	}
	return
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

func WriteResultToFile(filePath string, data string) {
	//打开文件
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Open file error!")
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
