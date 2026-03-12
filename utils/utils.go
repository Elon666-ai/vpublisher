package utils

import (
	"bufio"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	mathRand "math/rand"
	"net"
	"net/mail"
	"os"
	"strconv"
	"strings"
	"unicode"
)

var runEnv string = "dev"

func init() {

	//环境变量 > .env 文件
	env, _ := GetEnvFromFile(ENV_FILE, "ENV")
	if os.Getenv("ENV") != "" {
		env = os.Getenv("ENV")
	}

	env = strings.TrimRight(env, "\r\n")
	env = strings.TrimSpace(env)
	env = strings.ToLower(env)
	if env != "prod" && env != "stag" && env != "uat" && env != "dev" && env != "test" {
		env = "local"
	}
	runEnv = env
}

func GetConfigFile() (string, string) {

	configFile := CONF_FILE
	if len(runEnv) > 0 {
		configFile = CONF_FILE + "." + runEnv
	}

	return configFile, runEnv
}

func CalcPageOffset(page string, pageSize string) (int64, int64) {
	n, _ := strconv.Atoi(page)
	if n == 0 {
		n = 1
	}

	size, _ := strconv.Atoi(pageSize)
	switch {
	case size > 100:
		size = 100
	case size <= 0:
		size = 20
	}

	offset := (n - 1) * size

	return int64(offset), int64(size)
}

func CreateIfNotExist(dir string) bool {

	_, err := os.Stat(dir)
	if err == nil {
		return true
	}

	err = os.MkdirAll(dir, 0750)
	//return os.IsExist(err) //it's a bug
	return err == nil
}

func CheckFileExist(filename string) bool {
	_, err := os.Open(filename)
	return err == nil
}

func GetLocalIp() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}

	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet == nil || ipNet.IP.IsLoopback() {
			continue
		}

		if ipNet.IP.To4() != nil { // IPv4
			return ipNet.IP.String()
		} else if len(ipNet.IP) > 15 && ipNet.IP[0] == 239 && ipNet.IP[1] == 255 { // IPv6 link-local unicast
			return fmt.Sprintf("fe80::%x", ipNet.IP[2:])
		}
	}

	return ""
}

func GetList(value string, sep string) []string {
	var res = []string{}

	myList := strings.Split(value, sep)
	for _, v := range myList {
		if v == "" {
			continue
		}
		res = append(res, v)
	}

	return res
}

func StrCmp(str1 string, str2 string) bool {
	if strings.EqualFold(str1, str2) {
		return true
	} else {
		return false
	}
}

func GetUUID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return uuid
}

func GetRandomResult() int {
	// rand.Seed(1) // 设置随机数种子，不设置的话，默认使用当前时间作为种子
	min, max := 0, 2
	k := mathRand.Intn(max-min+1) + min
	return k
}

func ParseTopic(topic string) (string, string) {
	var mcid, act string = topic, ""
	arr := strings.Split(topic, "/")
	if len(arr) >= 3 {
		mcid = arr[1]
		act = arr[2]
		if len(arr) == 4 {
			act = arr[3]
		}
	}
	return mcid, act
}

func IsValidEmailLib(email string) bool {
	_, err := mail.ParseAddress(email)
	return err == nil
}

func IsExactHas(list []string, item string) bool {
	for _, s := range list {
		if s == item {
			return true
		}
	}
	return false
}

func HasPrefix(list []string, item string) bool {
	for _, s := range list {
		if strings.HasPrefix(s, item) {
			return true
		}
	}
	return false
}

// 生成 MD5 签名（appSecrect + stream_id + txTime）. stream_id 格式: {app}/{stream}
func GenTxSecret(key, streamID, txTime string) string {
	h := md5.Sum([]byte(key + streamID + strings.ToUpper(txTime)))
	return hex.EncodeToString(h[:])
}

// GetEnvFromFile 读取 .env 文件并返回指定 key 的值
func GetEnvFromFile(filename, key string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// 跳过空行或注释
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, key+"=") {
			value := strings.TrimPrefix(line, key+"=")
			return value, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	return "", errors.New("key not found in file")
}

// ParseGOMemLimit 读取 GOMEMLIMIT 并转换为 int64 字节数
func ParseGOMemLimit() (int64, error) {
	val := os.Getenv("GOMEMLIMIT")
	if val == "" {
		return 0, nil // 未设置
	}

	return parseBytes(val)
}

// parseBytes 解析带单位的内存字符串 (兼容 Go 官方格式)
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty string")
	}

	// 找到数字部分的结束位置
	end := 0
	for i, r := range s {
		if !unicode.IsDigit(r) {
			end = i
			break
		}
		end = i + 1
	}

	// 解析数字部分
	numStr := s[:end]
	val, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number: %v", err)
	}

	// 解析单位部分
	unit := s[end:]
	var multiplier int64 = 1

	switch unit {
	case "", "B":
		multiplier = 1
	case "KiB":
		multiplier = 1 << 10 // 1024
	case "MiB":
		multiplier = 1 << 20 // 1024 * 1024
	case "GiB":
		multiplier = 1 << 30 // 1024 * 1024 * 1024
	case "TiB":
		multiplier = 1 << 40
	case "KB", "k": // 十进制单位
		multiplier = 1000
	case "MB", "M":
		multiplier = 1000 * 1000
	case "GB", "G":
		multiplier = 1000 * 1000 * 1000
	case "TB", "T":
		multiplier = 1000 * 1000 * 1000 * 1000
	default:
		return 0, fmt.Errorf("unknown unit: %s", unit)
	}

	return val * multiplier, nil
}

func GetContainerMemoryLimit() (uint64, error) {
	// cgroup v2
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		s := strings.TrimSpace(string(data))
		if s != "max" {
			if v, err := strconv.ParseUint(s, 10, 64); err == nil {
				return v, nil
			}
		}
	}

	// cgroup v1
	if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		if v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64); err == nil {
			// v1 unlimited 会返回一个非常大的数
			if v < 1<<60 {
				return v, nil
			}
		}
	}

	return 0, errors.New("cannot detect container memory limit")
}
