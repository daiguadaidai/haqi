package utils

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

const (
	TIME_FORMAT           = "2006-01-02 15:04:05"
	TIME_FORMAT_FILE_NAME = "2006_01_02_15_04_05"
)

func NewTime(timeStr string) (time.Time, error) {
	return time.Parse(TIME_FORMAT, timeStr)
}

func StrTime2Int(tsStr string) (int64, error) {
	t, err := time.Parse(TIME_FORMAT, tsStr)
	if err != nil {
		return 0, err
	}
	return t.Unix(), nil
}

func TS2String(ts int64, format string) string {
	tm := time.Unix(ts, 0)
	return tm.Format(format)
}

func NowTimestamp() int64 {
	return time.Now().Unix()
}

const (
	UUID_LEN      = 30
	UUID_TIME_LEN = 24
)

// 获取唯一自增ID
func GetUUID() string {
	t := time.Now()
	uuid := t.Format("20060102150405123456")
	currUUIDLen := len(uuid)
	for i := 0; i < UUID_TIME_LEN-currUUIDLen; i++ {
		uuid += "0"
	}
	randLen := 6
	if currUUIDLen > UUID_TIME_LEN {
		randLen = UUID_LEN - currUUIDLen
	}
	return fmt.Sprintf("%s%s", uuid, RandString(randLen))
}

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func RandString(n int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// 用掩码实现随机字符串
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, r.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = r.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

// 字符串重复
func StrRepeat(d string, cnt int, sep string) string {
	dSlice := make([]string, cnt)
	for i := 0; i < cnt; i++ {
		dSlice[i] = d
	}

	return strings.Join(dSlice, sep)
}

// 获取sql表达表达式, 通过字段
func SqlExprPlaceholderByColumns(names []string, symbol string, holder string, sep string) string {
	exprs := make([]string, len(names))
	for i, name := range names {
		exprs[i] = fmt.Sprintf("`%s` %s %s", name, symbol, holder)
	}
	return strings.Join(exprs, sep)
}
