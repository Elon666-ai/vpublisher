package utils

import (
	"crypto/md5"
	"encoding/hex"
)

func MD5Sum(str string) string {
	s := md5.New()
	s.Write([]byte(str))
	return hex.EncodeToString(s.Sum(nil))
}

type authGenReq struct {
	AppId  string `json:"appId"`
	Stream string `json:"stream"`
	Expire int64  `json:"expire"`
}

type authGenResp struct {
	User     string `json:"user"`
	Password string `json:"password"`
	TxTime   string `json:"txTime"`
	TxSecret string `json:"txSecret"`
}

type apiResp struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data authGenResp `json:"data"`
}
