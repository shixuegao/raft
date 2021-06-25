package exception

import "errors"

var ErrUnMatchedNumber error = errors.New("不匹配的包编号")
var ErrUnMatchedReqType error = errors.New("不匹配的请求类型")
var ErrShortLogData = errors.New("日志数据残缺")
