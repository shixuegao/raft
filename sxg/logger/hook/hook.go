package hook

import (
	"gopkg.in/natefinch/lumberjack.v2"
)

//fileName: 文件名称, maxSize: 最大大小(M), maxAge:保存最长天数
func NewHook(filename string, maxSize, maxAge int) *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename: filename,
		MaxSize:  maxSize,
		MaxAge:   maxAge,
	}
}
