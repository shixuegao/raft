package logger

import (
	logI "sxg/logger"
	zap "sxg/logger/candidate/zap"
)

var Logger logI.Logger
var factory *zap.ZapFactory

func init() {
	Logger = zapInit()
}

func zapInit() logI.Logger {
	factory = zap.BetterNewZapFactory("raft.log", logI.Info, 30, 7)
	return factory.Logger()
}

func Clear() {
	factory.Clear()
}
