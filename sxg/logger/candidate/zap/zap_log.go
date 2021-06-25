package log

import (
	"os"
	logI "sxg/logger"
	"sxg/logger/hook"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	Name = "zap"
)

type ZapFactory struct {
	factory *zap.Logger
}

func NewZapFactory() *ZapFactory {
	zap, _ := zap.NewProduction()
	return &ZapFactory{factory: zap}
}

func BetterNewZapFactory(filename string, level, maxSize, maxAge int) *ZapFactory {
	//hook
	hook := hook.NewHook(filename, maxSize, maxAge)
	//config
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "linenum",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,  // 小写编码器
		EncodeTime:     zapcore.ISO8601TimeEncoder,     // ISO8601 UTC 时间格式
		EncodeDuration: zapcore.SecondsDurationEncoder, //
		EncodeCaller:   zapcore.FullCallerEncoder,      // 全路径编码器
		EncodeName:     zapcore.FullNameEncoder,
	}
	//level
	atomicLevel := zap.NewAtomicLevel()
	atomicLevel.SetLevel(toZaplevel(level))
	//core
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),                                          // 编码器配置
		zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(hook)), // 打印到控制台和文件
		atomicLevel, // 日志级别
	)
	// 开启文件及行号
	development := zap.Development()

	zap := zap.New(core, development)
	return &ZapFactory{factory: zap}
}

func toZaplevel(level int) zapcore.Level {
	switch level {
	case logI.Debug:
		return zap.DebugLevel
	case logI.Info:
		return zap.InfoLevel
	case logI.Warn:
		return zap.WarnLevel
	case logI.Error:
		return zap.ErrorLevel
	default:
		return zap.InfoLevel
	}
}

func (z *ZapFactory) Logger() logI.Logger {
	logger := zagLogger{
		level: logI.Info,
		sugar: z.factory.Sugar(),
	}
	return &logger
}

func (z *ZapFactory) Clear() {
	z.factory.Sync()
}

type zagLogger struct {
	level int
	sugar *zap.SugaredLogger
}

func (z *zagLogger) permitted(level int) bool {
	return z.level <= level
}

func (z *zagLogger) Level() int {
	return z.level
}

func (z *zagLogger) SetLevel(level int) {
	z.level = level
}

func (z *zagLogger) Debug(content string) {
	if !z.permitted(logI.Debug) {
		return
	}
	z.sugar.Debug(content)
}

func (z *zagLogger) Debugf(format string, args ...interface{}) {
	if !z.permitted(logI.Debug) {
		return
	}
	z.sugar.Debugf(format, args...)
}

func (z *zagLogger) Info(content string) {
	if !z.permitted(logI.Info) {
		return
	}
	z.sugar.Info(content)
}

func (z *zagLogger) Infof(format string, args ...interface{}) {
	if !z.permitted(logI.Info) {
		return
	}
	z.sugar.Infof(format, args...)
}

func (z *zagLogger) Warn(content string) {
	if !z.permitted(logI.Warn) {
		return
	}
	z.sugar.Warn(content)
}

func (z *zagLogger) Warnf(format string, args ...interface{}) {
	if !z.permitted(logI.Warn) {
		return
	}
	z.sugar.Warnf(format, args...)
}

func (z *zagLogger) Error(content string) {
	if !z.permitted(logI.Error) {
		return
	}
	z.sugar.Error(content)
}

func (z *zagLogger) Errorf(format string, args ...interface{}) {
	if !z.permitted(logI.Error) {
		return
	}
	z.sugar.Errorf(format, args...)
}
