package log

const (
	Debug = iota
	Info
	Warn
	Error
)

type LoggerFactory interface {
	Logger() Logger
	Clear()
}

type Logger interface {
	Level() int
	SetLevel(level int)
	Debug(content string)
	Debugf(format string, args ...interface{})
	Info(content string)
	Infof(format string, args ...interface{})
	Warn(content string)
	Warnf(format string, args ...interface{})
	Error(content string)
	Errorf(format string, args ...interface{})
}
