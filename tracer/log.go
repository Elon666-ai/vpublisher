package tracer

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// content of emnu Level ,level of log
	EMEGR = iota
	ALERT
	CRIT
	ERROR
	WARN
	NOTICE
	INFO
	DEBUG
	TRACE
)

const (
	ID_SYS   = 0 << 3
	ID_ADMIN = 1 << 3
	ID_SEC   = 13 << 3
	ID_APP   = 16 << 3
	ID_DB    = 17 << 3
	ID_GAME  = 18 << 3
	ID_USER  = 19 << 3
)

type Outputer int

const (
	STD = iota
	FILE
)

const (
	CONSOLE = 1 << iota
	LOCAL
	REMOTE
)

type logger struct {
	logFilename   string
	logFd         *os.File // 文件描述符
	starLev       int      // 日志记录的等级
	buf           []byte   // 缓冲区
	path          string   // 路径
	baseName      string   // 通用名称
	logName       string   // 日志名称
	debugOutputer Outputer // 调试输出
	debugSwitch   bool     // 调试模式切换
	callDepth     int      // 日志文件记录深度
	fullPath      string   // 文件全路径
	lastHour      int      // 上一次记录的小时
	lastDay       int
	lastDate      string // last date
	IsShowConsole bool   // 是否控制台显示
	logChan       chan string
	saveFlags     int // Decide if save or send to remote
	remoteLog     *remote
	msgQueue      []string
	msgQueueMutex sync.Mutex
	quitCh        chan struct{}
	stoppedCh     chan struct{}
	closeOnce     sync.Once
}

var gLogger *logger
var DBLogger = &gLogger
var once sync.Once
var initMu sync.Mutex

func InitLog(level int, logF string) {
	initMu.Lock()
	defer initMu.Unlock()

	os.MkdirAll("./logs", 0750)

	old := gLogger
	nl := newLogger("./logs", "", "Log4Golang", level, LOCAL)
	nl.setCallDepth(3)
	nl.starLev = level
	nl.logFilename = logF
	nl.start()
	gLogger = nl

	if old != nil {
		old.shutdown()
	}
}

// func init() {
// 	InitLog(DEBUG, "mediamtx")
// }

func EnableRemoteLog(ident string, addr string) {
	if nil == gLogger {
		panic("init log first")
	} else {
		gLogger.saveFlags |= REMOTE
		if gLogger.remoteLog != nil {
			gLogger.remoteLog.start(ident, addr)
		} else {
			gLogger.remoteLog = newRemote()
			gLogger.remoteLog.start(ident, addr)
		}
	}
}

func newLogger(path, baseName, logName string, level int, saveFlags int) *logger {
	logger := &logger{path: path, baseName: baseName, logName: logName, starLev: level}
	logger.debugSwitch = true
	logger.debugOutputer = STD
	logger.callDepth = 3
	logger.logChan = make(chan string, 8096)
	logger.saveFlags = CONSOLE | saveFlags
	logger.msgQueue = make([]string, 0)
	logger.quitCh = make(chan struct{})
	logger.stoppedCh = make(chan struct{})
	if logger.saveFlags&REMOTE != 0 {
		gLogger.remoteLog = newRemote()
	}
	return logger
}

func (l *logger) getCurDate() string {
	now := time.Now()
	str := now.Format("2006-01-02")
	return str
}

func (l *logger) pathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	return false
}

func (l *logger) getLoggerFd() *os.File {
	curDate := l.getCurDate()
	if l.lastDate != curDate {
		//path := strings.TrimSuffix(l.path, "/")
		//path = path + "/"
		//if !l.pathExists(path){
		//	err := os.Mkdir(path, os.ModePerm)
		//	if err != nil{
		//		fmt.Println(err)
		//		panic(err)
		//	}
		//}
		l.lastDate = curDate
	}

	var err error
	path := strings.TrimSuffix(l.path, "/")
	flag := os.O_WRONLY | os.O_APPEND | os.O_CREATE
	l.fullPath = path + "/" + l.baseName
	now := time.Now()
	l.fullPath += fmt.Sprintf("%s-%04d%02d%02d.log", l.logFilename, now.Year(), now.Month(), now.Day())
	l.logFd, err = os.OpenFile(l.fullPath, flag, 0666)
	if err != nil {
		panic(err)
	}
	return l.logFd
}

func (l *logger) start() {
	if l.saveFlags&LOCAL != 0 {
		err := os.MkdirAll(l.path, os.ModePerm)
		if err != nil {
			panic(err)
		}
		l.logFd = l.getLoggerFd()
		AsyncRunCoroutine(func() {
			l.autoWrite()
		})
	}
}

func (l *logger) autoWrite() {
	defer close(l.stoppedCh)
	for {
		select {
		case <-l.quitCh:
			return
		default:
		}
		d := l.pop_front()
		if d != "" {
			l.writeLog(bytes.NewBufferString(d).Bytes())
		} else {
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (l *logger) shutdown() {
	l.closeOnce.Do(func() {
		close(l.quitCh)
		<-l.stoppedCh
		if l.logFd != nil {
			_ = l.logFd.Close()
			l.logFd = nil
		}
	})
}

func (l *logger) writeLog(buf []byte) {

	now := time.Now()
	if now.Day() != l.lastDay {
		//先将当前文件关闭
		err := l.logFd.Close()
		if err != nil {
			str := fmt.Sprintf("close file[%v] failed[err:%v]", l.fullPath, err.Error())
			fmt.Println(str)
		}
		//获取下一个索引的文件
		l.logFd = l.getLoggerFd()
		//l.lastHour = now.Hour()
		l.lastDay = now.Day()
	}
	_, err := l.logFd.Write(buf)
	if err != nil {
		fmt.Printf("write failed, %v", err)
	}
}

func (l *logger) output(fd io.Writer, level, prefix string, format string, v ...interface{}) (err error) {
	var msg string
	if format == "" {
		msg = fmt.Sprintln(v...)
	} else {
		msg = fmt.Sprintf(format, v...)
	}

	l.buf = l.buf[:0]

	l.buf = append(l.buf, "["+l.logName+"]"...)
	l.buf = append(l.buf, level...)
	l.buf = append(l.buf, prefix...)

	l.buf = append(l.buf, ":"+msg...)
	if len(msg) > 0 && msg[len(msg)-1] != '\n' {
		l.buf = append(l.buf, '\n')
	}

	_, err = fd.Write(l.buf)

	return nil
}

func (l *logger) setCallDepth(d int) {
	l.callDepth = d
}

func (l *logger) openDebug() {
	l.debugSwitch = true
}

func (l *logger) getFileLine() string {
	_, file, line, ok := runtime.Caller(l.callDepth)
	if !ok {
		file = "???"
		line = 0
	}
	return l.getFileName(file) + ":" + itoa(line, -1)
}

func (l *logger) getFileName(path string) string {
	strArr := strings.Split(path, "/")
	nLen := len(strArr)
	if nLen > 0 {
		return strArr[nLen-1]
	}
	return path
}

/**
* Change from Golang's log.go
* Cheap integer to fixed-width decimal ASCII.  Give a negative width to avoid zero-padding.
* Knows the buffer has capacity.
 */
func itoa(i int, wid int) string {
	var u uint = uint(i)
	if u == 0 && wid <= 1 {
		return "0"
	}

	// Assemble decimal in reverse order.
	var b [32]byte
	bp := len(b)
	for ; u > 0 || wid > 0; u /= 10 {
		bp--
		wid--
		b[bp] = byte(u%10) + '0'
	}
	return string(b[bp:])
}

func (l *logger) getTime() string {
	// Time is yyyy-mm-dd hh:mm:ss.microsec
	var buf []byte
	t := time.Now()
	year, month, day := t.Date()
	buf = append(buf, itoa(int(year), 4)+"-"...)
	buf = append(buf, itoa(int(month), 2)+"-"...)
	buf = append(buf, itoa(int(day), 2)+" "...)

	hour, min, sec := t.Clock()
	buf = append(buf, itoa(hour, 2)+":"...)
	buf = append(buf, itoa(min, 2)+":"...)
	buf = append(buf, itoa(sec, 2)...)

	buf = append(buf, '.')
	buf = append(buf, itoa(t.Nanosecond()/1e3, 6)...)

	return string(buf[:])
}

func (l *logger) closeDebug() {
	l.debugSwitch = false
}

func (l *logger) setDebugOutput(o Outputer) {
	l.debugOutputer = o
}

func LogTrace(facilitity int, format string, v ...interface{}) error {
	return gLogger.addlog(TRACE, facilitity, format, v...)
}

func LogDebug(facilitity int, format string, v ...interface{}) error {
	return gLogger.addlog(DEBUG, facilitity, format, v...)
}

func LogInfo(facilitity int, format string, v ...interface{}) error {
	return gLogger.addlog(INFO, facilitity, format, v...)
}

func LogWarn(facilitity int, format string, v ...interface{}) error {
	return gLogger.addlog(WARN, facilitity, format, v...)
}

func LogNotice(facilitity int, format string, v ...interface{}) error {
	return gLogger.addlog(NOTICE, facilitity, format, v...)
}

func LogError(facilitity int, format string, v ...interface{}) error {
	return gLogger.addlog(ERROR, facilitity, format, v...)
}

func LogCrit(facilitity int, format string, v ...interface{}) error {
	return gLogger.addlog(CRIT, facilitity, format, v...)
}

func (l *logger) getLogLvlStr(logType int) string {
	str := ""
	switch logType {
	case EMEGR:
		str = "[EMEGR]"
	case ALERT:
		str = "[ALERT]"
	case CRIT:
		str = "[CRIT]"
	case ERROR:
		str = "[ ERR]"
	case WARN:
		str = "[WARN]"
	case NOTICE:
		str = "[NOTICE]"
	case INFO:
		str = "[INFO]"
	case DEBUG:
		str = "[DEBUG]"
	case TRACE:
		str = "[TRACE]"
	default:
		str = "[DEBUG]"
	}
	return str
}

func (l *logger) getIDStr(ID int) string {
	str := ""
	switch ID {
	case ID_SYS:
		str = "[  SYS]"
	case ID_ADMIN:
		str = "[ADMIN]"
	case ID_SEC:
		str = "[  SEC]"
	case ID_APP:
		str = "[  APP]"
	case ID_DB:
		str = "[   DB]"
	case ID_GAME:
		str = "[ GAME]"
	case ID_USER:
		str = "[ USER]"
	default:
		break
	}
	return str
}

func (l *logger) GetGoID() int32 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return int32(id)
}

func (l *logger) addlog(logLev int, ID int, format string, v ...interface{}) error {
	if logLev > l.starLev {
		return nil
	}

	strLevel := l.getLogLvlStr(logLev)
	strID := l.getIDStr(ID)
	strGoID := fmt.Sprintf("[%05d]", l.GetGoID())

	strTime := l.getTime() + " "
	strFile := "[" + l.getFileLine() + "]"

	var msg string
	if format == "" {
		msg = fmt.Sprint(v...)
	} else {
		msg = fmt.Sprintf(format, v...)
	}

	//[时间][级别][设备][协程ID][文件]日志内容
	strContent := fmt.Sprintf("%s%s%s", strGoID, strFile, msg)
	strLog := fmt.Sprintf("%s%s%s%s", strTime, strLevel, strID, strContent)

	if l.saveFlags&CONSOLE != 0 {
		fmt.Println(strLog)
	}

	if l.saveFlags&REMOTE != 0 {
		if l.remoteLog != nil {
			l.remoteLog.put(uint64(time.Now().UnixNano()/10e5),
				uint8(ID), uint8(logLev), strContent)
		}
	}

	strLog += "\n"
	// l.logChan <- strLog
	l.put(strLog)

	return nil
}

// func (l *logger) popLog() *string {
// 	str := <-l.logChan
// 	return &str
// }

func (l *logger) put(data string) {
	lenData := len(data)
	if 0 == lenData {
		return
	}
	if lenData > max_dataLen_local {
		data = data[:max_dataLen_local]
	}

	l.msgQueueMutex.Lock()
	l.msgQueue = append(l.msgQueue, data)
	if len(l.msgQueue) > max_local_queue_size {
		l.msgQueue = l.msgQueue[1:]
	}
	l.msgQueueMutex.Unlock()
}

func (l *logger) pop_front() string {
	l.msgQueueMutex.Lock()
	defer l.msgQueueMutex.Unlock()
	if len(l.msgQueue) > 0 {
		d := l.msgQueue[0]
		l.msgQueue = l.msgQueue[1:]
		return d
	}
	return ""
}
