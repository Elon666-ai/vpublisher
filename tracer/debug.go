package tracer

import (
	"fmt"
	"os"
	"runtime"
	"time"
)

const (
	maxStack  = 20
	separator = "---------------------------------------\n"
)

func HandlePanic(needLog bool, args ...interface{}) interface{} {
	if err := recover(); err != nil {
		errstr := fmt.Sprintf("%sruntime error: %v\ntraceback:\n", separator, err)

		i := 1
		for {
			pc, file, line, ok := runtime.Caller(i)

			errstr += fmt.Sprintf("    stack: %d %v [file: %s] [func: %s] [line: %d]\n", i, ok, file, runtime.FuncForPC(pc).Name(), line)

			i++
			if !ok || i > maxStack {
				break
			}
		}
		errstr += separator

		if len(args) > 0 {
			cb, ok := args[0].(func())
			if ok {
				defer func() {
					recover()
				}()
				cb()
			}
		}
		return err
	}
	return nil
}

func GetStackInfo() string {
	errstr := fmt.Sprintf("%straceback:\n", separator)

	i := 1
	for {
		pc, file, line, ok := runtime.Caller(i)

		errstr += fmt.Sprintf("    stack: %d %v [file: %s] [func: %s] [line: %d]\n", i, ok, file, runtime.FuncForPC(pc).Name(), line)

		i++
		if !ok || i > maxStack {
			break
		}
	}
	errstr += separator

	return errstr
}

func TryException() {
	errs := recover()
	if errs == nil {
		return
	}
	// exeName := os.Args[0] //获取程序名称

	now := time.Now() //获取当前时间
	// pid := os.Getpid() //获取进程ID

	time_str := now.Format("20060102-150405")
	fname := fmt.Sprintf("dump_%s.log", time_str)
	fmt.Println("dumpToFile:", fname)
	fmt.Println(GetStackInfo())

	f, err := os.Create(fname)
	if err != nil {
		return
	}
	defer f.Close()

	f.WriteString(fmt.Sprintf("%v\r\n", errs)) //输出panic信息
	f.WriteString("========\r\n")
	f.WriteString(GetStackInfo()) //输出堆栈信息
}
