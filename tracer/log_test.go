package tracer

import (
	"testing"
	"time"
)

func TestInitLog(t *testing.T) {
	InitLog(DEBUG, "test")
	if gLogger == nil {
		t.Fatal("logger should be initialized")
	}
	if err := LogInfo(ID_APP, "smoke test"); err != nil {
		t.Fatalf("log write failed: %v", err)
	}
}

func TestInitLogReinit(t *testing.T) {
	InitLog(DEBUG, "test_a")
	old := gLogger
	if old == nil || old.logFd == nil {
		t.Fatal("old logger should be initialized")
	}

	InitLog(DEBUG, "test_b")
	if gLogger == nil || gLogger.logFd == nil {
		t.Fatal("new logger should be initialized")
	}
	if gLogger == old {
		t.Fatal("logger instance should be replaced on re-init")
	}
	if old.logFd != nil {
		t.Fatal("old logger file descriptor should be closed and cleared")
	}

	if err := LogInfo(ID_APP, "reinit smoke test"); err != nil {
		t.Fatalf("log write failed after re-init: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
}
