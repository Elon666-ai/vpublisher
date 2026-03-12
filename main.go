package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"vpublisher/conf"
	"vpublisher/tracer"
	"vpublisher/utils"
	"vpublisher/ws"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func main() {

	confFile, env := utils.GetConfigFile()
	if !utils.CheckFileExist(confFile) {
		fmt.Printf("%s not found! exit\n", confFile)
		return
	}

	cfg, err := conf.Load(confFile)
	if err != nil {
		fmt.Printf("load %s failed: %v", confFile, err)
		return
	}
	tracer.InitLog(tracer.DEBUG, fmt.Sprintf("%s%02d", cfg.WorkerType, cfg.WorkerID))
	tracer.LogInfo(tracer.ID_APP, "env=%s, load %s success!", env, confFile)

	targetURLs, wsPublishURL, err := resolvePublishTargets(cfg)
	if err != nil {
		tracer.LogError(tracer.ID_APP, "resolve publish url failed: %v", err)
		os.Exit(1)
	}
	tracer.LogInfo(tracer.ID_APP, "starting vpublisher version=%s buildTime=%s commit=%s worker=%s#%d region=%s",
		Version, BuildTime, GitCommit, cfg.WorkerType, cfg.WorkerID, cfg.WorkerRegion)
	tracer.LogInfo(tracer.ID_APP, "videoLayout=%s", cfg.VideoLayout)

	go ws.WebsocketClientThread(
		cfg.WorkerType,
		cfg.WorkerRegion,
		cfg.WorkerMgrAddr,
		wsPublishURL,
		cfg.WorkerID,
	)

	ffCfg := ws.FFmpegConfig{
		InputFile:   cfg.InputFile,
		TargetURLs:  targetURLs,
		VideoLayout: cfg.VideoLayout,
	}
	ws.InitFFmpegPublisher(ffCfg)
	if cfg.PublishOnReady {
		if err := ws.StartFFmpegPublisher(); err != nil {
			_ = tracer.LogWarn(tracer.ID_APP, "publishOnReady is true but start publisher failed: %v", err)
		} else {
			_ = tracer.LogInfo(tracer.ID_APP, "publishOnReady is true, publisher started")
		}
	} else {
		_ = tracer.LogInfo(tracer.ID_APP, "ffmpeg publisher initialized, waiting for websocket command %s/%s", utils.CMD_TYPE_startPub, utils.CMD_TYPE_stopPub)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	s := <-sigCh
	_ = tracer.LogNotice(tracer.ID_APP, "received signal: %v, shutting down...", s)
	_ = ws.StopFFmpegPublisher()
	ws.CloseWebsocketMainThread()
}

func resolvePublishTargets(cfg *conf.Config) ([]string, string, error) {
	rawTargets := []string{cfg.PublishURL, cfg.PublishURL2}
	targets := make([]string, 0, 2)
	seen := make(map[string]struct{}, 2)

	for _, raw := range rawTargets {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		resolved, err := utils.ResolvePublishURL(cfg.AuthAPIAddr, cfg.AppSecret, raw)
		if err != nil {
			return nil, "", fmt.Errorf("resolve %s by %s failed: %w", raw, cfg.AuthAPIAddr, err)
		}

		if cfg.AuthAPIAddr != "" {
			tracer.LogInfo(tracer.ID_APP, "publishUrl resolved: %s", resolved)
		}

		if _, ok := seen[resolved]; ok {
			continue
		}
		seen[resolved] = struct{}{}
		targets = append(targets, resolved)
	}

	if len(targets) == 0 {
		return nil, "", fmt.Errorf("publish targets are empty")
	}

	return targets, targets[0], nil
}
