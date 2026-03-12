package ws

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"vpublisher/tracer"
)

const (
	retryDelayMin      = 3 * time.Second
	retryDelayMax      = 60 * time.Second
	retryResetAfterRun = 30 * time.Second
	stallCheckInterval = 3 * time.Second
	stallTimeout       = 20 * time.Second
	sharedWorkerKey    = "__shared__"
)

type FFmpegConfig struct {
	InputFile   string
	TargetURLs  []string
	VideoLayout string
}

type ffmpegWorker struct {
	targetURL  string
	targetURLs []string

	mu        sync.Mutex
	cmd       *exec.Cmd
	running   bool
	stopCh    chan struct{}
	doneCh    chan struct{}
	startedAt time.Time

	lastPtsFnMs atomic.Int64
}

type FFmpegManager struct {
	mu      sync.Mutex
	cfg     FFmpegConfig
	workers map[string]*ffmpegWorker
	paused  map[string]bool
}

var publisherMgr = &FFmpegManager{
	workers: make(map[string]*ffmpegWorker),
	paused:  make(map[string]bool),
}

func GetCurrentEncodingPtsFnMs() (int64, error) {
	publisherMgr.mu.Lock()
	workers := make([]*ffmpegWorker, 0, len(publisherMgr.workers))
	for _, w := range publisherMgr.workers {
		workers = append(workers, w)
	}
	publisherMgr.mu.Unlock()

	var anyRunning bool
	var maxPts int64
	for _, w := range workers {
		running, pts, startedAt := w.snapshot()
		if !running {
			continue
		}
		anyRunning = true
		if pts <= 0 {
			pts = time.Since(startedAt).Milliseconds()
		}
		if pts > maxPts {
			maxPts = pts
		}
	}

	if !anyRunning {
		return 0, errors.New("publisher is not running")
	}
	if maxPts <= 0 {
		return 0, errors.New("publisher started but pts is unavailable")
	}
	return maxPts, nil
}

func InitFFmpegPublisher(cfg FFmpegConfig) {
	publisherMgr.mu.Lock()
	defer publisherMgr.mu.Unlock()

	cfg.TargetURLs = uniqueNonEmpty(cfg.TargetURLs)
	publisherMgr.cfg = cfg
	useShared := shouldUseSharedFFmpeg(cfg.InputFile, cfg.TargetURLs)

	if useShared {
		w, ok := publisherMgr.workers[sharedWorkerKey]
		if !ok || w == nil {
			w = &ffmpegWorker{targetURL: sharedWorkerKey}
			publisherMgr.workers[sharedWorkerKey] = w
		}
		w.targetURLs = append([]string(nil), cfg.TargetURLs...)

		for key, worker := range publisherMgr.workers {
			if key == sharedWorkerKey || worker == nil {
				continue
			}
			stopCh, doneCh, _ := worker.stopLocked()
			delete(publisherMgr.workers, key)
			if stopCh != nil {
				close(stopCh)
			}
			if doneCh != nil {
				select {
				case <-doneCh:
				case <-time.After(5 * time.Second):
				}
			}
		}
		_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] shared process mode enabled for live input, outputs=%d", len(cfg.TargetURLs))
		return
	}

	newSet := make(map[string]struct{}, len(cfg.TargetURLs))
	for _, url := range cfg.TargetURLs {
		newSet[url] = struct{}{}
		if _, ok := publisherMgr.workers[url]; !ok {
			publisherMgr.workers[url] = &ffmpegWorker{targetURL: url}
		}
	}

	for url, w := range publisherMgr.workers {
		if _, ok := newSet[url]; !ok {
			stopCh, doneCh, _ := w.stopLocked()
			publisherMgr.workers[url] = nil
			delete(publisherMgr.workers, url)
			if stopCh != nil {
				close(stopCh)
			}
			if doneCh != nil {
				select {
				case <-doneCh:
				case <-time.After(5 * time.Second):
				}
			}
		}
	}
}

func StartFFmpegPublisher() error {
	publisherMgr.mu.Lock()
	defer publisherMgr.mu.Unlock()

	if publisherMgr.cfg.InputFile == "" || len(publisherMgr.cfg.TargetURLs) == 0 {
		return errors.New("ffmpeg config is not initialized")
	}

	if shouldUseSharedFFmpeg(publisherMgr.cfg.InputFile, publisherMgr.cfg.TargetURLs) {
		activeTargets := make([]string, 0, len(publisherMgr.cfg.TargetURLs))
		for _, url := range publisherMgr.cfg.TargetURLs {
			if publisherMgr.paused[url] {
				_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] start skipped for paused target=%s", url)
				continue
			}
			activeTargets = append(activeTargets, url)
		}
		if len(activeTargets) == 0 {
			_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] all shared-mode targets are paused, skip start")
			return nil
		}

		w := publisherMgr.workers[sharedWorkerKey]
		if w == nil {
			w = &ffmpegWorker{targetURL: sharedWorkerKey}
			publisherMgr.workers[sharedWorkerKey] = w
		}
		w.targetURLs = append([]string(nil), activeTargets...)
		if err := w.startLocked(publisherMgr.cfg.InputFile); err != nil {
			return err
		}
		_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] shared process started for %d targets", len(activeTargets))
		return nil
	}

	var firstErr error
	for _, url := range publisherMgr.cfg.TargetURLs {
		if publisherMgr.paused[url] {
			_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] start skipped for paused target=%s", url)
			continue
		}
		w := publisherMgr.workers[url]
		if w == nil {
			w = &ffmpegWorker{targetURL: url}
			publisherMgr.workers[url] = w
		}
		if err := w.startLocked(publisherMgr.cfg.InputFile); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			_ = tracer.LogWarn(tracer.ID_APP, "[FFMPEG] failed to start target=%s: %v", url, err)
		}
	}
	if firstErr == nil {
		_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] start requested for %d targets", len(publisherMgr.cfg.TargetURLs))
	}
	return firstErr
}

func StopFFmpegPublisher() error {
	publisherMgr.mu.Lock()
	workers := make([]*ffmpegWorker, 0, len(publisherMgr.workers))
	for _, w := range publisherMgr.workers {
		if w != nil {
			workers = append(workers, w)
		}
	}
	publisherMgr.mu.Unlock()

	for _, w := range workers {
		stopCh, doneCh, cmd := w.stopLocked()
		if stopCh != nil {
			close(stopCh)
		}
		if cmd != nil && cmd.Process != nil {
			if err := killProcess(cmd); err != nil {
				_ = tracer.LogWarn(tracer.ID_APP, "[FFMPEG] failed to kill process target=%s: %v", w.targetURL, err)
			}
		}
		if doneCh != nil {
			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				_ = tracer.LogWarn(tracer.ID_APP, "[FFMPEG] stop timeout target=%s", w.targetURL)
			}
		}
	}
	return nil
}

func PauseFFmpegPublisher(reason string) error {
	publisherMgr.mu.Lock()
	for _, url := range publisherMgr.cfg.TargetURLs {
		publisherMgr.paused[url] = true
	}
	publisherMgr.mu.Unlock()

	_ = tracer.LogWarn(tracer.ID_APP, "[FFMPEG] all targets paused: %s", reason)
	return StopFFmpegPublisher()
}

func ResumeFFmpegPublisher(reason string) error {
	publisherMgr.mu.Lock()
	for _, url := range publisherMgr.cfg.TargetURLs {
		delete(publisherMgr.paused, url)
	}
	publisherMgr.mu.Unlock()

	_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] all targets resumed: %s", reason)
	return StartFFmpegPublisher()
}

func PauseFFmpegPublisherByURL(targetURL, reason string) error {
	targetURL = strings.TrimSpace(targetURL)
	if targetURL == "" {
		return errors.New("publishUrl is empty")
	}

	publisherMgr.mu.Lock()
	matches := publisherMgr.resolveTargetsLocked(targetURL)
	useSharedMode := shouldUseSharedFFmpeg(publisherMgr.cfg.InputFile, publisherMgr.cfg.TargetURLs)
	for _, m := range matches {
		publisherMgr.paused[m] = true
	}
	workers := make([]*ffmpegWorker, 0, len(matches))
	for _, m := range matches {
		if w := publisherMgr.workers[m]; w != nil {
			workers = append(workers, w)
		}
	}
	publisherMgr.mu.Unlock()

	if len(matches) == 0 {
		return errors.New("target publishUrl is not configured")
	}
	if useSharedMode {
		_ = tracer.LogWarn(tracer.ID_APP, "[FFMPEG] shared process mode: restart publisher to apply target pause")
		if err := StopFFmpegPublisher(); err != nil {
			return err
		}
		return StartFFmpegPublisher()
	}

	_ = tracer.LogWarn(tracer.ID_APP, "[FFMPEG] target paused: input=%s matched=%s reason=%s",
		targetURL, strings.Join(matches, ","), reason)

	for _, w := range workers {
		stopCh, doneCh, cmd := w.stopLocked()
		if stopCh != nil {
			close(stopCh)
		}
		if cmd != nil && cmd.Process != nil {
			if err := killProcess(cmd); err != nil {
				return err
			}
		}
		if doneCh != nil {
			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				_ = tracer.LogWarn(tracer.ID_APP, "[FFMPEG] pause wait timeout target=%s", w.targetURL)
			}
		}
	}
	return nil
}

func ResumeFFmpegPublisherByURL(targetURL, reason string) error {
	targetURL = strings.TrimSpace(targetURL)
	if targetURL == "" {
		return errors.New("publishUrl is empty")
	}

	publisherMgr.mu.Lock()
	matches := publisherMgr.resolveTargetsLocked(targetURL)
	useSharedMode := shouldUseSharedFFmpeg(publisherMgr.cfg.InputFile, publisherMgr.cfg.TargetURLs)
	for _, m := range matches {
		delete(publisherMgr.paused, m)
	}
	workers := make([]*ffmpegWorker, 0, len(matches))
	for _, m := range matches {
		if w := publisherMgr.workers[m]; w != nil {
			workers = append(workers, w)
		}
	}
	inputFile := publisherMgr.cfg.InputFile
	publisherMgr.mu.Unlock()

	if len(matches) == 0 {
		return errors.New("target publishUrl is not configured")
	}
	if useSharedMode {
		_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] shared process mode: restart publisher to apply target resume")
		if err := StopFFmpegPublisher(); err != nil {
			return err
		}
		return StartFFmpegPublisher()
	}

	_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] target resumed: input=%s matched=%s reason=%s",
		targetURL, strings.Join(matches, ","), reason)

	var firstErr error
	for _, w := range workers {
		if err := w.startLocked(inputFile); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *FFmpegManager) resolveTargetsLocked(target string) []string {
	target = strings.TrimSpace(target)
	if target == "" {
		return nil
	}

	seen := make(map[string]struct{})
	add := func(v string) {
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
	}

	for _, configured := range m.cfg.TargetURLs {
		if configured == target {
			add(configured)
		}
	}
	if len(seen) > 0 {
		return mapKeysInOrder(m.cfg.TargetURLs, seen)
	}

	targetBase := normalizeURLBase(target)
	for _, configured := range m.cfg.TargetURLs {
		if normalizeURLBase(configured) == targetBase && targetBase != "" {
			add(configured)
			continue
		}
		if strings.HasPrefix(configured, target+"?") {
			add(configured)
		}
	}

	return mapKeysInOrder(m.cfg.TargetURLs, seen)
}

func normalizeURLBase(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host) + u.Path
}

func mapKeysInOrder(order []string, selected map[string]struct{}) []string {
	out := make([]string, 0, len(selected))
	for _, v := range order {
		if _, ok := selected[v]; ok {
			out = append(out, v)
		}
	}
	return out
}

func (w *ffmpegWorker) startLocked(inputFile string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if inputFile == "" {
		return errors.New("input file is empty")
	}
	if w.running {
		_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] start ignored, target already running: %s", w.targetURL)
		return nil
	}

	w.running = true
	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})
	go w.runLoop(inputFile)
	return nil
}

func (w *ffmpegWorker) stopLocked() (chan struct{}, chan struct{}, *exec.Cmd) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.running {
		return nil, nil, nil
	}
	return w.stopCh, w.doneCh, w.cmd
}

func (w *ffmpegWorker) snapshot() (bool, int64, time.Time) {
	w.mu.Lock()
	running := w.running
	startedAt := w.startedAt
	w.mu.Unlock()
	return running, w.lastPtsFnMs.Load(), startedAt
}

func (w *ffmpegWorker) runLoop(inputFile string) {
	defer tracer.TryException()
	defer func() {
		w.mu.Lock()
		w.running = false
		w.cmd = nil
		w.startedAt = time.Time{}
		w.lastPtsFnMs.Store(0)
		if w.doneCh != nil {
			close(w.doneCh)
		}
		w.mu.Unlock()
	}()

	retryDelay := retryDelayMin
	retryAttempt := 0

	for {
		if w.isPaused() {
			_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] target paused, exit run loop: %s", w.targetURL)
			return
		}

		select {
		case <-w.stopCh:
			return
		default:
		}

		targets := append([]string(nil), w.targetURLs...)
		if len(targets) == 0 {
			targets = []string{w.targetURL}
		}
		layout := "portrait"
		publisherMgr.mu.Lock()
		if strings.TrimSpace(publisherMgr.cfg.VideoLayout) != "" {
			layout = publisherMgr.cfg.VideoLayout
		}
		publisherMgr.mu.Unlock()
		cfg := FFmpegConfig{InputFile: inputFile, TargetURLs: targets, VideoLayout: layout}
		cmd, err := buildFFmpegCommand(cfg)
		if err != nil {
			retryAttempt++
			_ = tracer.LogWarn(tracer.ID_APP, "[FFMPEG] build failed target=%s: %v", w.targetURL, err)
			if !w.waitRetry(retryAttempt, retryDelay, "build_failed") {
				return
			}
			retryDelay = minDuration(retryDelay*2, retryDelayMax)
			continue
		}

		stderr, err := cmd.StderrPipe()
		if err != nil {
			retryAttempt++
			_ = tracer.LogWarn(tracer.ID_APP, "[FFMPEG] stderr pipe failed target=%s: %v", w.targetURL, err)
			if !w.waitRetry(retryAttempt, retryDelay, "stderr_pipe_failed") {
				return
			}
			retryDelay = minDuration(retryDelay*2, retryDelayMax)
			continue
		}

		if err := cmd.Start(); err != nil {
			retryAttempt++
			_ = tracer.LogWarn(tracer.ID_APP, "[FFMPEG] start failed target=%s: %v", w.targetURL, err)
			if !w.waitRetry(retryAttempt, retryDelay, "start_failed") {
				return
			}
			retryDelay = minDuration(retryDelay*2, retryDelayMax)
			continue
		}

		startedAt := time.Now()
		w.mu.Lock()
		w.cmd = cmd
		w.startedAt = startedAt
		w.lastPtsFnMs.Store(0)
		w.mu.Unlock()
		_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] process started target=%s pid=%d", w.targetURL, cmd.Process.Pid)

		parseDoneCh := make(chan struct{})
		go w.trackPtsFromFFmpegProgress(stderr, parseDoneCh)
		procDone := make(chan struct{})
		go w.monitorStallAndKill(cmd, startedAt, procDone)

		err = cmd.Wait()
		close(procDone)
		select {
		case <-parseDoneCh:
		case <-time.After(500 * time.Millisecond):
		}

		if err != nil {
			_ = tracer.LogWarn(tracer.ID_APP, "[FFMPEG] process exited target=%s err=%v", w.targetURL, err)
		}

		if time.Since(startedAt) >= retryResetAfterRun {
			retryDelay = retryDelayMin
			retryAttempt = 0
		}

		if w.isPaused() {
			return
		}
		select {
		case <-w.stopCh:
			return
		default:
		}

		retryAttempt++
		if !w.waitRetry(retryAttempt, retryDelay, "process_exit") {
			return
		}
		retryDelay = minDuration(retryDelay*2, retryDelayMax)
	}
}

func (w *ffmpegWorker) isPaused() bool {
	publisherMgr.mu.Lock()
	defer publisherMgr.mu.Unlock()
	return publisherMgr.paused[w.targetURL]
}

func (w *ffmpegWorker) waitRetry(attempt int, delay time.Duration, reason string) bool {
	if delay < retryDelayMin {
		delay = retryDelayMin
	}
	if delay > retryDelayMax {
		delay = retryDelayMax
	}
	_ = tracer.LogInfo(tracer.ID_APP,
		"[FFMPEG] retry scheduled: target=%s attempt=%d reason=%s delay=%s",
		w.targetURL, attempt, reason, delay)

	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-w.stopCh:
		return false
	case <-t.C:
		return true
	}
}

func (w *ffmpegWorker) monitorStallAndKill(cmd *exec.Cmd, startedAt time.Time, procDone <-chan struct{}) {
	ticker := time.NewTicker(stallCheckInterval)
	defer ticker.Stop()

	lastPts := w.lastPtsFnMs.Load()
	lastProgressAt := time.Now()

	for {
		select {
		case <-procDone:
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
		}

		if time.Since(startedAt) < stallTimeout {
			continue
		}

		curPts := w.lastPtsFnMs.Load()
		if curPts > lastPts {
			lastPts = curPts
			lastProgressAt = time.Now()
			continue
		}

		if time.Since(lastProgressAt) >= stallTimeout {
			_ = tracer.LogWarn(tracer.ID_APP,
				"[FFMPEG] stalled target=%s no pts progress for %s, killing pid=%d",
				w.targetURL, stallTimeout, cmd.Process.Pid)
			_ = killProcess(cmd)
			return
		}
	}
}

func (w *ffmpegWorker) trackPtsFromFFmpegProgress(stderr io.ReadCloser, doneCh chan struct{}) {
	defer close(doneCh)
	defer stderr.Close()

	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "out_time_us=") {
			raw := strings.TrimPrefix(line, "out_time_us=")
			if us, err := strconv.ParseInt(raw, 10, 64); err == nil && us >= 0 {
				w.lastPtsFnMs.Store(us / 1000)
			}
			continue
		}
		if strings.HasPrefix(line, "out_time_ms=") {
			raw := strings.TrimPrefix(line, "out_time_ms=")
			if us, err := strconv.ParseInt(raw, 10, 64); err == nil && us >= 0 {
				w.lastPtsFnMs.Store(us / 1000)
			}
			continue
		}
		// _ = tracer.LogWarn(tracer.ID_APP, "[FFMPEG][%s] %s", w.targetURL, line)
	}
}

func uniqueNonEmpty(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	ret := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		ret = append(ret, v)
	}
	return ret
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func killProcess(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}

	if err := cmd.Process.Kill(); err == nil {
		return nil
	}

	if runtime.GOOS == "windows" {
		taskkill := exec.Command("taskkill", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F")
		if err := taskkill.Run(); err != nil {
			return err
		}
		return nil
	}

	return cmd.Process.Kill()
}

func buildFFmpegCommand(cfg FFmpegConfig) (*exec.Cmd, error) {
	var args []string

	isDShowInput := runtime.GOOS == "windows" && isDShowInputSpec(cfg.InputFile)

	args = append(args,
		"-hide_banner", "-loglevel", "error",
		"-progress", "pipe:2", "-stats_period", "0.5",
		"-re",
	)
	if !isDShowInput {
		args = append(args, "-stream_loop", "-1")
	}

	if runtime.GOOS == "windows" && !isDShowInput {
		inputArgs := []string{"-i", cfg.InputFile}
		args = append(args,
			"-init_hw_device", "qsv=hw",
			"-filter_hw_device", "hw",
		)
		args = append(args, inputArgs...)
		args = appendVideoRenditions(args, "h264_qsv", cfg.VideoLayout)
	} else {
		inputArgs := []string{"-i", cfg.InputFile}
		if runtime.GOOS == "windows" && isDShowInput {
			_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] dshow input detected on Windows, using libx264 software encoding")
			videoReq, audioReq := parseDShowInputSpec(cfg.InputFile)
			videoName, audioName := videoReq, audioReq
			videoDevices, audioDevices, probeErr := listDShowDevices()
			if probeErr != nil {
				_ = tracer.LogWarn(tracer.ID_APP, "[FFMPEG] failed to probe dshow devices: %v", probeErr)
			} else {
				if len(videoDevices) > 0 {
					_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] dshow video devices: %s", strings.Join(videoDevices, " | "))
				}
				if len(audioDevices) > 0 {
					_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] dshow audio devices: %s", strings.Join(audioDevices, " | "))
				}

				videoName = resolveDShowDeviceName(videoReq, videoDevices)
				audioName = resolveDShowDeviceName(audioReq, audioDevices)

				if videoReq != "" && videoName != videoReq {
					_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] dshow video device remapped: %q -> %q", videoReq, videoName)
				}
				if audioReq != "" && audioName != audioReq {
					_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] dshow audio device remapped: %q -> %q", audioReq, audioName)
				}
			}

			inputSpec := buildDShowInputSpec(videoName, audioName)
			inputArgs = []string{"-f", "dshow", "-i", inputSpec}
		} else if runtime.GOOS != "windows" {
			_ = tracer.LogInfo(tracer.ID_APP, "[FFMPEG] running on Linux, utilizing libx264 (software)")
		}
		args = append(args, inputArgs...)
		args = appendVideoRenditions(args, "libx264", cfg.VideoLayout)
	}

	args = append(args,
		"-map", "0:a:0?",
		"-c:a", "libopus",
		"-b:a", "64k",
		"-ar", "48000",
		"-ac", "2",
	)

	if len(cfg.TargetURLs) == 0 {
		return nil, errors.New("target url is empty")
	}
	if len(cfg.TargetURLs) == 1 {
		args = append(args, "-f", "mpegts", cfg.TargetURLs[0])
	} else {
		teeTargets := make([]string, 0, len(cfg.TargetURLs))
		for _, targetURL := range cfg.TargetURLs {
			teeTargets = append(teeTargets, "[f=mpegts]"+targetURL)
		}
		args = append(args, "-f", "tee", strings.Join(teeTargets, "|"))
	}

	cmd := exec.Command("ffmpeg", args...)
	cmd.Stdout = io.Discard
	return cmd, nil
}

type videoRendition struct {
	width   int
	height  int
	bitrate string
	maxrate string
}

func appendVideoRenditions(args []string, codec, layout string) []string {
	renditions := buildVideoRenditions(layout)
	for i, r := range renditions {
		args = append(args,
			"-map", "0:v:0", fmt.Sprintf("-c:v:%d", i), codec,
			fmt.Sprintf("-b:v:%d", i), r.bitrate, fmt.Sprintf("-maxrate:v:%d", i), r.maxrate,
			fmt.Sprintf("-profile:v:%d", i), "high", fmt.Sprintf("-bf:v:%d", i), "0", fmt.Sprintf("-g:v:%d", i), "30",
			fmt.Sprintf("-filter:v:%d", i), fmt.Sprintf("scale=%d:%d", r.width, r.height),
		)
	}
	return args
}

func buildVideoRenditions(layout string) []videoRendition {
	layout = strings.ToLower(strings.TrimSpace(layout))
	portrait := []videoRendition{
		{width: 1080, height: 1920, bitrate: "2000k", maxrate: "3000k"},
		{width: 720, height: 1280, bitrate: "1000k", maxrate: "1500k"},
		{width: 540, height: 960, bitrate: "400k", maxrate: "600k"},
	}
	landscape := []videoRendition{
		{width: 1920, height: 1080, bitrate: "2000k", maxrate: "3000k"},
		{width: 1280, height: 720, bitrate: "1000k", maxrate: "1500k"},
		{width: 960, height: 540, bitrate: "400k", maxrate: "600k"},
	}

	switch layout {
	case "landscape":
		return landscape
	case "both":
		out := make([]videoRendition, 0, len(portrait)+len(landscape))
		out = append(out, portrait...)
		out = append(out, landscape...)
		return out
	default:
		return portrait
	}
}

func parseDShowInputSpec(input string) (string, string) {
	var videoName string
	var audioName string

	parts := strings.Split(input, ":")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lower := strings.ToLower(part)
		if strings.HasPrefix(lower, "video=") {
			videoName = trimDShowDeviceValue(part[len("video="):])
			continue
		}
		if strings.HasPrefix(lower, "audio=") {
			audioName = trimDShowDeviceValue(part[len("audio="):])
		}
	}
	return videoName, audioName
}

func trimDShowDeviceValue(v string) string {
	v = strings.TrimSpace(v)
	if len(v) >= 2 && strings.HasPrefix(v, "\"") && strings.HasSuffix(v, "\"") {
		v = v[1 : len(v)-1]
	}
	return strings.TrimSpace(v)
}

func buildDShowInputSpec(videoName, audioName string) string {
	parts := make([]string, 0, 2)
	if strings.TrimSpace(videoName) != "" {
		parts = append(parts, "video="+strings.TrimSpace(videoName))
	}
	if strings.TrimSpace(audioName) != "" {
		parts = append(parts, "audio="+strings.TrimSpace(audioName))
	}
	return strings.Join(parts, ":")
}

func listDShowDevices() ([]string, []string, error) {
	cmd := exec.Command("ffmpeg", "-hide_banner", "-list_devices", "true", "-f", "dshow", "-i", "dummy")
	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr
	err := cmd.Run()

	videoDevices, audioDevices := parseDShowDevices(stderr.String())
	if len(videoDevices) == 0 && len(audioDevices) == 0 && err != nil {
		return nil, nil, err
	}
	return videoDevices, audioDevices, nil
}

func parseDShowDevices(output string) ([]string, []string) {
	videoDevices := make([]string, 0)
	audioDevices := make([]string, 0)
	seenVideo := map[string]struct{}{}
	seenAudio := map[string]struct{}{}

	section := ""
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		lower := strings.ToLower(line)
		switch {
		case strings.Contains(lower, "directshow video devices"):
			section = "video"
			continue
		case strings.Contains(lower, "directshow audio devices"):
			section = "audio"
			continue
		}

		first := strings.Index(line, "\"")
		if first < 0 {
			continue
		}
		rest := line[first+1:]
		second := strings.Index(rest, "\"")
		if second < 0 {
			continue
		}
		name := strings.TrimSpace(rest[:second])
		if name == "" {
			continue
		}

		switch section {
		case "video":
			if _, ok := seenVideo[name]; ok {
				continue
			}
			seenVideo[name] = struct{}{}
			videoDevices = append(videoDevices, name)
		case "audio":
			if _, ok := seenAudio[name]; ok {
				continue
			}
			seenAudio[name] = struct{}{}
			audioDevices = append(audioDevices, name)
		}
	}

	return videoDevices, audioDevices
}

func resolveDShowDeviceName(requested string, devices []string) string {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return ""
	}
	if len(devices) == 0 {
		return requested
	}

	reqLower := strings.ToLower(requested)
	for _, dev := range devices {
		if strings.EqualFold(strings.TrimSpace(dev), requested) {
			return dev
		}
	}

	matches := make([]string, 0, 2)
	for _, dev := range devices {
		devLower := strings.ToLower(strings.TrimSpace(dev))
		if strings.Contains(devLower, reqLower) || strings.Contains(reqLower, devLower) {
			matches = append(matches, dev)
		}
	}
	if len(matches) == 1 {
		return matches[0]
	}

	return requested
}

func isDShowInputSpec(input string) bool {
	v := strings.TrimSpace(strings.ToLower(input))
	return strings.HasPrefix(v, "video=") || strings.HasPrefix(v, "audio=")
}

func shouldUseSharedFFmpeg(inputFile string, targets []string) bool {
	if len(targets) <= 1 {
		return false
	}
	// Live capture input (dshow) can usually be opened by only one ffmpeg process.
	return runtime.GOOS == "windows" && isDShowInputSpec(inputFile)
}
