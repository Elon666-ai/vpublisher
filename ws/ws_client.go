package ws

import (
	"container/list"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"vpublisher/pb3"
	"vpublisher/tracer"
	"vpublisher/utils"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

type NotificationPack struct {
	Event string
	Act   string
	Idx   int
	Buf   []byte
}

func MsgToMessenger(event, act string, idx int, buf []byte) {
	lockWebsocket.RLock()
	defer lockWebsocket.RUnlock()

	for _, wc := range mapWsClient {
		if len(wc.chanWebSocket) < cap(wc.chanWebSocket) {
			wc.chanWebSocket <- &NotificationPack{
				Event: event,
				Act:   act,
				Idx:   idx,
				Buf:   buf,
			}
		}
		tracer.LogTrace(tracer.ID_APP, "send (%s:%s) to worker:%s", event, act, wc.nodeMgrAddr)
	}
}

var mapWsClient map[string]*ReconnectClient = make(map[string]*ReconnectClient)
var lockWebsocket = &sync.RWMutex{}

func RestartWebsocketMainThread() {
	lockWebsocket.RLock()
	workers := make([]*ReconnectClient, 0, len(mapWsClient))
	for _, wc := range mapWsClient {
		workers = append(workers, wc)
	}
	lockWebsocket.RUnlock()

	for _, wc := range workers {
		wc.closeWsClient()
		wc.wg.Wait()
		go WebsocketClientThread(wc.nodeType, wc.nodeRegion, wc.nodeMgrAddr, wc.publishUrl, wc.nodeId)
	}

	tracer.LogDebug(tracer.ID_APP, "RestartWebsocketMainThread done!")
}

func CloseWebsocketMainThread() {
	lockWebsocket.RLock()
	workers := make([]*ReconnectClient, 0, len(mapWsClient))
	for _, wc := range mapWsClient {
		workers = append(workers, wc)
	}
	lockWebsocket.RUnlock()

	for _, wc := range workers {
		wc.closeWsClient()
		wc.wg.Wait()
	}
}

/*
主线程，四个sub-routines: read/write/heatbeat/reconnect.
r/w IO出错时，4个协程只需要return即可。conn的重新连接由reconnectLoop来唯一触发。
main-thread回收conn资源和go routine资源。

修复：使用 stopCh + sync.Once 确保每个连接周期只有一对 readLoop/heartbeatLoop，
重连时先关闭旧连接和旧协程，再启动新的，避免协程泄漏导致多个heartbeat并存。
*/
func WebsocketClientThread(nodeType, nodeRegion, nodeMgrAddr, publishUrl string, nodeId int) {
	defer tracer.TryException()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	tracer.LogDebug(tracer.ID_APP, "[%d]connecting with %s", nodeId, nodeMgrAddr)

	wc := NewReconnectClient(nodeType, nodeRegion, nodeMgrAddr, publishUrl, nodeId)
	if err := wc.Start(); err != nil {
		tracer.LogWarn(tracer.ID_APP, "[%d]Failed to start ws-clt! %v", nodeId, err)
	}
	lockWebsocket.Lock()
	mapWsClient[nodeMgrAddr] = wc
	lockWebsocket.Unlock()
	wc.wg.Add(1)
	defer wc.wg.Done()
	tracer.LogDebug(tracer.ID_APP, "[%d] websocket client created for %s", wc.nodeId, wc.nodeMgrAddr)

outer:
	for {
		select {
		case <-wc.doneCh:
			_ = tracer.LogNotice(tracer.ID_APP, "done signal, WebsocketClientThread[%d] exit loop", wc.nodeId)
			wc.connMu.Lock()
			if wc.conn != nil {
				wc.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client shutdown"))
				wc.conn.Close()
			}
			wc.connMu.Unlock()
			break outer

		case <-interrupt:
			_ = tracer.LogNotice(tracer.ID_APP, "interrupt signal, WebsocketClientThread[%d] exit loop", wc.nodeId)
			break outer
		}
	}

	wc.reconnect.Store(false)
	tracer.LogDebug(tracer.ID_APP, "WebsocketClientThread[%d] exit!", wc.nodeId)
}

type Queue struct {
	items *list.List
}

func NewQueue() *Queue {
	return &Queue{items: list.New()}
}

func (q *Queue) Enqueue(item interface{}) {
	if q.items.Len() < 256 {
		q.items.PushBack(item)
	}
}

func (q *Queue) Dequeue() (interface{}, error) {
	if q.items.Len() == 0 {
		return nil, errors.New("queue is empty")
	}

	front := q.items.Front()
	q.items.Remove(front)
	return front.Value, nil
}

func (q *Queue) IsEmpty() bool {
	return q.items.Len() == 0
}

func (q *Queue) Size() int {
	return q.items.Len()
}

func (q *Queue) Peek() (interface{}, error) {
	if q.items.Len() == 0 {
		return nil, errors.New("queue is empty")
	}
	return q.items.Front().Value, nil
}

type ReconnectClient struct {
	nodeType     string
	nodeId       int
	nodeRegion   string
	nodeMgrAddr  string
	nodeCapacity int
	publishUrl   string

	chanWebSocket        chan *NotificationPack
	wg                   *sync.WaitGroup
	conn                 *websocket.Conn
	connMu               sync.Mutex // protects conn field
	reconnect            atomic.Bool
	reconnectCh          chan struct{} // buffered(1), signals reconnectLoop
	doneCh               chan struct{} // closed on shutdown
	doneOnce             sync.Once
	stopCh               chan struct{} // per-connection lifecycle, closed to stop readLoop+heartbeatLoop
	triggerOnce          sync.Once     // ensures only one goroutine triggers reconnect per connection
	reconnectInterval    time.Duration
	maxReconnectInterval time.Duration
	wsErrCnt             int
	lockWsErrCnt         *sync.RWMutex
	que                  *Queue
	writeMu              *sync.Mutex
}

func NewReconnectClient(nodeType, nodeRegion, nodeMgrAddr, publishUrl string, nodeId int) *ReconnectClient {
	var nodeCap int = utils.NODE_CAPACITY_publisher

	c := &ReconnectClient{
		nodeType:             nodeType,
		nodeRegion:           nodeRegion,
		nodeId:               nodeId,
		nodeMgrAddr:          nodeMgrAddr,
		nodeCapacity:         nodeCap,
		publishUrl:           publishUrl,
		chanWebSocket:        make(chan *NotificationPack, 500),
		wg:                   &sync.WaitGroup{},
		reconnectCh:          make(chan struct{}, 1), // buffered to prevent goroutine blocking
		doneCh:               make(chan struct{}),
		stopCh:               make(chan struct{}),
		triggerOnce:          sync.Once{},
		reconnectInterval:    1 * time.Second,
		maxReconnectInterval: 30 * time.Second,
		wsErrCnt:             0,
		lockWsErrCnt:         &sync.RWMutex{},
		que:                  NewQueue(),
		writeMu:              &sync.Mutex{},
	}
	c.reconnect.Store(true)
	return c
}

func (c *ReconnectClient) closeWsClient() {
	c.reconnect.Store(false)
	c.doneOnce.Do(func() {
		close(c.doneCh)
	})
}

func (c *ReconnectClient) connect() error {
	dialer := &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 12 * time.Second,
	}

	conn, _, err := dialer.Dial(c.nodeMgrAddr, nil)
	if err != nil {
		return err
	}
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
	c.reconnectInterval = 1 * time.Second // Reset interval on successful connect
	tracer.LogDebug(tracer.ID_APP, "[%d]ws-connected with %s", c.nodeId, c.nodeMgrAddr)
	return nil
}

// triggerReconnect safely signals the reconnectLoop that a reconnect is needed.
// Only the first caller per connection lifecycle actually sends the signal;
// subsequent calls (e.g. both readLoop and heartbeatLoop error) are no-ops via sync.Once.
func (c *ReconnectClient) triggerReconnect(reason string) {
	c.triggerOnce.Do(func() {
		tracer.LogDebug(tracer.ID_APP, "[%d]triggering reconnect, reason: %s", c.nodeId, reason)
		// Close old connection to unblock any stuck ReadMessage/WriteMessage
		c.connMu.Lock()
		if c.conn != nil {
			c.conn.Close()
		}
		c.connMu.Unlock()
		// Signal reconnectLoop (non-blocking due to buffered channel)
		select {
		case c.reconnectCh <- struct{}{}:
		default:
		}
	})
}

// startSessionLoops spawns readLoop and heartbeatLoop with a fresh stopCh and triggerOnce.
func (c *ReconnectClient) startSessionLoops() {
	c.stopCh = make(chan struct{})
	c.triggerOnce = sync.Once{} // reset so new session can trigger reconnect

	go c.readLoop()
	go c.heartbeatLoop()
}

func (c *ReconnectClient) heartbeatLoop() {
	defer tracer.TryException()

	if err := c.sendWorkerIndication(utils.CMD_TYPE_indication); err != nil {
		tracer.LogWarn(tracer.ID_APP, "[%d]ws-send initial indication failure!%v", c.nodeId, err)
		c.triggerReconnect("initial indication send failed")
		return
	}

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-c.doneCh:
			return
		case <-ticker.C:
			err := c.sendWorkerIndication(utils.CMD_TYPE_indication)
			if err != nil {
				tracer.LogWarn(tracer.ID_APP, "[%d]ws-send heartbeat failure!%v", c.nodeId, err)
				c.triggerReconnect("heartbeat send failed")
				return
			}
			tracer.LogDebug(tracer.ID_APP, "[%d]ws-send heartbeat success!", c.nodeId)
		}
	}
}

func (c *ReconnectClient) readLoop() {
	defer tracer.TryException()

	for {
		select {
		case <-c.stopCh:
			return
		case <-c.doneCh:
			return
		default:
		}

		c.connMu.Lock()
		conn := c.conn
		c.connMu.Unlock()
		if conn == nil {
			c.triggerReconnect("read on nil connection")
			return
		}

		msgType, responseData, err := conn.ReadMessage()
		if err != nil {
			tracer.LogDebug(tracer.ID_APP, "[%d]Failed to read ws: %v", c.nodeId, err)
			c.triggerReconnect("read failed")
			return
		}

		if msgType != websocket.BinaryMessage {
			tracer.LogDebug(tracer.ID_APP, "[%d]ws-recv non-binary message type=%d", c.nodeId, msgType)
			continue
		}

		var req pb3.NodeMsgReq
		if err := proto.Unmarshal(responseData, &req); err != nil {
			tracer.LogWarn(tracer.ID_APP, "[%d]ws-recv: deserialize failure!%v", c.nodeId, err)
			continue
		}
		tracer.LogInfo(tracer.ID_APP, "[%d]ws-recv command: msgType=%s workerType=%s workerId=%d streamPath=%s msgId=%s",
			c.nodeId, req.GetMsgType(), req.GetWorkerType(), req.GetWorkerId(), req.GetStreamPath(), req.GetMsgId())
		c.handleNodeMsgReq(&req)
	}
}

func (c *ReconnectClient) handleNodeMsgReq(req *pb3.NodeMsgReq) {
	var err error
	code := int32(0)
	reason := "ok"

	switch req.GetMsgType() {
	case utils.CMD_TYPE_startPub:
		err = StartFFmpegPublisher()
	case utils.CMD_TYPE_stopPub:
		err = StopFFmpegPublisher()
	case utils.CMD_TYPE_originDown:
		targetURL := strings.TrimSpace(req.GetPublishUrl())
		if targetURL == "" {
			targetURL = strings.TrimSpace(req.GetStreamPath())
		}
		if targetURL == "" {
			err = errors.New("origin down missing publishUrl")
			break
		}
		err = PauseFFmpegPublisherByURL(targetURL, "origin down notification from vcenter")
	case utils.CMD_TYPE_originUp:
		targetURL := strings.TrimSpace(req.GetPublishUrl())
		if targetURL == "" {
			targetURL = strings.TrimSpace(req.GetStreamPath())
		}
		if targetURL == "" {
			err = errors.New("origin up missing publishUrl")
			break
		}
		err = ResumeFFmpegPublisherByURL(targetURL, "origin up notification from vcenter")
	case utils.CMD_TYPE_queryPubPts:
		var ptsFnMs int64
		ptsFnMs, err = GetCurrentEncodingPtsFnMs()
		if err == nil {
			reason = fmt.Sprintf("pts_fn_ms=%d", ptsFnMs)
		}
	default:
		code = 400
		reason = "unsupported command type: " + req.GetMsgType()
	}

	if err != nil {
		code = 500
		reason = err.Error()
		_ = tracer.LogWarn(tracer.ID_APP, "[%d]handle command %s failed: %v", c.nodeId, req.GetMsgType(), err)
	} else {
		_ = tracer.LogInfo(tracer.ID_APP, "[%d]handle command %s success", c.nodeId, req.GetMsgType())
	}

	if sendErr := c.sendNodeMsgRsp(req, code, reason); sendErr != nil {
		_ = tracer.LogWarn(tracer.ID_APP, "[%d]send NodeMsgRsp failed: %v", c.nodeId, sendErr)
	}
}

func (c *ReconnectClient) sendNodeMsgRsp(req *pb3.NodeMsgReq, code int32, reason string) error {
	rsp := &pb3.NodeMsgRsp{
		MsgType:    utils.CMD_TYPE_response,
		WorkerType: c.nodeType,
		WorkerId:   int32(c.nodeId),
		Code:       code,
		Reason:     reason,
		MsgId:      req.GetMsgId(),
	}

	body, err := proto.Marshal(rsp)
	if err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return errors.New("websocket connection is nil")
	}
	return conn.WriteMessage(websocket.BinaryMessage, body)
}

func (c *ReconnectClient) reconnectLoop() {
	defer tracer.TryException()

	for c.reconnect.Load() {
		select {
		case <-c.reconnectCh:
		case <-c.doneCh:
			tracer.LogDebug(tracer.ID_APP, "WebsocketClientThread[%d] reconnectLoop got done signal!", c.nodeId)
			return
		}

		if !c.reconnect.Load() {
			break
		}

		// 1. Close stopCh to signal old readLoop + heartbeatLoop to exit
		close(c.stopCh)

		// 2. Close old connection (may already be closed by triggerReconnect, that's ok)
		c.connMu.Lock()
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		c.connMu.Unlock()

		// 3. Retry connection with exponential backoff
		for c.reconnect.Load() {
			time.Sleep(c.reconnectInterval)

			// Check if we should stop
			select {
			case <-c.doneCh:
				return
			default:
			}

			err := c.connect()
			if err == nil {
				tracer.LogDebug(tracer.ID_APP, "[%d]ws re-connect succeed!", c.nodeId)
				c.startSessionLoops()
				break
			}
			tracer.LogDebug(tracer.ID_APP, "[%d]ws re-connecting...%v", c.nodeId, err)
			c.reconnectInterval = min(c.reconnectInterval*2, c.maxReconnectInterval)
		}
	}
	tracer.LogDebug(tracer.ID_APP, "WebsocketClientThread[%d] reconnectLoop exit!", c.nodeId)
}

func (c *ReconnectClient) Start() error {
	go c.reconnectLoop()
	// Try initial connect first. If it fails, reconnectLoop keeps retrying.
	if err := c.connect(); err != nil {
		c.triggerReconnect("initial connect failed")
		return nil
	}
	c.startSessionLoops()

	return nil
}

func (c *ReconnectClient) sendWorkerIndication(msgType string) error {
	pbMsg := &pb3.WorkerIndication{
		MsgType:    msgType,
		WorkerType: c.nodeType,
		WorkerId:   int32(c.nodeId),
		Version:    utils.VERSION,
		Region:     c.nodeRegion,
		Capacity:   int32(c.nodeCapacity),
		PublishUrl: &c.publishUrl,
	}

	body, err := proto.Marshal(pbMsg)
	if err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return errors.New("websocket connection is nil")
	}
	return conn.WriteMessage(websocket.BinaryMessage, body)
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
