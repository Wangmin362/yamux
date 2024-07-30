package yamux

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Session is used to wrap a reliable ordered connection and to
// multiplex it into multiple streams.
type Session struct {
	// remoteGoAway indicates the remote side does
	// not want futher connections. Must be first for alignment.
	// 远端已经关闭了，不再接受新的Stream
	// TODO 1、这玩意什么时候改变的？
	remoteGoAway int32

	// localGoAway indicates that we should stop
	// accepting futher connections. Must be first for alignment.
	localGoAway int32

	// nextStreamID is the next stream we should
	// send. This depends if we are a client/server.
	nextStreamID uint32

	// config holds our configuration
	config *Config

	// logger is used for our logs
	logger Logger

	// conn is the underlying connection
	// 底层的TCP连接
	conn io.ReadWriteCloser

	// bufRead is a buffered reader
	// 全局的Buffer，用于从底层的TCP连接当中获取数据
	bufRead *bufio.Reader

	// pings is used to track inflight pings
	pings    map[uint32]chan struct{}
	pingID   uint32
	pingLock sync.Mutex

	// streams maps a stream id to a stream, and inflight has an entry
	// for any outgoing stream that has not yet been established. Both are
	// protected by streamLock.
	streams    map[uint32]*Stream
	inflight   map[uint32]struct{} // 用于表示正在创建连接中的Stream，远端还没有回复ACK信号
	streamLock sync.Mutex

	// synCh acts like a semaphore. It is sized to the AcceptBacklog which
	// is assumed to be symmetric between the client and server. This allows
	// the client to avoid exceeding the backlog and instead blocks the open.
	// TODO 这玩意有啥用？
	synCh chan struct{}

	// acceptCh is used to pass ready streams to the client
	acceptCh chan *Stream

	// sendCh is used to mark a stream as ready to send,
	// or to send a header out directly.
	sendCh chan *sendReady

	// recvDoneCh is closed when recv() exits to avoid a race
	// between stream registration and stream shutdown
	// TODO 这个信号是干嘛的？
	recvDoneCh chan struct{}
	sendDoneCh chan struct{}

	// shutdown is used to safely close a session
	shutdown        bool
	shutdownErr     error
	shutdownCh      chan struct{} // 用于判断当前session是否已经关闭
	shutdownLock    sync.Mutex
	shutdownErrLock sync.Mutex
}

// sendReady is used to either mark a stream as ready
// or to directly send a header
type sendReady struct {
	Hdr  []byte     // 帧数据
	mu   sync.Mutex // Protects Body from unsafe reads.
	Body []byte     // 帧数据
	// 用于接收数据的发送结果
	// TODO 这里为什么不需要返回成功发送的字节数？
	Err chan error
}

// newSession is used to construct a new session
func newSession(config *Config, conn io.ReadWriteCloser, client bool) *Session {
	logger := config.Logger
	if logger == nil {
		logger = log.New(config.LogOutput, "", log.LstdFlags)
	}

	s := &Session{
		config:     config,                         // 多路复用器的配置
		logger:     logger,                         // 日志输出
		conn:       conn,                           // 真正的底层TCP连接
		bufRead:    bufio.NewReader(conn),          // 读取底层TCP的连接数据，多路复用器肯定需要组装逻辑帧，拼接成为一个完整的数据
		pings:      make(map[uint32]chan struct{}), // 心跳
		streams:    make(map[uint32]*Stream),       // 会话的Stream，key位Stream的ID
		inflight:   make(map[uint32]struct{}),      // TODO 这玩意干嘛的？
		synCh:      make(chan struct{}, config.AcceptBacklog),
		acceptCh:   make(chan *Stream, config.AcceptBacklog),
		sendCh:     make(chan *sendReady, 64),
		recvDoneCh: make(chan struct{}),
		sendDoneCh: make(chan struct{}),
		shutdownCh: make(chan struct{}),
	}
	if client {
		s.nextStreamID = 1 // 客户端产生的流序号位奇数
	} else {
		s.nextStreamID = 2 // 服务端产生的流序号位偶数
	}
	go s.recv()                 // 接收数据
	go s.send()                 // 发送数据
	if config.EnableKeepAlive { // 开启心跳，防止长时间没有通信的情况下断开连接
		go s.keepalive()
	}
	return s
}

// IsClosed does a safe check to see if we have shutdown
func (s *Session) IsClosed() bool {
	select {
	case <-s.shutdownCh: // 这个channel一定是一个无缓冲chan，因为消费方根本就不关心，只要close了，这里一定能读出数据，可以读取无限次
		return true
	default:
		return false
	}
}

// CloseChan returns a read-only channel which is closed as
// soon as the session is closed.
func (s *Session) CloseChan() <-chan struct{} {
	return s.shutdownCh
}

// NumStreams returns the number of currently open streams
func (s *Session) NumStreams() int {
	s.streamLock.Lock()
	num := len(s.streams)
	s.streamLock.Unlock()
	return num
}

// Open is used to create a new stream as a net.Conn
func (s *Session) Open() (net.Conn, error) {
	conn, err := s.OpenStream()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// OpenStream is used to create a new stream
func (s *Session) OpenStream() (*Stream, error) {
	if s.IsClosed() {
		return nil, ErrSessionShutdown
	}
	if atomic.LoadInt32(&s.remoteGoAway) == 1 {
		return nil, ErrRemoteGoAway
	}

	// Block if we have too many inflight SYNs
	select {
	case s.synCh <- struct{}{}: // 似乎是用来控制Session中的stream的数量
	case <-s.shutdownCh:
		return nil, ErrSessionShutdown
	}

GET_ID:
	// Get an ID, and check for stream exhaustion
	id := atomic.LoadUint32(&s.nextStreamID)
	if id >= math.MaxUint32-1 {
		return nil, ErrStreamsExhausted
	}
	// 如果失败了，说明其它协程也想使用这个id，而且当前的协程竞争失败了，因此需要重新获取ID
	if !atomic.CompareAndSwapUint32(&s.nextStreamID, id, id+2) {
		goto GET_ID
	}

	// 到了这里说明，已经成功获取到了ID，并且把下一个可以使用的ID会写到了变量当中

	// Register the stream
	stream := newStream(s, id, streamInit) // 实例化一个流
	s.streamLock.Lock()
	s.streams[id] = stream
	s.inflight[id] = struct{}{}
	s.streamLock.Unlock()

	if s.config.StreamOpenTimeout > 0 { // 在指定时间内如果建立一个Stream一直没有建立好，就直接关闭Session
		go s.setOpenTimeout(stream)
	}

	// Send the window update to create
	// 更新窗口
	if err := stream.sendWindowUpdate(); err != nil {
		select {
		case <-s.synCh: // TODO 这个信号是用来干嘛的？
		default:
			s.logger.Printf("[ERR] yamux: aborted stream open without inflight syn semaphore")
		}
		return nil, err
	}
	return stream, nil
}

// setOpenTimeout implements a timeout for streams that are opened but not established.
// If the StreamOpenTimeout is exceeded we assume the peer is unable to ACK,
// and close the session.
// The number of running timers is bounded by the capacity of the synCh.
func (s *Session) setOpenTimeout(stream *Stream) {
	timer := time.NewTimer(s.config.StreamOpenTimeout)
	defer timer.Stop()

	select {
	case <-stream.establishCh: // 说明成功创建了Stream
		return
	case <-s.shutdownCh: // 如果有协程关闭了Session，直接返回。因为Session都关闭了，创建Stream还有毛线意义
		return
	case <-timer.C:
		// Timeout reached while waiting for ACK.
		// Close the session to force connection re-establishment.
		// 如果一个流在规定的时间之内没有成功建立，说明底层的TCP连接一定存在问题了，此时强制关闭底层的TCP连接
		s.logger.Printf("[ERR] yamux: aborted stream open (destination=%s): %v", s.RemoteAddr().String(), ErrTimeout.err)
		s.Close()
	}
}

// Accept is used to block until the next available stream
// is ready to be accepted.
func (s *Session) Accept() (net.Conn, error) {
	conn, err := s.AcceptStream()
	if err != nil {
		return nil, err
	}
	return conn, err
}

// AcceptStream is used to block until the next available stream
// is ready to be accepted.
func (s *Session) AcceptStream() (*Stream, error) {
	select {
	case stream := <-s.acceptCh: // 等待新的Stream的建立
		// 当有新的流建立上来了，先更新窗口
		if err := stream.sendWindowUpdate(); err != nil {
			return nil, err
		}
		return stream, nil
	case <-s.shutdownCh: // 也有可能在等待的过程中，会话被关闭，此时无需在等待
		return nil, s.shutdownErr
	}
}

// AcceptStream is used to block until the next available stream
// is ready to be accepted.
func (s *Session) AcceptStreamWithContext(ctx context.Context) (*Stream, error) {
	select {
	case <-ctx.Done(): // 相比于AcceptStream，这里可以给用户一定的自由度，自己可以控制等待多久
		return nil, ctx.Err()
	case stream := <-s.acceptCh:
		if err := stream.sendWindowUpdate(); err != nil {
			return nil, err
		}
		return stream, nil
	case <-s.shutdownCh:
		return nil, s.shutdownErr
	}
}

// Close is used to close the session and all streams.
// Attempts to send a GoAway before closing the connection.
func (s *Session) Close() error {
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	if s.shutdown {
		return nil
	}
	s.shutdown = true

	s.shutdownErrLock.Lock()
	if s.shutdownErr == nil {
		s.shutdownErr = ErrSessionShutdown
	}
	s.shutdownErrLock.Unlock()

	close(s.shutdownCh)

	// 关闭底层的TCP连接
	s.conn.Close()
	// TODO 等待底层TCP真正关闭完成？
	<-s.recvDoneCh

	s.streamLock.Lock()
	defer s.streamLock.Unlock()
	for _, stream := range s.streams {
		// TODO 是怎么强制关闭流的？
		stream.forceClose()
	}
	<-s.sendDoneCh
	return nil
}

// exitErr is used to handle an error that is causing the
// session to terminate.
func (s *Session) exitErr(err error) {
	s.shutdownErrLock.Lock()
	if s.shutdownErr == nil {
		s.shutdownErr = err
	}
	s.shutdownErrLock.Unlock()
	s.Close()
}

// GoAway can be used to prevent accepting further
// connections. It does not close the underlying conn.
func (s *Session) GoAway() error {
	return s.waitForSend(s.goAway(goAwayNormal), nil)
}

// goAway is used to send a goAway message
func (s *Session) goAway(reason uint32) header {
	atomic.SwapInt32(&s.localGoAway, 1)
	hdr := header(make([]byte, headerSize))
	hdr.encode(typeGoAway, 0, 0, reason)
	return hdr
}

// Ping is used to measure the RTT response time
func (s *Session) Ping() (time.Duration, error) {
	// Get a channel for the ping
	ch := make(chan struct{})

	// Get a new ping id, mark as pending
	s.pingLock.Lock()
	id := s.pingID
	s.pingID++
	s.pings[id] = ch
	s.pingLock.Unlock()

	// Send the ping request
	hdr := header(make([]byte, headerSize))
	hdr.encode(typePing, flagSYN, 0, id)
	if err := s.waitForSend(hdr, nil); err != nil {
		return 0, err
	}

	// Wait for a response
	start := time.Now()
	select {
	case <-ch:
	case <-time.After(s.config.ConnectionWriteTimeout):
		s.pingLock.Lock()
		delete(s.pings, id) // Ignore it if a response comes later.
		s.pingLock.Unlock()
		return 0, ErrTimeout
	case <-s.shutdownCh:
		return 0, ErrSessionShutdown
	}

	// Compute the RTT
	return time.Now().Sub(start), nil
}

// keepalive is a long running goroutine that periodically does
// a ping to keep the connection alive.
func (s *Session) keepalive() {
	for {
		select {
		case <-time.After(s.config.KeepAliveInterval):
			_, err := s.Ping()
			if err != nil {
				if err != ErrSessionShutdown {
					s.logger.Printf("[ERR] yamux: keepalive failed: %v", err)
					s.exitErr(ErrKeepAliveTimeout)
				}
				return
			}
		case <-s.shutdownCh:
			return
		}
	}
}

// waitForSendErr waits to send a header, checking for a potential shutdown
func (s *Session) waitForSend(hdr header, body []byte) error {
	errCh := make(chan error, 1)
	return s.waitForSendErr(hdr, body, errCh)
}

// waitForSendErr waits to send a header with optional data, checking for a
// potential shutdown. Since there's the expectation that sends can happen
// in a timely manner, we enforce the connection write timeout here.
func (s *Session) waitForSendErr(hdr header, body []byte, errCh chan error) error {
	t := timerPool.Get()
	timer := t.(*time.Timer)
	timer.Reset(s.config.ConnectionWriteTimeout)
	defer func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
		timerPool.Put(t) // 用完之后，重新放回池子中
	}()

	ready := &sendReady{Hdr: hdr, Body: body, Err: errCh}
	select {
	case s.sendCh <- ready: // 发送数据
	case <-s.shutdownCh: // 如果数据还没发送，此时Session被关闭了，那么只能关闭Session
		return ErrSessionShutdown
	case <-timer.C: // 如果一个数据迟迟发送不出去，那么直接关闭连接，这里定义的是10万个小时 TODO 为什么定义这么大的一个数？
		return ErrConnectionWriteTimeout
	}

	bodyCopy := func() {
		if body == nil {
			return // A nil body is ignored.
		}

		// In the event of session shutdown or connection write timeout,
		// we need to prevent `send` from reading the body buffer after
		// returning from this function since the caller may re-use the
		// underlying array.
		ready.mu.Lock()
		defer ready.mu.Unlock()

		if ready.Body == nil {
			return // Body was already copied in `send`.
		}

		// TODO 以前的BODY是怎么处理了？为什么这里需要这么处理？
		newBody := make([]byte, len(body))
		copy(newBody, body)
		ready.Body = newBody
	}

	select {
	case err := <-errCh: // 接收数据发送的返回结果，这里其实就是在想底层的TCP连接写入数据，所以错误其实就是写入底层TCP连接的错误
		return err
	case <-s.shutdownCh:
		bodyCopy() // TODO 拷贝Body的意义何在？
		return ErrSessionShutdown
	case <-timer.C:
		bodyCopy() // TODO 拷贝Body的意义何在？
		return ErrConnectionWriteTimeout
	}
}

// sendNoWait does a send without waiting. Since there's the expectation that
// the send happens right here, we enforce the connection write timeout if we
// can't queue the header to be sent.
func (s *Session) sendNoWait(hdr header) error {
	t := timerPool.Get()
	timer := t.(*time.Timer)
	timer.Reset(s.config.ConnectionWriteTimeout)
	defer func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
		timerPool.Put(t)
	}()

	select {
	case s.sendCh <- &sendReady{Hdr: hdr}:
		return nil
	case <-s.shutdownCh:
		return ErrSessionShutdown
	case <-timer.C:
		return ErrConnectionWriteTimeout
	}
}

// send is a long running goroutine that sends data
func (s *Session) send() {
	if err := s.sendLoop(); err != nil {
		s.exitErr(err)
	}
}

func (s *Session) sendLoop() error {
	defer close(s.sendDoneCh)

	var bodyBuf bytes.Buffer
	for {
		bodyBuf.Reset()

		select {
		case ready := <-s.sendCh: // 发送数据通道
			// Send a header if ready
			if ready.Hdr != nil { // TODO 难道header可以为空？  从协议定义上看，肯定是不能为空的
				// 底层连接写入Header
				_, err := s.conn.Write(ready.Hdr)
				if err != nil {
					s.logger.Printf("[ERR] yamux: Failed to write header: %v", err)
					asyncSendErr(ready.Err, err)
					return err
				}
				// 每次发送数据的时候其实都需要发送Header，因此在这里不需要置空
			}

			// TODO 为什么在发送Body的时候需要加锁？ 这里只有一个协程在消费数据，又不会修改里面的数据，我举得是不需要加锁的
			ready.mu.Lock()
			if ready.Body != nil {
				// Copy the body into the buffer to avoid
				// holding a mutex lock during the write.
				_, err := bodyBuf.Write(ready.Body)
				if err != nil {
					ready.Body = nil
					ready.mu.Unlock()
					s.logger.Printf("[ERR] yamux: Failed to copy body into buffer: %v", err)
					asyncSendErr(ready.Err, err)
					return err
				}
				ready.Body = nil
			}
			ready.mu.Unlock()

			if bodyBuf.Len() > 0 {
				// Send data from a body if given  写入Body
				_, err := s.conn.Write(bodyBuf.Bytes())
				if err != nil {
					s.logger.Printf("[ERR] yamux: Failed to write body: %v", err)
					asyncSendErr(ready.Err, err)
					return err
				}
			}

			// No error, successful send
			asyncSendErr(ready.Err, nil)
		case <-s.shutdownCh:
			return nil
		}
	}
}

// recv is a long running goroutine that accepts new data
func (s *Session) recv() {
	if err := s.recvLoop(); err != nil {
		s.exitErr(err)
	}
}

// Ensure that the index of the handler (typeData/typeWindowUpdate/etc) matches the message type
var (
	handlers = []func(*Session, header) error{
		typeData:         (*Session).handleStreamMessage,
		typeWindowUpdate: (*Session).handleStreamMessage,
		typePing:         (*Session).handlePing,
		typeGoAway:       (*Session).handleGoAway,
	}
)

// recvLoop continues to receive data until a fatal error is encountered
func (s *Session) recvLoop() error {
	defer close(s.recvDoneCh)
	hdr := header(make([]byte, headerSize))
	for {
		// Read the header 每次都是先读取Header
		if _, err := io.ReadFull(s.bufRead, hdr); err != nil {
			if err != io.EOF && !strings.Contains(err.Error(), "closed") && !strings.Contains(err.Error(), "reset by peer") {
				s.logger.Printf("[ERR] yamux: Failed to read header: %v", err)
			}
			return err
		}

		// Verify the version
		if hdr.Version() != protoVersion {
			s.logger.Printf("[ERR] yamux: Invalid protocol version: %d", hdr.Version())
			return ErrInvalidVersion
		}

		mt := hdr.MsgType()
		if mt < typeData || mt > typeGoAway {
			return ErrInvalidMsgType
		}

		if err := handlers[mt](s, hdr); err != nil {
			return err
		}
	}
}

// handleStreamMessage handles either a data or window update frame
func (s *Session) handleStreamMessage(hdr header) error {
	// Check for a new stream creation
	id := hdr.StreamID()
	flags := hdr.Flags()
	if flags&flagSYN == flagSYN {
		// 说明有新的Stream建立
		if err := s.incomingStream(id); err != nil {
			return err
		}
	}

	// Get the stream
	s.streamLock.Lock() // TODO 这里为什么不使用互斥锁？
	stream := s.streams[id]
	s.streamLock.Unlock()

	// If we do not have a stream, likely we sent a RST
	if stream == nil { // stream还没有建立，就直接开始传输数据，这里认为这种情况是一种非法情况，因此需要把数据丢弃
		// Drain any data on the wire
		if hdr.MsgType() == typeData && hdr.Length() > 0 {
			s.logger.Printf("[WARN] yamux: Discarding data for stream: %d", id)
			if _, err := io.CopyN(ioutil.Discard, s.bufRead, int64(hdr.Length())); err != nil {
				s.logger.Printf("[ERR] yamux: Failed to discard data: %v", err)
				return nil
			}
		} else {
			s.logger.Printf("[WARN] yamux: frame for missing stream: %v", hdr)
		}
		return nil
	}

	// Check if this is a window update
	if hdr.MsgType() == typeWindowUpdate {
		// 更新发送端的窗口
		if err := stream.incrSendWindow(hdr, flags); err != nil {
			if sendErr := s.sendNoWait(s.goAway(goAwayProtoErr)); sendErr != nil {
				s.logger.Printf("[WARN] yamux: failed to send go away: %v", sendErr)
			}
			return err
		}
		return nil
	}

	// Read the new data
	if err := stream.readData(hdr, flags, s.bufRead); err != nil {
		if sendErr := s.sendNoWait(s.goAway(goAwayProtoErr)); sendErr != nil {
			s.logger.Printf("[WARN] yamux: failed to send go away: %v", sendErr)
		}
		return err
	}
	return nil
}

// handlePing is invokde for a typePing frame
func (s *Session) handlePing(hdr header) error {
	flags := hdr.Flags()
	pingID := hdr.Length()

	// Check if this is a query, respond back in a separate context so we
	// don't interfere with the receiving thread blocking for the write.
	// TODO 新建一个Stream
	if flags&flagSYN == flagSYN {
		go func() {
			hdr := header(make([]byte, headerSize))
			hdr.encode(typePing, flagACK, 0, pingID)
			if err := s.sendNoWait(hdr); err != nil {
				s.logger.Printf("[WARN] yamux: failed to send ping reply: %v", err)
			}
		}()
		return nil
	}

	// Handle a response
	s.pingLock.Lock()
	ch := s.pings[pingID]
	if ch != nil {
		delete(s.pings, pingID)
		close(ch)
	}
	s.pingLock.Unlock()
	return nil
}

// handleGoAway is invokde for a typeGoAway frame
func (s *Session) handleGoAway(hdr header) error {
	code := hdr.Length()
	switch code {
	case goAwayNormal:
		atomic.SwapInt32(&s.remoteGoAway, 1)
	case goAwayProtoErr:
		s.logger.Printf("[ERR] yamux: received protocol error go away")
		return fmt.Errorf("yamux protocol error")
	case goAwayInternalErr:
		s.logger.Printf("[ERR] yamux: received internal error go away")
		return fmt.Errorf("remote yamux internal error")
	default:
		s.logger.Printf("[ERR] yamux: received unexpected go away")
		return fmt.Errorf("unexpected go away received")
	}
	return nil
}

// incomingStream is used to create a new incoming stream
func (s *Session) incomingStream(id uint32) error {
	// Reject immediately if we are doing a go away
	// 如果本地已经关了，那么直接让告诉客户端不让连接
	if atomic.LoadInt32(&s.localGoAway) == 1 {
		hdr := header(make([]byte, headerSize))
		hdr.encode(typeWindowUpdate, flagRST, id, 0)
		return s.sendNoWait(hdr)
	}

	// Allocate a new stream
	// 接收方也需要记录当前发送方有哪些流，后需要接收数据就需要区分每个流的数据
	stream := newStream(s, id, streamSYNReceived)

	s.streamLock.Lock()
	defer s.streamLock.Unlock()

	// Check if stream already exists
	if _, ok := s.streams[id]; ok { // 如果当前流已经存在，那么直接报错
		s.logger.Printf("[ERR] yamux: duplicate stream declared")
		if sendErr := s.sendNoWait(s.goAway(goAwayProtoErr)); sendErr != nil {
			s.logger.Printf("[WARN] yamux: failed to send go away: %v", sendErr)
		}
		return ErrDuplicateStream
	}

	// Register the stream
	s.streams[id] = stream

	// Check if we've exceeded the backlog
	select {
	// 到了这一步，说明流已经成功建立了，两边的元数据都已经维护好了
	case s.acceptCh <- stream: // 生产消费模型，这里作为生产者，实例化了一个Stream
		return nil
	default:
		// 已经超过了流的最大限制，直接告诉发送方关闭连接
		// Backlog exceeded! RST the stream
		s.logger.Printf("[WARN] yamux: backlog exceeded, forcing connection reset")
		delete(s.streams, id)
		hdr := header(make([]byte, headerSize))
		hdr.encode(typeWindowUpdate, flagRST, id, 0)
		return s.sendNoWait(hdr)
	}
}

// closeStream is used to close a stream once both sides have
// issued a close. If there was an in-flight SYN and the stream
// was not yet established, then this will give the credit back.
func (s *Session) closeStream(id uint32) {
	s.streamLock.Lock()
	if _, ok := s.inflight[id]; ok {
		select {
		case <-s.synCh:
		default:
			s.logger.Printf("[ERR] yamux: SYN tracking out of sync")
		}
	}
	delete(s.streams, id)
	s.streamLock.Unlock()
}

// establishStream is used to mark a stream that was in the
// SYN Sent state as established.
func (s *Session) establishStream(id uint32) {
	s.streamLock.Lock()
	if _, ok := s.inflight[id]; ok {
		delete(s.inflight, id)
	} else {
		s.logger.Printf("[ERR] yamux: established stream without inflight SYN (no tracking entry)")
	}
	select {
	case <-s.synCh:
	default:
		s.logger.Printf("[ERR] yamux: established stream without inflight SYN (didn't have semaphore)")
	}
	s.streamLock.Unlock()
}
