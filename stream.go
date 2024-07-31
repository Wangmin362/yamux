package yamux

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type streamState int

const (
	streamInit streamState = iota
	streamSYNSent
	streamSYNReceived
	streamEstablished
	streamLocalClose
	streamRemoteClose
	streamClosed
	streamReset
)

// Stream is used to represent a logical stream
// within a session.
type Stream struct {
	recvWindow uint32
	sendWindow uint32

	id      uint32
	session *Session

	state     streamState
	stateLock sync.Mutex

	// TODO 1、这里是如何接收流的数据的？
	// TODO 2、如果接收方没有及时读取数据，接收方会一直接收数据缓存下来么？ 会阻塞底层的TCP连接么？
	recvBuf  *bytes.Buffer // 用于接收数据
	recvLock sync.Mutex

	controlHdr     header
	controlErr     chan error
	controlHdrLock sync.Mutex

	sendHdr  header
	sendErr  chan error
	sendLock sync.Mutex

	recvNotifyCh chan struct{}
	sendNotifyCh chan struct{}

	readDeadline  atomic.Value // time.Time
	writeDeadline atomic.Value // time.Time

	// establishCh is notified if the stream is established or being closed.
	// 用来表示当前Stream是否是关闭还是打开状态？
	// TODO 为什么关闭流也是复用的这个信号？
	establishCh chan struct{}

	// closeTimer is set with stateLock held to honor the StreamCloseTimeout
	// setting on Session.
	closeTimer *time.Timer
}

// newStream is used to construct a new stream within
// a given session for an ID
func newStream(session *Session, id uint32, state streamState) *Stream {
	s := &Stream{
		id:           id,                               // 流ID，若是客户端创建的流，那么ID为奇数；若为服务端创建的流，那么ID为偶数
		session:      session,                          // 当前会话逻辑，会话底层持有的其实就是TCP连接
		state:        state,                            // 当前流的状态
		controlHdr:   header(make([]byte, headerSize)), // TODO 什么叫做控制头，什么叫做发送头？
		controlErr:   make(chan error, 1),
		sendHdr:      header(make([]byte, headerSize)),
		sendErr:      make(chan error, 1),
		recvWindow:   initialStreamWindow,    // 接受窗口数据大小
		sendWindow:   initialStreamWindow,    // 发送窗口数据大小
		recvNotifyCh: make(chan struct{}, 1), // 接受停止信号
		sendNotifyCh: make(chan struct{}, 1), // 发送停止信号
		establishCh:  make(chan struct{}, 1), // TODO 连接建立信号
	}
	s.readDeadline.Store(time.Time{})
	s.writeDeadline.Store(time.Time{})
	return s
}

// Session returns the associated stream session
func (s *Stream) Session() *Session {
	return s.session
}

// StreamID returns the ID of this stream
func (s *Stream) StreamID() uint32 {
	return s.id
}

// Read is used to read from the stream
func (s *Stream) Read(b []byte) (n int, err error) {
	defer asyncNotify(s.recvNotifyCh) // TODO 这里是在干嘛？
START:
	s.stateLock.Lock()
	switch s.state {
	case streamLocalClose: // 如果流已经关闭了，直接返回错误
		fallthrough
	case streamRemoteClose:
		fallthrough
	case streamClosed:
		s.recvLock.Lock()
		if s.recvBuf == nil || s.recvBuf.Len() == 0 {
			s.recvLock.Unlock()
			s.stateLock.Unlock()
			return 0, io.EOF // 表示已经接受完成了
		}
		// TODO  到了这里，说明还没有接收完成数据
		s.recvLock.Unlock()
	case streamReset:
		s.stateLock.Unlock()
		return 0, ErrConnectionReset
	}
	s.stateLock.Unlock()

	// 接收数据

	// If there is no data available, block
	s.recvLock.Lock()
	if s.recvBuf == nil || s.recvBuf.Len() == 0 {
		// 如果此时Buf中没有数据，只能阻塞起来，等待数据的到来
		s.recvLock.Unlock()
		goto WAIT
	}

	// Read any bytes
	// 此时Buffer中有多少数据，就返回多数据
	// TODO 这里的设计和我之前的设计到是有不同，确实应该这么设计，当用户来读取的，有多少数据，直接返回多少数据，只不过忽略bytes.Buffer返回
	// 的io.EOF错误，因为当前没有数据，并不代表以后一直没有数据，在后续的某个时间很有可能有数据。所以io.EOF必须由发送方说了算，只有当发送方
	// 关闭了Stream，才认为数据发送完成了
	n, _ = s.recvBuf.Read(b)
	s.recvLock.Unlock()

	// Send a window update potentially
	// TODO 为什么这里又在更新窗口？
	err = s.sendWindowUpdate()
	if err == ErrSessionShutdown {
		err = nil
	}
	return n, err

WAIT:
	var timeout <-chan time.Time
	var timer *time.Timer
	readDeadline := s.readDeadline.Load().(time.Time)
	if !readDeadline.IsZero() { // 必须在规定时间内读取到数据，否则直接返回错误
		delay := readDeadline.Sub(time.Now())
		timer = time.NewTimer(delay)
		timeout = timer.C
	}
	select {
	case <-s.recvNotifyCh: // 说明来数据了，开始接收数据
		if timer != nil {
			timer.Stop()
		}
		goto START
	case <-timeout: // 如果没有设置超时时间，那么timeout就是一个空的channel，因此就会一直阻塞
		return 0, ErrTimeout
	}
}

// Write is used to write to the stream
func (s *Stream) Write(b []byte) (n int, err error) {
	s.sendLock.Lock()
	defer s.sendLock.Unlock()

	total := 0
	// TODO 我猜测窗口的作用就是用于防止某个Stream一直霸占底层的TCP连接，一直发送数据，导致其它的Stream没有机会发送数据
	for total < len(b) { // 根据窗口大小写入数据
		n, err := s.write(b[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// write is used to write to the stream, may return on
// a short write.
func (s *Stream) write(b []byte) (n int, err error) {
	var flags uint16
	var maxWindow uint32
	var body []byte
START:
	s.stateLock.Lock()
	switch s.state {
	case streamLocalClose: // 如果本地或者流已经关了就直接返回错误
		fallthrough
	case streamClosed:
		s.stateLock.Unlock()
		return 0, ErrStreamClosed
	case streamReset: // 如果连接被重置了，直接返回错误
		s.stateLock.Unlock()
		return 0, ErrConnectionReset
	}
	s.stateLock.Unlock()

	// If there is no data available, block
	window := atomic.LoadUint32(&s.sendWindow)
	if window == 0 {
		goto WAIT
	}

	// Determine the flags if any
	flags = s.sendFlags()

	// Send up to our send window
	maxWindow = min(window, uint32(len(b)))
	body = b[:maxWindow]

	// Send the header
	s.sendHdr.encode(typeData, flags, s.id, maxWindow)
	// 发送数据
	if err = s.session.waitForSendErr(s.sendHdr, body, s.sendErr); err != nil {
		if errors.Is(err, ErrSessionShutdown) || errors.Is(err, ErrConnectionWriteTimeout) {
			// Message left in ready queue, header re-use is unsafe.
			s.sendHdr = header(make([]byte, headerSize))
		}
		return 0, err
	}

	// Reduce our send window
	// TODO 缩减发送窗口
	atomic.AddUint32(&s.sendWindow, ^uint32(maxWindow-1))

	// Unlock
	return int(maxWindow), err

WAIT:
	var timeout <-chan time.Time
	writeDeadline := s.writeDeadline.Load().(time.Time)
	if !writeDeadline.IsZero() {
		delay := writeDeadline.Sub(time.Now())
		timeout = time.After(delay)
	}
	select {
	case <-s.sendNotifyCh: // TODO 什么时候会通知可以发送数据了？
		goto START
	case <-timeout:
		return 0, ErrTimeout
	}
	return 0, nil
}

// sendFlags determines any flags that are appropriate
// based on the current stream state
func (s *Stream) sendFlags() uint16 {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	var flags uint16
	switch s.state {
	case streamInit: // 说明当前想要主动建立一个Stream
		flags |= flagSYN
		s.state = streamSYNSent // Stream发送完SYN信号之后，就进入SYNSent状态
	case streamSYNReceived: // 说明当前是被动接受一个Stream的创建，因此需要恢复Ack，表示自己已经接收到了Stream新建的信号
		flags |= flagACK
		s.state = streamEstablished // Stream发送完成这个ACK之后，就转为Established状态
	}
	return flags
}

// sendWindowUpdate potentially sends a window update enabling
// further writes to take place. Must be invoked with the lock.
func (s *Stream) sendWindowUpdate() error {
	s.controlHdrLock.Lock()
	defer s.controlHdrLock.Unlock()

	// Determine the delta update
	maxWindow := s.session.config.MaxStreamWindowSize
	var bufLen uint32
	s.recvLock.Lock()
	if s.recvBuf != nil {
		bufLen = uint32(s.recvBuf.Len())
	}
	// TODO 如何理解这里的窗口计算
	delta := (maxWindow - bufLen) - s.recvWindow

	// 确定当前Stream的状态，以及计算控制头的状态
	flags := s.sendFlags()

	// Check if we can omit the update
	// TODO 如何理解这里的逻辑
	if delta < (maxWindow/2) && flags == 0 {
		s.recvLock.Unlock()
		return nil
	}

	// Update our window
	s.recvWindow += delta
	s.recvLock.Unlock()

	// Send the header 合成协议头
	s.controlHdr.encode(typeWindowUpdate, flags, s.id, delta)
	// 发送协议头，并且接收发送的返回结果
	if err := s.session.waitForSendErr(s.controlHdr, nil, s.controlErr); err != nil {
		if errors.Is(err, ErrSessionShutdown) || errors.Is(err, ErrConnectionWriteTimeout) {
			// Message left in ready queue, header re-use is unsafe.
			s.controlHdr = header(make([]byte, headerSize))
		}
		return err
	}
	return nil
}

// sendClose is used to send a FIN
func (s *Stream) sendClose() error {
	s.controlHdrLock.Lock()
	defer s.controlHdrLock.Unlock()

	flags := s.sendFlags()
	flags |= flagFIN
	s.controlHdr.encode(typeWindowUpdate, flags, s.id, 0)
	if err := s.session.waitForSendErr(s.controlHdr, nil, s.controlErr); err != nil {
		if errors.Is(err, ErrSessionShutdown) || errors.Is(err, ErrConnectionWriteTimeout) {
			// Message left in ready queue, header re-use is unsafe.
			s.controlHdr = header(make([]byte, headerSize))
		}
		return err
	}
	return nil
}

// Close is used to close the stream
func (s *Stream) Close() error {
	closeStream := false
	s.stateLock.Lock()
	switch s.state {
	// Opened means we need to signal a close
	case streamSYNSent:
		fallthrough
	case streamSYNReceived:
		fallthrough
	case streamEstablished:
		s.state = streamLocalClose
		goto SEND_CLOSE

	case streamLocalClose:
	case streamRemoteClose:
		s.state = streamClosed
		closeStream = true
		goto SEND_CLOSE

	case streamClosed:
	case streamReset:
	default:
		panic("unhandled state")
	}
	s.stateLock.Unlock()
	return nil
SEND_CLOSE:
	// This shouldn't happen (the more realistic scenario to cancel the
	// timer is via processFlags) but just in case this ever happens, we
	// cancel the timer to prevent dangling timers.
	if s.closeTimer != nil {
		s.closeTimer.Stop()
		s.closeTimer = nil
	}

	// If we have a StreamCloseTimeout set we start the timeout timer.
	// We do this only if we're not already closing the stream since that
	// means this was a graceful close.
	//
	// This prevents memory leaks if one side (this side) closes and the
	// remote side poorly behaves and never responds with a FIN to complete
	// the close. After the specified timeout, we clean our resources up no
	// matter what.
	if !closeStream && s.session.config.StreamCloseTimeout > 0 {
		s.closeTimer = time.AfterFunc(
			s.session.config.StreamCloseTimeout, s.closeTimeout)
	}

	s.stateLock.Unlock()
	s.sendClose()
	s.notifyWaiting()
	if closeStream {
		s.session.closeStream(s.id)
	}
	return nil
}

// closeTimeout is called after StreamCloseTimeout during a close to
// close this stream.
func (s *Stream) closeTimeout() {
	// Close our side forcibly
	s.forceClose()

	// Free the stream from the session map
	s.session.closeStream(s.id)

	// Send a RST so the remote side closes too.
	s.sendLock.Lock()
	defer s.sendLock.Unlock()
	hdr := header(make([]byte, headerSize))
	hdr.encode(typeWindowUpdate, flagRST, s.id, 0)
	s.session.sendNoWait(hdr)
}

// forceClose is used for when the session is exiting
func (s *Stream) forceClose() {
	s.stateLock.Lock()
	s.state = streamClosed
	s.stateLock.Unlock()
	s.notifyWaiting()
}

// processFlags is used to update the state of the stream
// based on set flags, if any. Lock must be held
func (s *Stream) processFlags(flags uint16) error {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	// Close the stream without holding the state lock
	closeStream := false
	defer func() {
		if closeStream {
			if s.closeTimer != nil {
				// Stop our close timeout timer since we gracefully closed
				s.closeTimer.Stop()
			}

			s.session.closeStream(s.id)
		}
	}()

	if flags&flagACK == flagACK {
		if s.state == streamSYNSent {
			s.state = streamEstablished
		}
		asyncNotify(s.establishCh)
		s.session.establishStream(s.id)
	}
	if flags&flagFIN == flagFIN {
		switch s.state {
		case streamSYNSent:
			fallthrough
		case streamSYNReceived:
			fallthrough
		case streamEstablished:
			s.state = streamRemoteClose
			s.notifyWaiting()
		case streamLocalClose:
			s.state = streamClosed
			closeStream = true
			s.notifyWaiting()
		default:
			s.session.logger.Printf("[ERR] yamux: unexpected FIN flag in state %d", s.state)
			return ErrUnexpectedFlag
		}
	}
	if flags&flagRST == flagRST {
		s.state = streamReset
		closeStream = true
		s.notifyWaiting()
	}
	return nil
}

// notifyWaiting notifies all the waiting channels
func (s *Stream) notifyWaiting() {
	asyncNotify(s.recvNotifyCh)
	asyncNotify(s.sendNotifyCh)
	asyncNotify(s.establishCh)
}

// incrSendWindow updates the size of our send window
func (s *Stream) incrSendWindow(hdr header, flags uint16) error {
	if err := s.processFlags(flags); err != nil {
		return err
	}

	// Increase window, unblock a sender
	atomic.AddUint32(&s.sendWindow, hdr.Length())
	asyncNotify(s.sendNotifyCh)
	return nil
}

// readData is used to handle a data frame
func (s *Stream) readData(hdr header, flags uint16, conn io.Reader) error {
	if err := s.processFlags(flags); err != nil {
		return err
	}

	// Check that our recv window is not exceeded
	length := hdr.Length()
	if length == 0 {
		return nil
	}

	// Wrap in a limited reader
	conn = &io.LimitedReader{R: conn, N: int64(length)}

	// Copy into buffer
	s.recvLock.Lock()

	if length > s.recvWindow {
		s.session.logger.Printf("[ERR] yamux: receive window exceeded (stream: %d, remain: %d, recv: %d)", s.id, s.recvWindow, length)
		s.recvLock.Unlock()
		return ErrRecvWindowExceeded
	}

	if s.recvBuf == nil {
		// Allocate the receive buffer just-in-time to fit the full data frame.
		// This way we can read in the whole packet without further allocations.
		// 接收一个完整的包长度
		s.recvBuf = bytes.NewBuffer(make([]byte, 0, length))
	}
	copiedLength, err := io.Copy(s.recvBuf, conn)
	if err != nil {
		s.session.logger.Printf("[ERR] yamux: Failed to read stream data: %v", err)
		s.recvLock.Unlock()
		return err
	}

	// Decrement the receive window
	// 减少需要接收帧的长度
	s.recvWindow -= uint32(copiedLength)
	s.recvLock.Unlock()

	// Unblock any readers
	asyncNotify(s.recvNotifyCh)
	return nil
}

// SetDeadline sets the read and write deadlines
func (s *Stream) SetDeadline(t time.Time) error {
	if err := s.SetReadDeadline(t); err != nil {
		return err
	}
	if err := s.SetWriteDeadline(t); err != nil {
		return err
	}
	return nil
}

// SetReadDeadline sets the deadline for blocked and future Read calls.
func (s *Stream) SetReadDeadline(t time.Time) error {
	s.readDeadline.Store(t)
	asyncNotify(s.recvNotifyCh)
	return nil
}

// SetWriteDeadline sets the deadline for blocked and future Write calls
func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.writeDeadline.Store(t)
	asyncNotify(s.sendNotifyCh)
	return nil
}

// Shrink is used to compact the amount of buffers utilized
// This is useful when using Yamux in a connection pool to reduce
// the idle memory utilization.
func (s *Stream) Shrink() {
	s.recvLock.Lock()
	if s.recvBuf != nil && s.recvBuf.Len() == 0 {
		s.recvBuf = nil
	}
	s.recvLock.Unlock()
}
