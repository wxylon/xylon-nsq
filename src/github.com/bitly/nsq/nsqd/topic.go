package main

import (
	"bytes"
	"errors"
	"github.com/bitly/nsq/nsq"
	"github.com/bitly/nsq/util"
	"log"
	"sync"
	"sync/atomic"
)

type Topic struct {
	sync.RWMutex
	name       string
	channelMap map[string]*Channel
	// 备份消息
	backend BackendQueue
	//新put进来的消息存放于此, 通过 topic.router() 线程将会监听该 chan,
	incomingMsgChan   chan *nsq.Message
	memoryMsgChan     chan *nsq.Message
	exitChan          chan int
	channelUpdateChan chan int
	waitGroup         util.WaitGroupWrapper
	exitFlag          int32
	// message数量,客户端每发送一条数据,该数量将增加1
	messageCount uint64
	notifier     Notifier
	options      *nsqdOptions
}

/** 创建一个新Topic
  Notifier 在topic的创建和成功退出的时候将会被触发: go notifier.Notify(topic),同时也伴随着topic向lookup程序的 Register 和 UnRegister;

*/
func NewTopic(topicName string, options *nsqdOptions, notifier Notifier) *Topic {
	topic := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),
		backend:           NewDiskQueue(topicName, options.dataPath, options.maxBytesPerFile, options.syncEvery, options.syncTimeout),
		incomingMsgChan:   make(chan *nsq.Message, 1),
		memoryMsgChan:     make(chan *nsq.Message, options.memQueueSize),
		notifier:          notifier,
		options:           options,
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
	}

	topic.waitGroup.Wrap(func() { topic.router() })
	topic.waitGroup.Wrap(func() { topic.messagePump() })

	go notifier.Notify(topic)

	return topic
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	if isNew {
		// update messagePump state
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}

	return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.options, t.notifier, deleteCallback)
		t.channelMap[channelName] = channel
		log.Printf("TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	t.Unlock()

	log.Printf("TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	channel.Delete()

	// update messagePump state
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

// PutMessage writes to the appropriate incoming message channel
/** 发送一条 message */
func (t *Topic) PutMessage(msg *nsq.Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	t.incomingMsgChan <- msg
	atomic.AddUint64(&t.messageCount, 1)
	return nil
}

func (t *Topic) PutMessages(messages []*nsq.Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	for _, m := range messages {
		t.incomingMsgChan <- m
		atomic.AddUint64(&t.messageCount, 1)
	}
	return nil
}

func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
func (t *Topic) messagePump() {
	var msg *nsq.Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *nsq.Message
	var backendChan chan []byte

	t.RLock()
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()

	if len(chans) > 0 {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	for {
		select {
		case msg = <-memoryMsgChan:
			log.Println("topic 有接收到内存新消息:", msg)
		case buf = <-backendChan:
			msg, err = nsq.DecodeMessage(buf)
			if err != nil {
				log.Printf("ERROR: failed to decode message - %s", err.Error())
				continue
			}
			log.Println("topic 有接收到磁盘新消息:", msg)
		case <-t.channelUpdateChan:
			log.Println("topic 有接收到channel变化信息:")
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c)
			}
			t.RUnlock()
			if len(chans) == 0 {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan:
			goto exit
		}
		// 同步到该topic下的所有channel 中
		for i, channel := range chans {
			log.Printf("同步消息到topic:%s, channel:%s, msg:(%s)", channel.topicName, channel.name, msg)
			chanMsg := msg
			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			if i > 0 {
				chanMsg = nsq.NewMessage(msg.Id, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
			}
			err := channel.PutMessage(chanMsg)
			if err != nil {
				log.Printf("TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s", t.name, msg.Id, channel.name, err.Error())
			}
		}
	}

exit:
	log.Printf("TOPIC(%s): closing ... messagePump", t.name)
}

// router handles muxing of Topic messages including
// proxying messages to memory or backend
// topic 创建完毕后,
func (t *Topic) router() {
	var msgBuf bytes.Buffer
	for msg := range t.incomingMsgChan {
		select {
		case t.memoryMsgChan <- msg:
			log.Printf("topic--->router()--->case t.memoryMsgChan <- msg")
		default:
			log.Printf("topic--->router()--->default")
			err := WriteMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
				// theres not really much we can do at this point, you're certainly
				// going to lose messages...
			}
		}
	}

	log.Printf("TOPIC(%s): closing ... router", t.name)
}

// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		log.Printf("TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		go t.notifier.Notify(t)
	} else {
		log.Printf("TOPIC(%s): closing", t.name)
	}

	close(t.exitChan)

	t.Lock()
	close(t.incomingMsgChan)
	t.Unlock()

	// synchronize the close of router() and messagePump()
	t.waitGroup.Wait()

	if deleted {
		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		t.Empty()
		return t.backend.Delete()
	}

	// close all the channels
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			log.Printf("ERROR: channel(%s) close - %s", channel.name, err.Error())
		}
	}

	// write anything leftover to disk
	t.flush()
	return t.backend.Close()
}

func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return t.backend.Empty()
}

func (t *Topic) flush() error {
	var msgBuf bytes.Buffer

	if len(t.memoryMsgChan) > 0 {
		log.Printf("TOPIC(%s): flushing %d memory messages to backend", t.name, len(t.memoryMsgChan))
	}

	for {
		select {
		case msg := <-t.memoryMsgChan:
			err := WriteMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}
