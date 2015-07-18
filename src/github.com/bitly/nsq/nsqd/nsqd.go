package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/bitly/nsq/nsq"
	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/lookupd"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"runtime"
	"sync"
	"time"
)

type Notifier interface {
	Notify(v interface{})
}

/** NSQd 结构体 */
type NSQd struct {
	sync.RWMutex
	// 配置信息
	options *nsqdOptions
	// nsqd 容器的 id
	workerId int64
	// 订阅主题 字典表
	topicMap map[string]*Topic
	// 订阅主题 字典表
	lookupdTCPAddrs util.StringArray
	// nsqd 监听的tcp端口:0.0.0.0:4150
	tcpAddr *net.TCPAddr
	// nsqd 监听的http端口:0.0.0.0:4151
	httpAddr *net.TCPAddr
	// nsqd tcp 请求处理的 监听器
	tcpListener net.Listener
	// nsqd http 请求处理的 监听器
	httpListener net.Listener
	// nsqd 的id生成器, topic和channel使用
	idChan chan nsq.MessageID
	// nsqd 容器退出标识,
	exitChan chan int
	// 一个实现锁功能的.... 后续关注
	waitGroup util.WaitGroupWrapper
	// nsqlookup servers 数组
	lookupPeers []*nsq.LookupPeer
	// 通知的 chan, 此处主要是topic和channel在创建的时候, 有 go程,监听该 chan
	// 并将 topic和channel 和信息 注册或者取消注册到 所有的nsqlookup server 中
	notifyChan chan interface{}
}

/** NSQd 配置信息*/
type nsqdOptions struct {
	memQueueSize         int64
	dataPath             string
	maxMessageSize       int64
	maxBodySize          int64
	maxBytesPerFile      int64
	maxRdyCount          int64
	syncEvery            int64
	syncTimeout          time.Duration
	msgTimeout           time.Duration
	maxMsgTimeout        time.Duration
	clientTimeout        time.Duration
	maxHeartbeatInterval time.Duration
	broadcastAddress     string

	maxOutputBufferSize    int64
	maxOutputBufferTimeout time.Duration
}

/** 创建 NSQd 配置信息 */
func NewNsqdOptions() *nsqdOptions {
	return &nsqdOptions{
		memQueueSize:         10000,
		dataPath:             os.TempDir(),
		maxMessageSize:       1024768,
		maxBodySize:          5 * 1024768,
		maxBytesPerFile:      104857600,
		maxRdyCount:          2500,
		syncEvery:            2500,
		syncTimeout:          2 * time.Second,
		msgTimeout:           60 * time.Second,
		maxMsgTimeout:        15 * time.Minute,
		clientTimeout:        nsq.DefaultClientTimeout,
		maxHeartbeatInterval: 60 * time.Second,
		broadcastAddress:     "",

		maxOutputBufferSize:    64 * 1024,
		maxOutputBufferTimeout: 1 * time.Second,
	}
}

/**创建一个 NSQd */
func NewNSQd(workerId int64, options *nsqdOptions) *NSQd {
	n := &NSQd{
		workerId:   workerId,
		options:    options,
		topicMap:   make(map[string]*Topic),
		idChan:     make(chan nsq.MessageID, 4096),
		exitChan:   make(chan int),
		notifyChan: make(chan interface{}),
	}

	n.waitGroup.Wrap(func() { n.idPump() })

	return n
}

func (n *NSQd) Main() {
	n.waitGroup.Wrap(func() { n.lookupLoop() })

	tcpListener, err := net.Listen("tcp", n.tcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.tcpAddr, err.Error())
	}
	n.tcpListener = tcpListener
	n.waitGroup.Wrap(func() { util.TcpServer(n.tcpListener, &TcpProtocol{protocols: protocols}) })

	httpListener, err := net.Listen("tcp", n.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.httpAddr, err.Error())
	}
	n.httpListener = httpListener
	n.waitGroup.Wrap(func() { httpServer(n.httpListener) })
}

// {
//	"topics":
//		[{
//			"channels":[
//				{"name":"testChannel","paused":false},
//				{"name":"testChannel2","paused":false}
//			],
//			"name":"testTopic"
//		}],
//	"version":"0.2.21"
//	}
// 从 nsqd.%d.dat 配置文件中读取 topic 和 channel 的信息
// 如果当前容器中包含 topic, 则获取 topic, 否则新建一个topic
// topic中的channel也同上,创建 or 新建
// channel 是否暂停, 又 配置文件中的 paused 属性来决定
func (n *NSQd) LoadMetadata() {
	log.Println("nsqd.LoadMetadata() start ")
	// 从文件中读取信息, 具体信息 见头部
	fn := fmt.Sprintf(path.Join(n.options.dataPath, "nsqd.%d.dat"), n.workerId)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("ERROR: failed to read channel metadata from %s - %s", fn, err.Error())
		}
		return
	}
	//json解析
	js, err := simplejson.NewJson(data)
	if err != nil {
		log.Printf("ERROR: failed to parse metadata - %s", err.Error())
		return
	}
	//获取配置文件中 topics 信息
	topics, err := js.Get("topics").Array()
	if err != nil {
		log.Printf("ERROR: failed to parse metadata - %s", err.Error())
		return
	}

	//遍历 topics 信息
	for ti := range topics {
		topicJs := js.Get("topics").GetIndex(ti)
		//取得 topic 的 name
		topicName, err := topicJs.Get("name").String()
		if err != nil {
			log.Printf("ERROR: failed to parse metadata - %s", err.Error())
			return
		}
		// 校验 name 是否符合规则
		if !nsq.IsValidTopicName(topicName) {
			log.Printf("WARNING: skipping creation of invalid topic %s", topicName)
			continue
		}
		// 当前容器中获取 topic 信息, 如果容器中不存在, 则新建一个 topic
		topic := n.GetTopic(topicName)
		//配置文件中获取 channels 配置信息
		channels, err := topicJs.Get("channels").Array()
		if err != nil {
			log.Printf("ERROR: failed to parse metadata - %s", err.Error())
			return
		}
		//遍历 channels 信息
		for ci := range channels {
			channelJs := topicJs.Get("channels").GetIndex(ci)
			//取得 channelName
			channelName, err := channelJs.Get("name").String()
			if err != nil {
				log.Printf("ERROR: failed to parse metadata - %s", err.Error())
				return
			}
			// 校验 channelName 是否符合规则
			if !nsq.IsValidChannelName(channelName) {
				log.Printf("WARNING: skipping creation of invalid channel %s", channelName)
				continue
			}
			// 当前topic中获取 channel 信息, 如果容器中不存在, 则新建一个 channel
			channel := topic.GetChannel(channelName)
			// 根据配置文件中的 channel 信息, 来决定,是否暂停还是继续启动该 channel
			paused, _ := channelJs.Get("paused").Bool()
			if paused {
				channel.Pause()
			}
		}
	}
}

// 将容器中的 topic 信息 写入文件中
func (n *NSQd) PersistMetadata() error {
	log.Println("nsqd.PersistMetadata() start ")
	// persist metadata about what topics/channels we have
	// so that upon restart we can get back to the same state
	fileName := fmt.Sprintf(path.Join(n.options.dataPath, "nsqd.%d.dat"), n.workerId)
	log.Printf("NSQ: persisting topic/channel metadata to %s", fileName)

	js := make(map[string]interface{})
	topics := make([]interface{}, 0)
	for _, topic := range n.topicMap {
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		channels := make([]interface{}, 0)
		topic.Lock()
		for _, channel := range topic.channelMap {
			channel.Lock()
			if !channel.ephemeralChannel {
				channelData := make(map[string]interface{})
				channelData["name"] = channel.name
				channelData["paused"] = channel.IsPaused()
				channels = append(channels, channelData)
			}
			channel.Unlock()
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	js["version"] = util.BINARY_VERSION
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fileName + ".tmp"
	f, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}

	return nil
}

/**
容器退出
tcp坚挺器关闭, http监听器关闭;持久化 topic和 channel信息
*/
func (n *NSQd) Exit() {
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		log.Printf("ERROR: failed to persist metadata - %s", err.Error())
	}
	log.Printf("NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

	// we want to do this last as it closes the idPump (if closed first it
	// could potentially starve items in process and deadlock)
	close(n.exitChan)
	n.waitGroup.Wait()
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
func (n *NSQd) GetTopic(topicName string) *Topic {
	n.Lock()
	t, ok := n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	} else {
		t = NewTopic(topicName, n.options, n)
		n.topicMap[topicName] = t
		log.Printf("TOPIC(%s): created", t.name)

		// release our global nsqd lock, and switch to a more granular topic lock while we init our
		// channels from lookupd. This blocks concurrent PutMessages to this topic.
		t.Lock()
		n.Unlock()
		// if using lookupd, make a blocking call to get the topics, and immediately create them.
		// this makes sure that any message received is buffered to the right channels
		if len(n.lookupPeers) > 0 {
			channelNames, _ := lookupd.GetLookupdTopicChannels(t.name, n.lookupHttpAddrs())
			for _, channelName := range channelNames {
				t.getOrCreateChannel(channelName)
			}
		}
		t.Unlock()

		// NOTE: I would prefer for this to only happen in topic.GetChannel() but we're special
		// casing the code above so that we can control the locks such that it is impossible
		// for a message to be written to a (new) topic while we're looking up channels
		// from lookupd...
		//
		// update messagePump state
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQd) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQd) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

// id生成器, id生成完毕后,存放在 n.idChan 中
func (n *NSQd) idPump() {
	log.Println("nsqd.idPump() start ")
	lastError := time.Now()
	for {
		id, err := NewGUID(n.workerId)
		if err != nil {
			now := time.Now()
			if now.Sub(lastError) > time.Second {
				// only print the error once/second
				log.Printf("ERROR: %s", err.Error())
				lastError = now
			}
			runtime.Gosched()
			continue
		}
		select {
		case n.idChan <- id.Hex():
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	log.Printf("ID: closing")
}

/**添加一个通知事件, 包括通知和退出事件
@tosee lookup.go-->lookupLoop()-->case val := <-n.notifyChan:
*/
func (n *NSQd) Notify(v interface{}) {
	// by selecting on exitChan we guarantee that
	// we do not block exit, see issue #123
	select {
	//退出
	case <-n.exitChan:
	//通知
	case n.notifyChan <- v:
	}
}
