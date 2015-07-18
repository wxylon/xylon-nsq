package main

import (
	"bytes"
	"encoding/json"
	"github.com/bitly/nsq/nsq"
	"github.com/bitly/nsq/util"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

/**
1. 容器启动后, 将与初始化与nsqlookup server 的连接, 并且将容器中的 topic 和 channel 信息,注册到 nsqlookup server
2. topic 和 channel 创建 或者 销毁, 发送 注册或者取消注册命令到所有的nsqlookup server
3. 每隔15秒, 发送心跳包到所有的nsqlookup server
*/
func (n *NSQd) lookupLoop() {
	syncTopicChan := make(chan *nsq.LookupPeer)

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: failed to get hostname - %s", err.Error())
	}

	for _, host := range n.lookupdTCPAddrs {
		log.Printf("LOOKUP: adding peer %s", host)

		// 初始化 与 nsqlookup 连接, 并将客户端信息发送给服务器
		lookupPeer := nsq.NewLookupPeer(host, func(lp *nsq.LookupPeer) {
			ci := make(map[string]interface{})
			// 客户端版本号
			ci["version"] = util.BINARY_VERSION
			// 客户端tcp端口
			ci["tcp_port"] = n.tcpAddr.Port
			// 客户端http端口
			ci["http_port"] = n.httpAddr.Port
			// 客户端地址
			ci["address"] = hostname //TODO: drop for 1.0
			// 客户端机器名
			ci["hostname"] = hostname
			// 客户端绑定的ip地址
			ci["broadcast_address"] = n.options.broadcastAddress
			// 创建一个 包含客户端的一个配置信息 Command命令

			cmd, err := nsq.Identify(ci)
			if err != nil {
				lp.Close()
				return
			}
			// 执行 Command
			resp, err := lp.Command(cmd)
			// 执行命令异常
			if err != nil {
				log.Printf("LOOKUPD(%s): ERROR %s - %s", lp, cmd, err.Error())
				// 服务器响应异常
			} else if bytes.Equal(resp, []byte("E_INVALID")) {
				log.Printf("LOOKUPD(%s): lookupd returned %s", lp, resp)
				// 服务器响应正确
			} else {
				err = json.Unmarshal(resp, &lp.Info)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR parsing response - %v", lp, resp)
				} else {
					log.Printf("LOOKUPD(%s): peer info %+v", lp, lp.Info)
				}
			}

			go func() {
				syncTopicChan <- lp
			}()
		})
		lookupPeer.Command(nil) // start the connection
		n.lookupPeers = append(n.lookupPeers, lookupPeer)
	}

	// for announcements, lookupd determines the host automatically
	ticker := time.Tick(15 * time.Second)
	for {
		select {
		// 定时向服务器发送心跳信息
		case <-ticker:
			log.Printf("定时向 nsqlookup 服务器发送心跳信息")
			// send a heartbeat and read a response (read detects closed conns)
			for _, lookupPeer := range n.lookupPeers {
				log.Printf("LOOKUPD(%s): sending heartbeat", lookupPeer)
				cmd := nsq.Ping()
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err.Error())
				}
			}
		// topic 和 channel 创建 或者 销毁, 发送 注册或者取消注册命令到所有的nsqlookup server
		case val := <-n.notifyChan:
			var cmd *nsq.Command
			var branch string

			switch val.(type) {
			case *Channel:
				// notify all nsqlookupds that a new channel exists, or that it's removed
				branch = "channel"
				channel := val.(*Channel)
				if channel.Exiting() == true {
					log.Printf("topic: %s, channel: %s 退出完毕后, 取消注册到nsqlookup", channel.topicName, channel.name)
					cmd = nsq.UnRegister(channel.topicName, channel.name)
				} else {
					log.Printf("topic: %s, channel: %s 创建完毕后, 注册到nsqlookup", channel.topicName, channel.name)
					cmd = nsq.Register(channel.topicName, channel.name)
				}
			case *Topic:
				// notify all nsqlookupds that a new topic exists, or that it's removed
				branch = "topic"
				topic := val.(*Topic)
				if topic.Exiting() == true {
					log.Printf("topic: %s, 退出完毕后, 取消注册到nsqlookup", topic.name)
					cmd = nsq.UnRegister(topic.name, "")
				} else {
					log.Printf("topic: %s, 创建完毕后, 注册到nsqlookup", topic.name)
					cmd = nsq.Register(topic.name, "")
				}
			}

			for _, lookupPeer := range n.lookupPeers {
				log.Printf("LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err.Error())
				}
			}
		// 每次 与 nsqlookup server 初始化完毕后, 将同步 topic 和channel 信息至 nsqlookup server
		case lookupPeer := <-syncTopicChan:
			log.Printf("同步 topic 和channel 信息至 nsqlookup")
			commands := make([]*nsq.Command, 0)
			// build all the commands first so we exit the lock(s) as fast as possible
			nsqd.RLock()
			// 将所有的 topic 和channel 生成 注册 命令,存放于数组中
			for _, topic := range nsqd.topicMap {
				topic.RLock()
				if len(topic.channelMap) == 0 {
					log.Printf("同步 topic:%s 信息至 nsqlookup", topic.name)
					commands = append(commands, nsq.Register(topic.name, ""))
				} else {
					for _, channel := range topic.channelMap {
						log.Printf("同步 topic:%s 和channel:%s 信息至 nsqlookup", channel.topicName, channel.name)
						commands = append(commands, nsq.Register(channel.topicName, channel.name))
					}
				}
				topic.RUnlock()
			}
			nsqd.RUnlock()
			// 同步所有注册命令 到  nsqlookup server 中
			for _, cmd := range commands {
				log.Printf("LOOKUPD(%s): %s", lookupPeer, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err.Error())
					break
				}
			}
			//退出
		case <-n.exitChan:
			log.Printf("退出")
			goto exit
		}
	}

exit:
	log.Printf("LOOKUP: closing")
}

func (n *NSQd) lookupHttpAddrs() []string {
	var lookupHttpAddrs []string
	for _, lp := range n.lookupPeers {

		//TODO: remove for 1.0
		if len(lp.Info.BroadcastAddress) <= 0 {
			lp.Info.BroadcastAddress = lp.Info.Address
		}

		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HttpPort))
		lookupHttpAddrs = append(lookupHttpAddrs, addr)
	}
	return lookupHttpAddrs
}
