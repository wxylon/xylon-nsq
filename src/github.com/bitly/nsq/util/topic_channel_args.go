package util

import (
	"errors"
	"github.com/bitly/nsq/nsq"
)

/**参见 req_params.go ReqParams 和 PostParams*/
type Getter interface {
	Get(key string) (string, error)
}

func GetTopicChannelArgs(rp Getter) (string, string, error) {
	topicName, err := rp.Get("topic")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_TOPIC")
	}

	if !nsq.IsValidTopicName(topicName) {
		return "", "", errors.New("INVALID_ARG_TOPIC")
	}

	channelName, err := rp.Get("channel")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_CHANNEL")
	}

	if !nsq.IsValidChannelName(channelName) {
		return "", "", errors.New("INVALID_ARG_CHANNEL")
	}

	return topicName, channelName, nil
}
