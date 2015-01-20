package main

import (
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

type ZKConfig struct {
	Hosts []string `hosts`
	Root  string   `root`
}

type ProducerConfig struct {
	Brokers        []string `brokers`
	FlushMsgCount  int      `flush.messages`
	FlushFrequency int      `flush.secondes`
	QueueSize      int      `queue.size`
	Num            int      `threads.num`
	Topic          string   `topic`
}

//type AlarmConfig struct {
//	ID      string       `id`
//	Title   string       `title`
//	Level   int          `level`
//	Servers AlarmServer  `servers`
//	Timeout AlarmTimeout `timeout`
//}
//
//type AlarmServer struct {
//	UDPServers []string `udp`
//	TCPServers []string `tcp`
//}
//
//type AlarmTimeout struct {
//	Conn  int `conn`
//	Read  int `read`
//	Write int `write`
//	Retry int `retry`
//}

type Conf struct {
	ProducerConfig ProducerConfig `producer`
	ZKConfig       ZKConfig       `zookeeper`
	//	AlarmConfig    AlarmConfig    `alarm`
	App            string `app`
	Source         string `source`
	StatusDB       string `status`
	MaxPendingSize int64  `max.pending.size`
	//	Topics         Topics         `topics`
}

func InitConf(cfgFile string) (Conf, error) {
	var c Conf
	content, err := ioutil.ReadFile(cfgFile)
	if err != nil {
		logger.Crit(Format("read config fail %v\n", err))
		return c, err
	}
	err = yaml.Unmarshal(content, &c)
	if c.Source == "" {
		c.Source, err = os.Hostname()
	}
	return c, err
}
