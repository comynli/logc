package main

import (
	"encoding/base64"
	"fmt"
	zookeeper "github.com/samuel/go-zookeeper/zk"
	tomb "gopkg.in/tomb.v2"
	"os"
	"path"
	"strings"
	"time"
)

type Collector struct {
	zk        *zookeeper.Conn
	event     <-chan zookeeper.Event
	tailers   map[string]*Tailer
	producers []*Producer
	queue     chan []byte
	cur       int
	tomb.Tomb
}

func NewCollector() (*Collector, error) {
	var (
		collector Collector
		ok        bool
	)

	zk, session, err := zookeeper.Connect(conf.ZKConfig.Hosts, time.Duration(10)*time.Second)
	if err != nil {
		logger.Crit(Format("Connect zookeeper %s error %v\n", strings.Join(conf.ZKConfig.Hosts, ","), err))
		return &collector, err
	}

	ok, _, err = zk.Exists(conf.ZKConfig.Root)
	if err != nil {
		logger.Crit(Format("Zookeeper error %v\n", err))
		return &collector, fmt.Errorf("Zookeeper error %v\n", err)
	}
	if !ok {
		logger.Warning(Format("Root path %s not exists, create it\n", conf.ZKConfig.Root))
		_, err = zk.Create(conf.ZKConfig.Root, []byte("0"), 0, zookeeper.WorldACL(zookeeper.PermAll))
		if err != nil {
			logger.Crit(Format("Create root path %s error %v", conf.ZKConfig.Root, err))
			return &collector, fmt.Errorf("Create root path %s error %v", conf.ZKConfig.Root, err)
		}
	}

	_, err = os.Stat(conf.StatusDB)
	if err != nil && os.IsNotExist(err) {
		os.MkdirAll(conf.StatusDB, os.ModePerm)
		return &collector, err
	}

	collector.producers = make([]*Producer, conf.ProducerConfig.Num)
	for i := 0; i < conf.ProducerConfig.Num; i++ {
		producer, err := NewProducer()
		if err != nil {
			logger.Crit(Format("init producer fail %v\n", err))
			return &collector, err
		}
		collector.producers[i] = producer
	}

	collector.zk = zk
	collector.event = session
	collector.tailers = make(map[string]*Tailer)
	collector.cur = 0
	collector.queue = make(chan []byte)
	c := &collector
	c.Go(c.loop)
	return &collector, nil
}

func (c *Collector) loop() error {

	defer c.zk.Close()
	c.init()
	heartbeat := Format("%s/heartbeat/%s", conf.ZKConfig.Root, conf.Source)
	now, _ := time.Now().MarshalText()
	_, err := c.zk.Create(heartbeat, now, zookeeper.FlagEphemeral, zookeeper.WorldACL(zookeeper.PermAll))
	if err != nil {
		logger.Crit(Format("create heartbeat node fail: %v\n", err))
		return err
	}

	for {
		select {
		case <-c.Dying():
			return nil
		case message := <-c.queue:
			logger.Debug(string(message))
			c.send(conf.ProducerConfig.Topic, message)
		case <-time.After(time.Duration(1) * time.Minute):
			for key := range c.tailers {
				logger.Info(Format("%s is running", key))
			}
			logger.Info("reload config")
			c.restart()
		}
	}

}

//func (c *Collector) Start() {
//	c.Go(c.loop)
//}

func (c *Collector) Stop() error {
	// stop tailers
	for key, value := range c.tailers {
		value.Stop()
		delete(c.tailers, key)
	}
	//stop producers
	for _, p := range c.producers {
		p.Stop()
	}

	c.Kill(nil)
	return c.Wait()
}

func (c *Collector) init() {
	err := c.zkCreateNode(Format("%s/heartbeat", conf.ZKConfig.Root))
	if err != nil {
		c.Kill(err)
		return
	}

	err = c.zkCreateNode(Format("%s/applog", conf.ZKConfig.Root))
	if err != nil {
		c.Kill(err)
		return
	}

	err = c.zkCreateNode(Format("%s/accesslog", conf.ZKConfig.Root))
	if err != nil {
		c.Kill(err)
		return
	}

	err = c.zkCreateNode(Format("%s/tracelog", conf.ZKConfig.Root))
	if err != nil {
		c.Kill(err)
		return
	}
}

func (c *Collector) zkCreateNode(node string) error {
	var (
		ok  bool
		err error
	)

	ok, _, err = c.zk.Exists(node)
	if err != nil {
		logger.Crit(Format("zookeeper error<exists node %s> %v\n", node, err))
		return err
	}
	if ok {
		return nil
	}
	parent := path.Dir(node)
	err = c.zkCreateNode(parent)
	if err != nil {
		return err
	}
	_, err = c.zk.Create(node, []byte(""), 0, zookeeper.WorldACL(zookeeper.PermAll))
	if err != nil {
		logger.Crit(Format("zookeeper error<create node %s> %v\n", node, err))
		return err
	}
	return nil

}

func (c *Collector) send(topic string, message []byte) {
	c.producers[c.cur].Send(topic, message)
	c.cur = (c.cur + 1) % conf.ProducerConfig.Num
}

func (c *Collector) restart() error {
	var tailer *Tailer
	childrenMap := make(map[string]string)
	zkPath := Format("%s/%s", conf.ZKConfig.Root, conf.App)
	children, _, err := c.zk.Children(zkPath)
	if err != nil {
		logger.Err(Format("watch %s fail %v\n", zkPath, err))
		return err
	}

	for _, child := range children {
		logPath, err := c.decode(path.Base(child))
		if err != nil {
			logger.Err(Format("decode log path %s fail %v\n", child, err))
			continue
		}
		data, _, err := c.zk.Get(Format("%s/tags", child))
		var tags []string
		if err != nil {
			tags = []string{}
		} else {
			tags = strings.Split(string(data), ",")
		}
		childrenMap[logPath] = child
		_, ok := c.tailers[logPath]
		if !ok {
			tailer, err = NewTailer(logPath, c.queue, tags)
			if err != nil {
				logger.Err(Format("init tailer for %s fail %v\n", logPath, err))
				continue
			}
			c.tailers[logPath] = tailer
		}
	}

	for key, value := range c.tailers {
		_, ok := childrenMap[key]
		if !ok {
			value.Stop()
			delete(c.tailers, key)
		}
	}
	return nil
}

func (c *Collector) encode(src string) string {
	return base64.URLEncoding.EncodeToString([]byte(src))
}

func (c *Collector) decode(src string) (string, error) {
	dst, err := base64.URLEncoding.DecodeString(src)
	return string(dst), err
}
