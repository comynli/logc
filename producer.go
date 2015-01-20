package main

import (
	"github.com/Shopify/sarama"
	tomb "gopkg.in/tomb.v2"
	"time"
)

type Producer struct {
	queue    chan *sarama.MessageToSend
	client   *sarama.Client
	producer *sarama.Producer
	tomb.Tomb
}

func NewProducer() (*Producer, error) {
	producer := &Producer{}

	// init kafka client
	client, err := sarama.NewClient("logc", conf.ProducerConfig.Brokers, sarama.NewClientConfig())
	if err != nil {
		logger.Crit(Format("connect to kafka fail %v\n", err))
		return producer, err
	}
	// producer config
	pconf := sarama.NewProducerConfig()
	pconf.FlushMsgCount = conf.ProducerConfig.FlushMsgCount
	pconf.FlushFrequency = time.Duration(conf.ProducerConfig.FlushFrequency) * time.Second
	pconf.Compression = sarama.CompressionSnappy //use snappy compression

	p, err := sarama.NewProducer(client, pconf)
	if err != nil {
		logger.Crit(Format("get producer fail %v\n", err))
		return producer, err
	}

	producer.producer = p
	producer.client = client
	producer.queue = make(chan *sarama.MessageToSend, conf.ProducerConfig.QueueSize)
	producer.Go(producer.loop)
	return producer, nil
}

func (p *Producer) Send(topic string, message []byte) {
	p.queue <- &sarama.MessageToSend{Topic: topic, Key: nil, Value: sarama.ByteEncoder(message)}
}

func (p *Producer) loop() error {
	defer p.client.Close()
	defer p.producer.Close()
	for {
		select {
		case message := <-p.queue:
			p.producer.Input() <- message
		case err := <-p.producer.Errors():
			logger.Warning(Format("send message fail %v\n", err))
		case <-p.Dying():
			return nil
		default:
			<-time.After(time.Duration(1) * time.Second)
		}
	}
}

func (p *Producer) Stop() error {
	p.Kill(nil)
	return p.Wait()
}
