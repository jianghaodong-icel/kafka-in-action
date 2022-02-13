package producer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"sync"
)

/*
	title: 简单的生产者
 */

var (
	BrokerList = []string{"localhost:9092"}
	Topic      = "jhd-test"
)

// AsyncProduce 异步生产消息
func AsyncProduce() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true // 设置消息发送成功通知，消息发送失败同时默认存在

	// 1. 创建生产者实例
	producer, err := sarama.NewAsyncProducer(BrokerList, config)
	if err != nil {
		panic(err)
	}

	// 设置一个信号量以实现优雅的退出 => Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	var (
		wg                                        sync.WaitGroup // 同步信号量
		enqueued, produceSuccesses, produceErrors int            // 消息进队列数，生产成功数，生产失败数
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			produceSuccesses++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Errors() {
			produceErrors++
		}
	}()

ProduceLoop:
	for {
		// 2. 构建待发送的消息
		message := &sarama.ProducerMessage{
			Topic: Topic,
			Value: sarama.StringEncoder("Hello World!!"),
		}
		select {
		// 3. 发送消息
		case producer.Input() <- message:
			enqueued++
		case <-signals:
			// 4. 关闭生产者实例
			producer.AsyncClose()
			break ProduceLoop
		}
	}

	wg.Wait()
	fmt.Printf("final result => enqueued: %d, produceSuccesses: %d, produceErrors: %d \n", enqueued, produceSuccesses, produceErrors)
}

// SyncProduce 同步生产消息
func SyncProduce() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// 1. 创建生产者实例
	producer, err := sarama.NewSyncProducer(BrokerList, config)
	if err != nil {
		panic(err)
	}

	// 2. 构建待发送消息
	message := &sarama.ProducerMessage{
		Topic: Topic,
		Value: sarama.StringEncoder("Hello World!!"),
	}

	// 3. 发送消息
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		panic(err)
	}
	fmt.Printf("send message => partition: %d, offset: %d \n", partition, offset)

	// 4. 关闭生产者实例
	_ = producer.Close()
}
