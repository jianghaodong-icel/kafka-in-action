package consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
)

/*
	title: 简单的消费者
*/

var (
	BrokerList = []string{"localhost:9092"}
	Topic      = "jhd-test"
	GroupID    = "test"
)

// ConsumeAtomicCommit 同步消费(消费消息都是同步的) 自动位移提交(间隔为1s)
func ConsumeAtomicCommit() {
	config := sarama.NewConfig()

	// 1. 创建消费者实例
	consumer, err := sarama.NewConsumer(BrokerList, config)
	if err != nil {
		panic(err)
	}

	// 2. 订阅主题，拉取消息
	// ConsumePartition
	// topic => 主题 | partition => 分区 | offset 偏移量
	// 注: offset 也可以指定 sarama.OffsetOldest，每次都会从最开始的位置消费消息
	partitionConsumer, err := consumer.ConsumePartition(Topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	for msg := range partitionConsumer.Messages() {
		fmt.Printf("consume msg info: Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %v\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
	}

	// 3. 关闭消费者实例
	_ = partitionConsumer.Close()
}

// ConsumeManualCommit 同步消费 手动提交
func ConsumeManualCommit() {
	wg := &sync.WaitGroup{}
	config := sarama.NewConfig()
	// 为验证手动提交，这里需要将自动提交关闭
	config.Consumer.Offsets.AutoCommit.Enable = false

	// 新建Kafka集群的一个操作实例
	client, err := sarama.NewClient(BrokerList, config)
	if err != nil {
		panic(err)
	}

	// 获取当前topic下的所有分区
	partitions, err := client.Partitions(Topic)
	if err != nil {
		panic(err)
	}

	// 新建offsetManager实例，用于提交消费偏移量
	offsetManager, err := sarama.NewOffsetManagerFromClient(GroupID, client)
	if err != nil {
		panic(err)
	}

	for _, partition := range partitions {
		wg.Add(1)
		go func() {
			defer wg.Done()

			partitionOffsetManager, err := offsetManager.ManagePartition(Topic, partition)
			if err != nil {
				panic(err)
			}

			// 1. 创建消费者实例
			consumer, err := sarama.NewConsumerFromClient(client)
			if err != nil {
				panic(err)
			}

			// 2. 订阅主题，拉取消息
			// 获取当前消费位置的起点
			nextOffset, _ := partitionOffsetManager.NextOffset()
			partitionConsumer, err := consumer.ConsumePartition(Topic, partition, nextOffset)
			for msg := range partitionConsumer.Messages() {
				fmt.Printf("consume msg info: Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %v\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				// 手动进行消费位移的提交
				partitionOffsetManager.MarkOffset(msg.Offset+1, "modify offset")
				offsetManager.Commit()
			}

			// 3. 关闭消费者实例
			_ = partitionConsumer.Close()
		}()
	}
	wg.Wait()

	_ = offsetManager.Close()
}
