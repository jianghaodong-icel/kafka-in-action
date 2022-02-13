package consumer

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
)

/*
	title: 消费者组
*/

type GroupHandler struct {
	name string
}

func (GroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Printf("set up claim %v\n", session.Claims())
	return nil
}

func (GroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Printf("clean up\n")
	return nil
}

func (GroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("consume msg info: Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %v\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
		// 手动更新偏移量，当然这里的自动更新也没有取消，可以当作兜底
		session.MarkMessage(msg, "modify offset")
		session.Commit()
	}
	return nil
}

func ConsumeByGroup() {
	config := sarama.NewConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. 新建消费者组实例
	consumerGroup, err := sarama.NewConsumerGroup(BrokerList, GroupID, config)
	if err != nil {
		panic(err)
	}

	// 2. 拉取消息，进行消费
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		groupHandler := &GroupHandler{
			name: "groupHandler",
		}
		for {
			err := consumerGroup.Consume(ctx, []string{Topic}, groupHandler)
			if err != nil {
				panic(err)
			}

			// 如果ctx被cancel掉，则退出
			if ctx.Err() != nil {
				return
			}
		}
	}()
	wg.Wait()

	// 3. 关闭消费者实例
	_ = consumerGroup.Close()
}
