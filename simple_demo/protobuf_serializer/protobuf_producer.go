package protobuf_serializer

import (
	"github.com/Shopify/sarama"
)

var (
	BrokerList = []string{"localhost:9092"}
	Topic      = "jhd-test"
)

func ProtobufSerializer() {
	config := sarama.NewConfig()

	// 1. 构建生产者实例
	producer, err := sarama.NewSyncProducer(BrokerList, config)
	if err != nil {
		panic(err)
	}

	// 2. 构建待发送消息

	message := sarama.ProducerMessage{
		Topic: Topic,
		Value:     ,
	}
}
