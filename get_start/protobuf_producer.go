package get_start

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/jianghaodong-icel/kafka-in-action/get_start/student"
)

// ProtobufProduce 通过Protobuf序列化生产消息
func ProtobufProduce() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// 1. 构建生产者实例
	producer, err := sarama.NewSyncProducer(BrokerList, config)
	if err != nil {
		panic(err)
	}

	// 2. 构建待发送消息
	stu := &student.Student{
		Name: "jhd",
		Age:  18,
	}
	studentBytes, err := proto.Marshal(stu)
	if err != nil {
		panic(err)
	}
	message := &sarama.ProducerMessage{
		Topic: Topic,
		Value: sarama.ByteEncoder(studentBytes),
	}

	// 3. 发送消息
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		panic(err)
	}
	fmt.Printf("final result => partition: %d, offset: %d\n", partition, offset)

	// 4. 关闭生产者实例
	_ = producer.Close()
}
