package test

import (
	"github.com/jianghaodong-icel/kafka-in-action/producer"
	"testing"
)

func TestSyncProduce(t *testing.T) {
	producer.SyncProduce()
}

func TestAsyncProduce(t *testing.T) {
	producer.AsyncProduce()
}

func TestProtobufProduce(t *testing.T) {
	producer.ProtobufProduce()
}
