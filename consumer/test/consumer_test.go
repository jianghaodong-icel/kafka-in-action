package test

import (
	"github.com/jianghaodong-icel/kafka-in-action/consumer"
	"testing"
)

func TestConsumeAtomicCommit(t *testing.T) {
	consumer.ConsumeAtomicCommit()
}

func TestConsumeManualCommit(t *testing.T) {
	consumer.ConsumeManualCommit()
}

func TestConsumeByGroup(t *testing.T) {
	consumer.ConsumeByGroup()
}
