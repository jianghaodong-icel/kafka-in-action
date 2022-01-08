package main

import "github.com/jianghaodong-icel/kafka-in-action/get_start"

func main() {
	get_start.SyncProduce()

	get_start.AsyncProduce()

	get_start.ProtobufProduce()
}
