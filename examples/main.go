package main

import (
	"fmt"

	"github.com/stgleb/hydra_pubsub/pkg/pubsub"
)

func main() {
	topicManager := pubsub.NewTopicManager(10)
	topicManager.Subscribe("my-topic", "subscriber1")
	topicManager.Subscribe("my-topic", "subscriber2")
	// Write message to topic
	topicManager.WriteMessage("my-topic", []byte("hello"))

	// Check if messages get delivered to every subscriber
	fmt.Println(string(<-topicManager.Poll("my-topic", "subscriber1")))
	fmt.Println(string(<-topicManager.Poll("my-topic", "subscriber2")))
}
