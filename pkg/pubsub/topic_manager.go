package pubsub

import (
	"sync"
)

type TopicManager struct {
	defaultCapacity int
	m               sync.RWMutex
	topics          map[string]*Topic
}

// NewTopicManager creates instance of topic manager
func NewTopicManager(defaultCapacity int) *TopicManager {
	return &TopicManager{
		defaultCapacity: defaultCapacity,
		topics:          map[string]*Topic{},
	}
}

// Poll for new message for this subscriber
func (t *TopicManager) Poll(topicName, subscriberName string) chan []byte {
	t.m.RLock()
	defer t.m.RUnlock()

	if topic, ok := t.topics[topicName]; ok {
		return topic.Poll(subscriberName)
	}

	return nil
}

// Subscribe subscriberName to topic
func (t *TopicManager) Subscribe(topicName, subscriberName string) chan []byte {
	t.m.Lock()
	defer t.m.Unlock()
	topic, ok := t.topics[topicName]

	if !ok || topic == nil {
		topic = newTopic(topicName, t.defaultCapacity)
		// spawn goroutine for topic
		go topic.Run()
		t.topics[topic.Name] = topic
	}

	return topic.Subscribe(subscriberName)
}

// Unsubscribe subscriberName to topic
func (t *TopicManager) Unsubscribe(topicName, subscriberName string) {
	t.m.Lock()
	defer t.m.Unlock()

	if topic, ok := t.topics[topicName]; ok {
		topic.Unsubscribe(subscriberName)
	}
}

// Write message to topic
func (t *TopicManager) WriteMessage(topicName string, message []byte) {
	t.m.Lock()
	defer t.m.Unlock()

	if topic, ok := t.topics[topicName]; ok {
		topic.WriteMessage(message)
	}
}

// Stop stops all goroutines for all topics
func (t *TopicManager) Stop() {
	t.m.Lock()
	defer t.m.Unlock()

	for _, topic := range t.topics {
		topic.Stop()
	}
}