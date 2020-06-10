package pubsub

// Topic structure for managing topic and messages
type Topic struct {
	// Name of topic
	Name string
	// subscribers map subscribed for topic
	subscribers map[string]*Consumer
	// subscribeChan channel for subscribe to topic
	subscribeChan chan *Consumer
	// UbscribeChan channel to unsubscribe for channel
	unsubscribeChan chan string
	// stopChan stops running goroutine
	stopChan chan struct{}
	// messageChan channel for receiving messages
	messageChan chan []byte
	// pollChan channel for polling for new messages
	pollChan chan messageRequest
	// message buffer
	messageBuffer [][]byte
	// index of oldest message
	oldestIndex int
	// latest index of message in message buffer
	latestIndex int
	// capacity of topic
	capacity int
}

type messageRequest struct {
	// subscriber name
	subscriberName string
	// channel to write back message
	writeBack chan []byte
}

func newTopic(name string, capacity int) *Topic {
	return &Topic{
		Name:            name,
		subscribers:     make(map[string]*Consumer),
		subscribeChan:   make(chan *Consumer),
		unsubscribeChan: make(chan string),
		stopChan:        make(chan struct{}),
		messageChan:     make(chan []byte),
		pollChan:        make(chan messageRequest),
		messageBuffer:   make([][]byte, 0, capacity),
		oldestIndex:     0,
		latestIndex:     0,
		capacity:        capacity,
	}
}

// Run goroutine for topic
func (t *Topic) Run() {
	for {
		select {
		case <-t.stopChan:
			return
		case message := <-t.messageChan:
			t.messageBuffer = append(t.messageBuffer, message)
			t.latestIndex += 1
		case consumer := <-t.subscribeChan:
			t.subscribers[consumer.Name] = consumer
		case consumerName := <-t.unsubscribeChan:
			delete(t.subscribers, consumerName)
			// If all subscribers are ahead of oldestIndex - advance it to oldest LastRead
			t.advanceOldestIndex()
		case messageReq := <-t.pollChan:
			subscriber := t.subscribers[messageReq.subscriberName]

			var i int
			// Skip older messages being read
			for i := t.oldestIndex; i < subscriber.LastRead; i++ {
			}
			message := t.messageBuffer[i-t.oldestIndex]
			subscriber.LastRead += 1
			messageReq.writeBack <- message
			// If all subscribers are ahead of oldestIndex - advance it to oldest LastRead
			t.advanceOldestIndex()
		}
	}
}

// Poll for new message for this subscriber
func (t *Topic) Poll(subscriberName string) chan []byte {
	writeBackChan := make(chan []byte, 1)
	messageReq := messageRequest{
		subscriberName: subscriberName,
		writeBack:      writeBackChan,
	}

	t.pollChan <- messageReq
	return writeBackChan
}

// Subscribe subscriberName to topic
func (t *Topic) Subscribe(subscriberName string) chan []byte {
	receiveChan := make(chan []byte)
	subscriber := &Consumer{
		Name:     subscriberName,
		Receive:  receiveChan,
		LastRead: t.oldestIndex,
	}
	t.subscribeChan <- subscriber
	return receiveChan
}

// Unsubscribe subscriberName to topic
func (t *Topic) Unsubscribe(subscriberName string) {
	t.unsubscribeChan <- subscriberName
}

// Write message to topic
func (t *Topic) WriteMessage(message []byte) {
	t.messageChan <- message
}

// Stop topic
func (t *Topic) Stop() {
	t.stopChan <- struct{}{}
}

// Advance Topic.oldestIndex if needed
func (t *Topic) advanceOldestIndex() {
	oldestSubscriberIndex := 1 << 63 - 1

	for _, subscriber := range t.subscribers {
		if subscriber.LastRead < oldestSubscriberIndex {
			oldestSubscriberIndex = subscriber.LastRead
		}
	}

	if oldestSubscriberIndex > t.oldestIndex {
		t.messageBuffer = t.messageBuffer[oldestSubscriberIndex-t.oldestIndex:]
		t.oldestIndex = oldestSubscriberIndex
	}
}
