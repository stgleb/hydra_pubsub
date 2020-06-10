package pubsub

type Consumer struct{
	// Name of consumer
	Name string
	// Receive channel where listener gets messages from
	Receive chan []byte
	// LastRead index of last read message
	LastRead int
}
