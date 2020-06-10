### hydra pub-sub

Hydra pub-sub is a project intended to provider ready to use package
with all publish-subscribe logic inside

# Interface

Every interaction with topics are going through topic manager

example create topic manager with default capacity of 10

```
   topicMannager := NewTopicManager(10)
```

For any interaction with topic call appropriate method of topic manager

```
   topicManager.Subscribe("my-topic", "my-name")
   resultChan := topicManager.Poll("my-topic", "my-name")
   message := <- resultChan 
```