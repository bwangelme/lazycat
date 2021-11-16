## 消费者和消费者组

+ 多个消费者构成一个消费者组，它们拥有相同的 `group_id`
+ 主题的分区只能被消费者组中的单个消费者订阅。

![img.png](img.png)

如上图所示，消费者组 A 中每个消费者订阅了两个分区。消费者组B中的每个消费者订阅了1个分区。

生产者向主题发送消息时只会向一个分区发，只有订阅了该分区的消费者才能收到消息。

以上图为例，假设消费者向 P0 发送了消息，只有 C1 和 C3 才能够收到消息。

这就意味着，消费者组中的消费者是轮询着消费消息的(在消息均匀分布在所有分区的前提下)。

## 示例

1. 创建主题 `lazycat`，让它拥有两个分区:

```shell
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic lazycat

# 查看主题的信息，可以看到它有两个分区
ø> kafka-topics --bootstrap-server localhost:9092 --describe --topic lazycat
Topic: lazycat  PartitionCount: 2       ReplicationFactor: 1    Configs: segment.bytes=1073741824
        Topic: lazycat  Partition: 0    Leader: 0       Replicas: 0     Isr: 0
        Topic: lazycat  Partition: 1    Leader: 0       Replicas: 0     Isr: 0
```

2. 在本项目中，使用 `Ch2Consumer` 和 `Ch2Consumer 2` 启动两个消费者，它们都订阅了主题 `lazycat`，且订阅了不同的分区。
3. 使用 `Ch2Producer` 启动生产者，它会向分区1发送一条消息，修改 `lazycat/src/main/java/org/example/chapter2/App.java` 中 `partition` 为0，再次运行，让它向分区0发送消息。
4. 可以看到，消息只会发送到一个消费者中，不会两个消费者都收到消息。