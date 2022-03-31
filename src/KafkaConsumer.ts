import {
  Consumer,
  ConsumerSubscribeTopic,
  Kafka,
  EachMessagePayload,
  KafkaConfig,
} from 'kafkajs';

export class KafkaConsumer {
  private consumer: Consumer;
  private readonly topic: ConsumerSubscribeTopic;

  private constructor(consumer: Consumer, topic: ConsumerSubscribeTopic) {
    this.consumer = consumer;
    this.topic = topic;
  }

  static create(
    kafkaConfig: KafkaConfig,
    groupId: string,
    topic: ConsumerSubscribeTopic
  ): KafkaConsumer {
    const kafka = new Kafka(kafkaConfig);
    const consumer = kafka.consumer({ groupId });
    return new KafkaConsumer(consumer, topic);
  }

  public async startConsumer(): Promise<void> {
    await this.consumer.subscribe(this.topic);

    await this.consumer.run({
      eachMessage: async (messagePayload: EachMessagePayload) => {
        const { topic, partition, message } = messagePayload;
        const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
        console.log(`- ${prefix} ${message.key}#${message.value}`);
      },
    });
  }

  public async start(): Promise<void> {
    return this.consumer.connect();
  }

  public async shutdown(): Promise<void> {
    await this.consumer.disconnect();
  }
}
