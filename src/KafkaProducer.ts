import {
  CompressionTypes,
  Kafka,
  KafkaConfig,
  Message,
  Producer,
  RecordMetadata,
} from 'kafkajs';

interface CustomMessage {
  a: string;
}

export class KafkaProducer {
  private producer: Producer;
  private readonly topic: string;

  private constructor(producer: Producer, topic: string) {
    this.producer = producer;
    this.topic = topic;
  }

  static create(kafkaConfig: KafkaConfig, topic: string): KafkaProducer {
    const kafka = new Kafka(kafkaConfig);
    return new KafkaProducer(kafka.producer(), topic);
  }

  public async send(messages: CustomMessage[]): Promise<RecordMetadata[]> {
    const kafkaMessages: Message[] = messages.map((message) => ({
      value: JSON.stringify(message),
    }));

    return this.producer.send({
      topic: this.topic,
      compression: CompressionTypes.GZIP,
      messages: kafkaMessages,
    });
  }

  public async start(): Promise<void> {
    return this.producer.connect();
  }

  public async shutdown(): Promise<void> {
    await this.producer.disconnect();
  }
}
