import { ClientMetrics, ConsumerGlobalConfig, ConsumerTopicConfig, LibrdKafkaError, Message, MessageKey, NumberNullUndefined, ProducerGlobalConfig, ProducerTopicConfig, SubscribeTopicList } from "node-rdkafka";

export type ConsumerActionFunction = (err: LibrdKafkaError, messages: Message[]) => void;

export type ErrorHandlingFunction = (err: LibrdKafkaError) => void;

export type DisconnectFunction = (err: any, data: ClientMetrics) => any;

export type StringMessageValue = string | null; 

export type BooleanOrNumber = boolean | number;

export class KafkaConsumer {
    constructor(clientId: string, groupId: string, config: ConsumerGlobalConfig, topicConfig: ConsumerTopicConfig);
    connect(): Promise<this | LibrdKafkaError>;
    subscribe(topics: SubscribeTopicList): this;
    unsubscribe(): this;
    consume(actionOnData: ConsumerActionFunction): void;
    consumeBatch(msgCount: number, actionOnData: ConsumerActionFunction): void;
}

export class KafkaProducer {
    constructor(clientId: string, config: ProducerGlobalConfig, topicConfig: ProducerTopicConfig);
    connect(): Promise<this | LibrdKafkaError>;
    produce(topic: string, partition: NumberNullUndefined, message: StringMessageValue, key?: MessageKey, timestamp?: NumberNullUndefined): BooleanOrNumber;
    flush(timeout?: NumberNullUndefined, postFlushAction?: ErrorHandlingFunction): this;
    disconnect(postDisconnectAction?: DisconnectFunction): this;
}