const Kafka = require('node-rdkafka');
const Client = require('./client');

class KafkaConsumer extends Client{

    /**
     * Initializes a KafkaConsumer.
     * @param {String} clientId: id to identify a client consuming the message. 
     * @param {String} groupId: consumer group id, the consumer belongs to. 
     * @param {import('node-rdkafka').ConsumerGlobalConfig} config: configs for consumer.
     * @param {import('node-rdkafka').ConsumerTopicConfig} topicConfig: topic configs 
     * @param {EventEmitter} emitter: to emit log events
     */
    constructor(clientId, groupId, config, topicConfig, emitter) {
        // consumer specific default configs we would like to have
        config = Object.assign({
            'allow.auto.create.topics': true,
          }, 
          config,
          { 
            'group.id': groupId,
        });
        super(clientId, 'consumer', config, topicConfig, emitter);
        this.consumer = new Kafka.KafkaConsumer(this.config, this.topicConfig);
    }

    /**
     * Asynchronous function which connects to kafka cluster. 
     * Resolves when connection is ready.
     *
     * @returns {Promise} 
     */
    connect() {
        return new Promise((resolve, reject) => {
            try {
                this.consumer
                .connect()
                .on('ready', (info, metadata) => {
                    console.log('connected');
                    this.success('Consumer connected to kafka cluster....', {
                        name: info.name,
                        metadata: JSON.stringify(metadata),
                    });
                    resolve(this);
                })
                .on('event.error', (err) => {
                    this.error('Consumer encountered error: ', err);
                    reject(err);
                })
                .on('event.log',  (eventData) => this.log('Logging consumer event: ', eventData))
                .on('disconnected', (metrics) => {
                    this.log('Consumer disconnected. Client metrics are: ', metrics.connectionOpened);
                })
            } catch (err) {
                this.error('Consumer encountered while connecting to kafka server.', err);
                reject(err);
            }
        });
    }

    /**
     * Subscribe to topics.
     * @param {import('node-rdkafka').SubscribeTopicList} topics: array of topic names. 
     * @returns {KafkaConsumer}
     */
    subscribe(topics) {
        try {
            this.consumer.subscribe(topics);
        } catch (err) {
            this.console.error(`Consumer encountered error while subscribing to topics=${topics}`, err);
        }
        return this;
    }

    /**
     * Unsubscribe from all the subscribed topics.s
     * @returns {KafkaConsumer}
     */
    unsubscribe() {
        try {
            this.consumer.unsubscribe();
        } catch (err) {
            this.console.error('Consumer encountered error while unsubscribing', err);
        }
        return this;
    }

    /**
     * Consumes message one-by-one and executes actionsOnData callback
     * on the message read.
     * 
     * @param {Function} actionOnData: callback to return when message is read. 
     */
    consume(actionOnData) {
        try {
            this.consumer.consume(actionOnData);
        } catch (err) {
            this.error('Consumer encountered error while consuming messages', err);
        }
    }

    /**
     * Consumes messages in a batch and executes actionsOnData callback
     * on the message read.
     * 
     * @param {Number} msgCount: number of messages to read.  
     * @param {Function | null} actionOnData: callback to be executed for each message.
     */
    consumeBatch(msgCount, actionOnData) {
        try {
            this.consumer.consume(msgCount, actionOnData);   
        } catch (err) {
            this.error(`Consumer encountered error while consuming messages in batch of size=${msgCount}`, err)
        }
    }


}

module.exports = KafkaConsumer;