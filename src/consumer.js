const Consumer = require('node-rdkafka').KafkaConsumer;
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
        super(clientId, 'consumer', emitter);
        this.config = Object.assign({
            'metadata.broker.list': 'localhost:9092',
            'socket.keepalive.enable': true,
            'allow.auto.create.topics': true,
          }, 
          config,
          { 
            'client.id' : clientId,
            'group.id': groupId,
        }
        );
        this.topicConfig = topicConfig;
        this.consumer = new Consumer(this.config);
    }

    /**
     * Asynchronous function which connects to kafka cluster. 
     * Resolves when connection is ready.
     *
     * @returns {Promise} 
     */
    connect() {
        return new Promise((resolve, reject) => {
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
        });
    }

    /**
     * Subscribe to topics.
     * @param {import('node-rdkafka').SubscribeTopicList} topics: array of topic names. 
     * @returns {KafkaConsumer}
     */
    subscribe(topics) {
        this.consumer.subscribe(topics);
        return this;        
    }

    /**
     * Unsubscribe from all the subscribed topics.s
     * @returns {KafkaConsumer}
     */
    unsubscribe() {
        this.consumer.unsubscribe();
        return this;
    }

    /**
     * Consumes message one-by-one and executes actionsOnData callback
     * on the message read.
     * 
     * @param {Function} actionOnData: callback to return when message is read. 
     */
    consume(actionOnData) {
        this.consumer.consume(actionOnData);
    }

    /**
     * Consumes messages in a batch and executes actionsOnData callback
     * on the message read.
     * 
     * @param {Number} msgCount: number of messages to read.  
     * @param {Function | null} actionOnData: callback to be executed for each message.
     */
    consumeBatch(msgCount, actionOnData) {
        this.consumer.consume(msgCount, actionOnData);   
    }


}

module.exports = KafkaConsumer;