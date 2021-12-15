const Producer = require('node-rdkafka').Producer;
const Client = require('./client');

class KafkaProducer extends Client {

    /**
     * Initializes a KafkaProducer.
     * @param {String} clientId: id to identify a client producing the message.
     * @param {import('node-rdkafka').ProducerGlobalConfig} config: configs for producer.
     * @param {import('node-rdkafka').ProducerTopicConfig} topicConfig: topic configs.
     * @param {EventEmitter} emitter: to emit log messages
     */
    constructor(clientId, config, topicConfig, emitter) {
        super(clientId, 'producer', emitter);
        this.config = Object.assign({
            'metadata.broker.list': 'localhost:9092',
            'retry.backoff.ms': 200,
            'message.send.max.retries': 10,
            'socket.keepalive.enable': true,
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.ms': 1000,
            'batch.num.messages': 1000000,
            'dr_cb': true
          }, 
          config,
          { 'client.id' : clientId }
        );
        this.topicConfig = Object.assign({
            'acks' : 1,
        }, topicConfig);
        this.producer = new Producer(this.config);
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
                this.producer
                .connect()
                .on('ready', (info, metadata) => {
                    this.success('Producer connected to kafka cluster...', {
                        name: info.name,
                        metadata: JSON.stringify(metadata),
                    });
                    resolve(this);
                })
                .on('delivery-report', (err, report) => {
                    if (err) {
                        this.error('Error producing message: ', err);
                    } else {
                        this.log(`Produced event: key=${report.key}, timestamp=${report.timestamp}.`);
                    }
                })
                .on('event.error', (err) => {
                    this.error('Producer encountered error: ', err);
                    reject(err);
                })
                .on('event.log',  (eventData) => this.log('Logging consumer event: ', eventData))
                .on('disconnected', (metrics) => {
                    this.log('Producer disconnected. Client metrics are: ', metrics.connectionOpened);
                });   
            } catch (err) {
                this.error('Producer encountered while connecting to kafka server.', err);
                reject(err);
            }
        });
    }

    /**
     * Produce a message to a topic-partition.
     * @param {String} topic: name of topic 
     * @param {import('node-rdkafka').NumberNullUndefined} partition: partition number to produce to.
     * @param {import('../types').StringMessageValue} message: message to be produced. 
     * @param {import('node-rdkafka').MessageKey} key: key associated with the message.
     * @param {import('node-rdkafka').NumberNullUndefined} timestamp: timestamp to send with the message. 
     * @returns {import('../types').BooleanOrNumber}: returns boolean or librdkafka error code.
     */
    produce(topic, partition, message, key, timestamp) {
        try {
            const isSuccess = this.producer.produce(topic, partition, Buffer.from(message), key, timestamp, null);
            // poll everytime, after producing events to see any new delivery reports.
            this.producer.poll();
            return isSuccess;
        } catch (err) {
            this.error(`Producer encountered error while producing message to topic=${topic}, partition=${partition} with key=${key}`, err);
            return false;
        }
    }

    /**
     * Flush everything on the internal librdkafka buffer. 
     * Good to perform before disconnect.
     * @param {import('node-rdkafka').NumberNullUndefined}} timeout 
     * @param {import('../types').ErrorHandlingFunction} postFlushAction 
     * @returns {KafkaProducer}
     */
    flush(timeout, postFlushAction) {
        try {
            this.producer.flush(timeout, postFlushAction);
        } catch (err) {
            this.error('Producer encountered error while flusing events.', err);
        }
        return this;
    }

    /**
     * Disconnects producer.
     * @param {import('../types').DisconnectFunction} postDisconnectAction 
     * @returns {KafkaProducer}
     */
    disconnect(postDisconnectAction) {
        try {
            this.producer.disconnect(postDisconnectAction);
        } catch (err) {
            this.error('Producer encountered error while disconnecting.', err);
        }
        return this;
    }
}

module.exports = KafkaProducer;