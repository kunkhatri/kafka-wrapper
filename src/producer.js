const Producer = require('node-rdkafka').Producer;

class KafkaProducer {

    /**
     * Initializes a KafkaProducer.
     * @param {String} clientId: id to identify a client producing the message.
     * @param {Object} config: configs for producer.
     * @param {Object} topicConfig: topic configs.
     */
    constructor(clientId, config, topicConfig) {
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
            this.producer
            .connect()
            .on('ready', () => {
                console.log('Producer connected to kafka cluster...');
                resolve(this.producer);
            })
            .on('event.error', (err) => {
                console.warn('event.error: ', err);
                reject(err);
            })
            .on('event.log',  (log) => console.log(log))
            .on('disconnected', (msg) => {
                console.log('Producer disconnected. ' + JSON.stringify(msg));
            })

            if (typeof this.topicConfig.acks === 'number' && this.topicConfig.acks > 0 && this.config['dr_cb']) {
                this.producer.on('delivery-report', (err, report) => {
                    if (err) {
                        console.warn('Error producing message: ', err);
                    } else {
                        console.log('Produced event: ', JSON.stringify(report));
                    }
                })
            }
        });
    }

    /**
     * Produce a message to a topic-partition.
     * @param {String} topic: name of topic 
     * @param {String} message: message to be produced. 
     * @param {String} key: key associated with the message.
     * @param {Number | null} partition: partition number to produce to.
     * @param {Number | null} timestamp: timestamp to send with the message. 
     */
    produce(topic, partition, message, key, timestamp) {
        this.producer
        .produce(topic, partition, Buffer.from(message), key, timestamp, null);
        // poll everytime, after producing events to see any new delivery reports.
        this.producer.poll();
    }

    flush(timeout, postFlushAction) {
        this.producer.flush(timeout, postFlushAction);
        return this;
    }

    disconnect() {
        this.producer.disconnect();
    }
}

module.exports = KafkaProducer;