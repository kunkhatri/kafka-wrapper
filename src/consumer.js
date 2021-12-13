const Consumer = require('node-rdkafka').KafkaConsumer;


class KafkaConsumer {

    /**
     * Initializes a KafkaConsumer.
     * @param {String} clientId: id to identify a client consuming the message. 
     * @param {String} groupId: consumer group id, the consumer belongs to. 
     * @param {Object} config: configs for consumer.
     * @param {Object} topicConfig: topic configs 
     */
    constructor(clientId, groupId, config, topicConfig) {
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
            .on('ready', () => {
                console.log('Consumer connected to kafka cluster....')
                resolve(this.consumer);
            })
            .on('event.error', (err) => {
                console.warn('event.error: ', err);
                reject(err);
            })
            .on('event.log',  (log) => console.log('Logging event: ', log))
            .on('disconnected', (msg) => {
                console.log('Consumer disconnected. ' + JSON.stringify(msg));
                console.log('Trying to connect it back.....');
                this.consumer.connect();
            })
        });
    }

    /**
     * Subscribe to topics.
     * @param {Array} topics: array of topic names. 
     */
    subscribe(topics) {
        this.consumer.subscribe(topics);
        return this;        
    }


    /**
     * Consumes message one-by-one and executes actionsOnData callback
     * on the message read.
     * 
     * @param {Function} actionOnData: callback to return when message is read. 
     */
    consume(actionOnData) {
        this._onDataReadEvent(actionOnData);
        this.consumer.consume();
    }

    /**
     * Consumes messages in a batch and executes actionsOnData callback
     * on the message read.
     * 
     * @param {Number} msgCount: number of messages to read.  
     * @param {Function | null} actionOnData: callback to be executed for each message.
     */
    consumeBatch(msgCount, actionOnData) {
        this._onDataReadEvent(actionOnData);
        this.consumer.consume(msgCount);   
    }

    _onDataReadEvent(actionOnData) {
        this.consumer.on('data', (msg) => {
            console.log(`Consumed message: 
                topic=${msg.topic}, partition=${msg.partition}, offset=${msg.offset}, 
                key=${msg.key}, size=${msg.size} bytes`
            );
            if (actionOnData && typeof actionOnData === 'function') {
                actionOnData({
                    value: msg.value.toString(),
                    key: msg.key,
                    topic: msg.topic,
                    partition: msg.partition,
                });
            }
        });
    }
}

module.exports = KafkaConsumer;