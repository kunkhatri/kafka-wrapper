const KafkaProducer = require('./src/producer');
const KafkaConsumer = require('./src/consumer');
const KafkaAdmin = require('./src/admin');

module.exports = {
    KafkaAdmin,
    KafkaConsumer,
    KafkaProducer,
};