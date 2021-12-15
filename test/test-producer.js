const KafkaProducer  = require('..').KafkaProducer;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function producerExample() {
    if (process.argv.length < 4) {
      console.log(
        'Please provide command line arguments to the script.\n' +
        'Expected arguments in order are: `bootstrap-server` and `topic`. Example....\n' + 
        'node test-producer.js bootstrap-servers=34.229.149.56:9092,54.196.127.213:9092 topic=test-topic'
      );
      process.exit(1);
    }
  
    const args = process.argv.slice(2);
    const kwargs = {};
    const expectedKeywords = ['bootstrap-servers', 'topic']
    for (let arg of args) {
      const kwarg = arg.split('=');
      if (!expectedKeywords.includes(kwarg[0])) {
        console.log('Unexpected command line argument keyword. Only expected keywords are: ', expectedKeywords);
        process.exit(1);       
      }
      kwargs[kwarg[0]] = kwarg[1];
    }
    const topic = kwargs['topic'];
    const bootstrapServers = kwargs['bootstrap-servers'];
    console.log('bootstrap-servers: ', bootstrapServers);
    console.log('topic: ', topic);

    const users = [ "eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther" ];
    const items = [ "book", "alarm clock", "t-shirts", "gift card", "batteries" ];
  
    const producer = new KafkaProducer(
        'test-producer-client', 
        { 'metadata.broker.list': bootstrapServers }
    );
    await producer.connect();
  
    const numEvents = 1000;
    const times = 1000;
    
    for (let t = 0; t < times; t++) {
      for (let idx = 0; idx < numEvents; ++idx) {
  
        const key = users[Math.floor(Math.random() * users.length)];
        const value = items[Math.floor(Math.random() * items.length)];

        producer.produce(topic, null, value, key);
      }
      await sleep(1000);
    }
  
    producer.flush(10000, () => {
      producer.disconnect();
    });
  }
  
  producerExample()
    .catch((err) => {
      console.error(`Something went wrong:\n${err}`);
      process.exit(1);
    });