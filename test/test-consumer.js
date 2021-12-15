const KafkaConsumer  = require('..').KafkaConsumer;


function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function consumerExample() {

if (process.argv.length < 4) {
    console.log(
        'Please provide command line arguments to the script.\n' +
        'Expected arguments in order are: `bootstrap-server` and `topic`. Example....\n' + 
        'node test-consumer.js bootstrap-servers=34.229.149.56:9092,54.196.127.213:9092 topic=test-topic'
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
  
    const consumer = new KafkaConsumer(
        'test-consumer-client', 
        'test-group1',
        { 'metadata.broker.list': bootstrapServers }
    );
    await consumer.connect();

    consumer
    .subscribe([topic]);
    
    const times = 100;
    for (let i = 0; i < times; i++) {
      consumer.consume((err, msg) => {
        console.log('error: ', err);
        console.log('msg read: ', msg);
      });
      console.log('sleeping...');
      await sleep(1000);
    }
  }
  
  consumerExample()
    .catch((err) => {
      console.error(`Something went wrong:\n${err}`);
      process.exit(1);
    });