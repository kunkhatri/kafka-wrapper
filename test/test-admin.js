const KafkaAdmin  = require('..').KafkaAdmin;


function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function adminExample() {

if (process.argv.length < 6) {
    console.log(
        'Please provide command line arguments to the script.\n' +
        'Expected arguments in order are: `bootstrap-server`, `new-topic`, `parititions` and `replication-factor`. Example....\n' + 
        'node test-admin.js bootstrap-servers=34.229.149.56:9092,54.196.127.213:9092 new-topic=test-topic partitions=10 replication-factor=2'
    );
    process.exit(1);
    }

    const args = process.argv.slice(2);
    const kwargs = {};
    const expectedKeywords = ['bootstrap-servers', 'new-topic', 'partitions', 'replication-factor']
    for (let arg of args) {
    const kwarg = arg.split('=');
    if (!expectedKeywords.includes(kwarg[0])) {
        console.log('Unexpected command line argument keyword. Only expected keywords are: ', expectedKeywords);
        process.exit(1);       
    }
    kwargs[kwarg[0]] = kwarg[1];
    }
    const topic = kwargs['new-topic'];
    const bootstrapServers = kwargs['bootstrap-servers'];
    const partitions = parseInt(kwargs['partitions'], 10);
    const replicationFactor = parseInt(kwargs['replication-factor'], 10);
    console.log('bootstrap-servers: ', bootstrapServers);
    console.log('topic: ', topic);
    console.log('partitions: ', partitions, typeof partitions);
    console.log('replicationFactor: ', replicationFactor, typeof replicationFactor);
  
    const newTopic = {
      topic,
      num_partitions: partitions,
      replication_factor: replicationFactor,
    };

    const admin = new KafkaAdmin(
        'test-admin-client', 
        { 'metadata.broker.list': bootstrapServers }
    );
    await admin.connect();

    admin.createTopic(newTopic, (err) => {
      if (err) {
        console.log(err);
        return;
      }
      console.log('created topic successfully');
    });
  }
  
  adminExample()
    .catch((err) => {
      console.error(`Something went wrong:\n${err}`);
      process.exit(1);
    });
