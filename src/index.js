var Kafka = require('node-rdkafka');


var consumer = new Kafka.KafkaConsumer({
  'group.id': process.env.KAFKA_CONSUMER_GROUP_ID || 'kafka-0',
  'metadata.broker.list': process.env.KAFKA_BORKERS || 'localhost:9092',
  'offset_commit_cb': function(err, topicPartitions) {

    if (err) {
      // There was an error committing
      console.error(err);
    } else {
      // Commit went through. Let's log the topic partitions
      console.log(topicPartitions);
    }

  }
})


console.log("connecting consumer ...")
consumer.connect();
console.log("... consumer connected")

let i = 100
const TOPIC_NAME = process.env.KAFKA_TOPIC || 'sync-test'

consumer
	.on('ready', function() {
		console.log("consumer subscribing ...")
		consumer.subscribe([TOPIC_NAME]);
		console.log("... consumer subscribed")

		consumer.consume();
	})
	.on('data', function(data) {
		sleep(1000 + 9 * i--)
		// Output the actual message contents
		console.log(data.value.toString());
	});

function sleep(ms){
    return new Promise(resolve=>{
        setTimeout(resolve,ms)
    })
}

