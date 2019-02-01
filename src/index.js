// cf. https://github.com/tulios/kafkajs/blob/master/examples/consumer.js
const { Kafka, logLevel } = require('kafkajs')

const kafka = new Kafka({
    logLevel: logLevel.INFO,
    brokers: (process.env.KAFKA_BORKERS || 'localhost:9092').split(","),
    clientId: 'example-consumer',
})

const consumer = kafka.consumer({
  groupId: process.env.KAFKA_CONSUMER_GROUP_ID || 'kafka-0',
})

const TOPIC_NAME = process.env.KAFKA_TOPIC || 'sync-test'

function sleep(ms){
    return new Promise(resolve=>{
        setTimeout(resolve,ms)
    })
}

const run = async () => {
	let i = 10
  const topic = TOPIC_NAME
  await consumer.connect()
  await consumer.subscribe({ topic })
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
			await sleep(9 * i--)
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)
    },
  })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})
