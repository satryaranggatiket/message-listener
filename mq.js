const amqp = require('amqplib')
const mqConfig = require('./configs/amqp')
const mongo = require('./configs/mongo')
const Refund = require('./models/Refund')

amqp.connect(mqConfig.connectionString)
  .then(conn=> {
    return conn.createChannel().then(ch => {
      const ok = ch.assertQueue(mqConfig.queueName, { durable: false })
      ok.then(() => {
        return ch.consume(
            mqConfig.queueName, 
            msg => {
                console.log('- Message Received', msg.content.toString())
                const refundObject = JSON.parse(msg.content.toString())
                let refund = new Refund(refundObject)

                refund.save(function (error) {
                    if(error) console.log("- Error saving refund. Order Id : ",error)
                    console.log("- Success Saving Refund. Order Id : ",refundObject.orderId)
                })
            }, 
            { noAck: true })
      })
      .then(() => {
        console.log('* Waiting for messages. Ctrl+C to exit')
      })
    })
  }).catch(console.warn)