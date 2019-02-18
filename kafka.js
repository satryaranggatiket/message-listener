var Kafka = require('no-kafka')
const mongo = require('./configs/mongo')
const Refund = require('./models/Refund')

var consumer = new Kafka.SimpleConsumer();
 
var dataHandler = function (messageSet, topic, partition) {
    messageSet.forEach(function (m) {
        console.log(topic, partition, m.offset, m.message.value.toString('utf8'));
        const decodedMessage = JSON.parse(m.message.value.toString('utf8'));
        const refundObject = decodedMessage.param;
        
        Refund.findByIdAndUpdate(decodedMessage.id, {$set: refundObject}, function (error, data) {
            if(error) console.log("- Error saving refund. Order Id : ",error)
            console.log("- Success Updating Refund. Refund Id : ",decodedMessage.id)
        });
    });
};
 
return consumer.init().then(function () {
    return consumer.subscribe('refund.queue', [0, 1], dataHandler);
});