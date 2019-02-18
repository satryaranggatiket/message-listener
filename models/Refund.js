const mongoose = require('mongoose');
const Schema = mongoose.Schema;

let RefundSchema = new Schema({
    orderId: {type: String, required: true },
    name: {type: String, required: true, max: 100},
    amount: {type: Number, required: true },
    reason: {type: String, required: true, max: 100},
    isPaid: {type: Boolean, default: false },
    paidToAccountNumber: {type: String, required: false, max: 100},
    paidToAccountBank: {type: String, required: false, max: 100},
    paidToAccountOwner: {type: String, required: false, max: 100},
    createdAt : { type : Date, default: Date.now },
    updatedAt : { type : Date, default: Date.now }
});

module.exports = mongoose.model('refund', RefundSchema);