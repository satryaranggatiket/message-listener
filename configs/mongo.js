const mongoose = require('mongoose');

const url = 'mongodb://localhost:27017/refund-queue';

mongoose.connect(url);
mongoose.Promise = global.Promise;

const db = mongoose.connection;
db.on('error', console.error.bind(console, 'MongoDB connection error:'));

module.exports = mongoose;
