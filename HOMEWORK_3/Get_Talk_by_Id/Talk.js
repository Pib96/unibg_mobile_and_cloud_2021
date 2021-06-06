const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    _id: String,
    main_speaker: String,
    title: String,
    details: String,
    posted: String,
    url: String,
    num_views: String,
    duration: String,
}, { collection: 'tedx_data' });

module.exports = mongoose.model('talk', talk_schema);