const mongoose = require('mongoose');

const main_speaker_schema = new mongoose.Schema({
    _id: String,
    main_speaker_name: String,
    details: String,
}, { collection: 'main_speakers_data' });

module.exports = mongoose.model('main_speaker', main_speaker_schema);