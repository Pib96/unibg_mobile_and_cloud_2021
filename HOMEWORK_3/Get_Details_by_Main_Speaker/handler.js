const connect_to_db = require('./db');

// GET DETAILS BY MAIN SPEAKER HANDLER

const main_speaker = require('./MainSpeaker');

module.exports.get_details_by_main_speaker = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    
    if(!body.main_speaker_name) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the details. Main_Speaker_Name is null.'
        })
    }
    
    connect_to_db().then(() => {
        console.log('=> get_all talks');
        main_speaker.findOne({main_speaker_name: body.main_speaker_name})

            .then(details => {
                    callback(null, {
                        statusCode: 200,
                        body: JSON.stringify(details)
                    })
                }
            )
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the details.'
                })
            );
    });
};