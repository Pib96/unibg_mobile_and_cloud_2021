const connect_to_db = require('./db');

// GET WATCH NEXT BY ID HANDLER

const talk = require('./Talk');

module.exports.get_watch_next_by_id = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    
    if(!body.id) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the watch next. Id is null.'
        })
    }
    
    connect_to_db().then(() => {
        console.log('=> get watch next');
        talk.findOne({_id: body.id})
            .select("watch_next_urls")
            .then(watch_next => {
                    callback(null, {
                        statusCode: 200,
                        body: JSON.stringify(watch_next)
                    })
                }
            )
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the watch next.'
                })
            );
    });
};