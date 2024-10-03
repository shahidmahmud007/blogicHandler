const { app, output } = require('@azure/functions');

// Define Cosmos DB output binding
const cosmosOutput = output.cosmosDB({
    databaseName: 'blogicbi',  // Replace with your actual DB name
    containerName: 'sim_events',  // Replace with your actual container name
    createIfNotExists: true,  // Set to false if container should already exist
    connection: 'CosmosDbConnectionSetting',  // Connection string setting in app settings
});

app.eventGrid('blogicHandler', {
    extraOutputs: [cosmosOutput],  // Add Cosmos DB as an output
    handler: async (eventGridEvent, context) => {
        try {
            // Log the event type and subject for context
            context.log('Event Type:', eventGridEvent.type);
            context.log('Event Subject:', eventGridEvent.source);

            // Log the entire payload for debugging
            context.log('Full Event Payload:', JSON.stringify(eventGridEvent, null, 2));

            // If the event contains data, log the data specifically
            if (eventGridEvent.data) {
                context.log('Event Data:', JSON.stringify(eventGridEvent.data, null, 2));

                const deviceId = Object.keys(eventGridEvent.data)[0];  // e.g., "22110"
                const deviceData = eventGridEvent.data[deviceId];  // The actual data for the device

                // Create a document to store in Cosmos DB NoSQL API
                const documentToInsert = {
                    id: eventGridEvent.id,  // Assign a unique ID
                    partitionKey: deviceId,  // Use deviceId as the partition key
                    deviceId: deviceId,
                    machineData: deviceData.machineData,
                    camStatistics_PerMinute: deviceData.camStatistics_PerMinute,
                    camStatistics_Percent: deviceData.camStatistics_Percent,
                    eventType: eventGridEvent.type,
                    eventTime: eventGridEvent.time,
                };

                // Log the document to be inserted for debugging
                context.log('Document to be inserted into Cosmos DB:', JSON.stringify(documentToInsert, null, 2));
                // Send the document to Cosmos DB via the output binding
                context.extraOutputs.set(cosmosOutput, documentToInsert);

                context.log('Document successfully set for Cosmos DB (NoSQL API)', JSON.stringify(documentToInsert));
            } else {
                context.log('No event data found.');
            }
        } catch (err) {
            // Catch and log any errors that occur during function execution
            context.log.error('Error processing Event Grid event:', err.message || err, JSON.stringify(err));
        }
    },
});
