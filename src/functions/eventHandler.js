const { app } = require('@azure/functions');
const { Client } = require('pg');  // PostgreSQL client

// PostgreSQL connection settings
const pgClient = new Client({
    user: process.env.PG_USER,            // PostgreSQL username from environment variables
    host: process.env.PG_HOST,            // PostgreSQL host from environment variables
    database: process.env.PG_DATABASE,    // PostgreSQL database name from environment variables
    password: process.env.PG_PASSWORD,    // PostgreSQL password from environment variables
    port: process.env.PG_PORT || 5432,    // PostgreSQL port (default is 5432)
});

// Connect to PostgreSQL
pgClient.connect();

app.eventGrid('blogicHandlerTest', {
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

                const deviceData = eventGridEvent.data;  // The actual data for the device
                const deviceId = deviceData.machineData.DeviceID;  // e.g., "22110"

                // Extract date and time (hh:mm) from the timestamp
                const timestamp = new Date(deviceData.machineData.TimeStamp);
                const date = timestamp.toISOString().split('T')[0];  // YYYY-MM-DD format
                const hour = timestamp.toTimeString().split(':').slice(0, 2).join(':');  // Extracts hh:mm

                 // Create the SQL insert query
                 const insertQuery = `
                 INSERT INTO device_data (
                     id, deviceId, timestamp, 
                     date, hour, state_current, cur_mach_speed, 
                     mach_speed, prod_processed_count, cam_statistics_per_minute_minutes, 
                     cam_statistics_first, cam_statistics_last, cam_statistics_total, 
                     cam_statistics_empty, cam_statistics_ok, cam_statistics_returns, 
                     cam_statistics_waste, cam_statistics_double, cam_statistics_bellyback, 
                     cam_statistics_head, cam_statistics_misc, cam_statistics_total_fpm, 
                     cam_statistics_dbg_total, cam_statistics_percent_minutes, 
                     cam_statistics_percent_total, cam_statistics_percent_empty, 
                     cam_statistics_percent_ok, cam_statistics_percent_returns, 
                     cam_statistics_percent_waste, cam_statistics_percent_double, 
                     cam_statistics_percent_bellyback, cam_statistics_percent_head, 
                     cam_statistics_percent_misc, event_time
                 ) VALUES (
                     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
                     $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 
                     $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, 
                     $31, $32, $33, $34
                 )
             `;

                // Prepare the data to insert
                const values = [
                    eventGridEvent.id,
                    deviceId,
                    deviceData.machineData.TimeStamp,
                    date,  // Extracted date
                    hour,  // Extracted hh:mm time
                    deviceData.machineData.StateCurrent,
                    deviceData.machineData.CurMachSpeed,
                    deviceData.machineData.MachSpeed,
                    deviceData.machineData.ProdProcessedCount,
                    deviceData.camStatistics_PerMinute.minutes,
                    deviceData.camStatistics_PerMinute.first,
                    deviceData.camStatistics_PerMinute.last,
                    deviceData.camStatistics_PerMinute.total,
                    deviceData.camStatistics_PerMinute.empty,
                    deviceData.camStatistics_PerMinute.ok,
                    deviceData.camStatistics_PerMinute.returns,
                    deviceData.camStatistics_PerMinute.waste,
                    deviceData.camStatistics_PerMinute.double,
                    deviceData.camStatistics_PerMinute.bellyback,
                    deviceData.camStatistics_PerMinute.head,
                    deviceData.camStatistics_PerMinute.misc,
                    deviceData.camStatistics_PerMinute.total_fpm,
                    deviceData.camStatistics_PerMinute.dbg_total,
                    deviceData.camStatistics_Percent.minutes,
                    deviceData.camStatistics_Percent.total,
                    deviceData.camStatistics_Percent.empty,
                    deviceData.camStatistics_Percent.ok,
                    deviceData.camStatistics_Percent.returns,
                    deviceData.camStatistics_Percent.waste,
                    deviceData.camStatistics_Percent.double,
                    deviceData.camStatistics_Percent.bellyback,
                    deviceData.camStatistics_Percent.head,
                    deviceData.camStatistics_Percent.misc,
                    eventGridEvent.time
                ];

                // Log the query for debugging
                context.log('Executing query:', insertQuery);
                context.log('With values:', values);

                // Execute the query
                await pgClient.query(insertQuery, values);

                context.log('Data successfully inserted into PostgreSQL');
            } else {
                context.log('No event data found.');
            }
        } catch (err) {
            // Catch and log any errors that occur during function execution
            context.log.error('Error processing Event Grid event:', err.message || err, JSON.stringify(err));
        }
    },
});
