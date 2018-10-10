#r "System.Configuration"
#r "System.Data"

using System.Net;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;

public const string insertTemperatureEventQuery = @"
INSERT INTO temperature_events (event_id, device_id, timestamp, temperature)
VALUES (@event_id, @device_id, @timestamp, @temperature);
";

public const string insertTouchEventQuery = @"
INSERT INTO touch_events (event_id, device_id, timestamp)
VALUES (@event_id, @device_id, @timestamp);
";

public const string insertProximityEventQuery = @"
INSERT INTO prox_events (event_id, device_id, timestamp, state)
VALUES (@event_id, @device_id, @timestamp, @state);
";

public const string upsertLatestTemperatureQuery = @"
IF NOT EXISTS (SELECT * FROM temperature_events_latest WHERE device_id = @device_id)
    INSERT INTO temperature_events_latest (device_id, timestamp, temperature)
    VALUES (@device_id, @timestamp, @temperature)
ELSE
    UPDATE temperature_events_latest
    SET timestamp = @timestamp, temperature = @temperature
    WHERE device_id = @device_id;
";

public const string upsertLatestTouchQuery = @"
IF NOT EXISTS (SELECT * FROM touch_events_latest WHERE device_id = @device_id)
    INSERT INTO touch_events_latest (device_id, timestamp)
    VALUES (@device_id, @timestamp)
ELSE
    UPDATE touch_events_latest
    SET timestamp = @timestamp
    WHERE device_id = @device_id;
";

public const string upsertLatestProximityQuery = @"
IF NOT EXISTS (SELECT * FROM prox_events_latest WHERE device_id = @device_id)
    INSERT INTO prox_events_latest (device_id, timestamp, state)
    VALUES (@device_id, @timestamp, @state)
ELSE
    UPDATE prox_events_latest
    SET timestamp = @timestamp, state = @state
    WHERE device_id = @device_id;
";

public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
    // Section A: Get event data
    dynamic body = await req.Content.ReadAsAsync<object>();
    string eventId = body.@event.eventId;
    string targetName = body.@event.targetName;
    string deviceId = targetName.Substring(targetName.Length - 20);
    string timestamp = body.@event.timestamp;
    string eventType = body.@event.eventType;


    // Section B: Create the SQL query based on event type
    string insertEventQuery;
    string upsertLatestEventQuery;
    string[] parameterNames;
    string[] parameterValues;

    switch (eventType) 
    {
        case "temperature":
            insertEventQuery = insertTemperatureEventQuery;
            upsertLatestEventQuery = upsertLatestTemperatureQuery;
            float temperature = body.@event.data.temperature.value;
            parameterNames = new string[] {"@event_id", "@device_id", "@timestamp", "@temperature"};
            parameterValues = new string[] {eventId, deviceId, timestamp, temperature.ToString()};
            break;
            
        case "touch":
            insertEventQuery = insertTouchEventQuery;
            upsertLatestEventQuery = upsertLatestTouchQuery;
            parameterNames = new string[] {"@event_id", "@device_id", "@timestamp"};
            parameterValues = new string[] {eventId, deviceId, timestamp};
            break;

        case "objectPresent":
            insertEventQuery = insertProximityEventQuery;
            upsertLatestEventQuery = upsertLatestProximityQuery;
            string state = body.@event.data.objectPresent.state;
            parameterNames = new string[] {"@event_id", "@device_id", "@timestamp", "@state"};
            parameterValues = new string[] {eventId, deviceId, timestamp, state};
            break;

        default:
            log.Error("Unsupported event type received, check Data Connector. EventType = " + eventType);
            return req.CreateResponse(HttpStatusCode.OK); // Return OK to prevent data connector from resending
    }

    // Log for debugging and information
    log.Info("Update: " + deviceId + ", " + eventType + ", " + timestamp);

    // Section C: Connect to SQL database and 
    var str = Environment.GetEnvironmentVariable("SQLDB_CONNECTION");
    using (SqlConnection conn = new SqlConnection(str))
    {
        conn.Open();
        using (SqlCommand cmd = new SqlCommand(insertEventQuery, conn))
        {
            // Add the parameters
            for (int i = 0; i < parameterValues.Length; i++)
            {
                cmd.Parameters.AddWithValue(parameterNames[i], parameterValues[i]);
            }

            try
            {
                // Execute query
                var rows = await cmd.ExecuteNonQueryAsync();
            }
            catch (SqlException ex) when (ex.Number == 2627)
            {
                // Ignore duplicate events...
            }
            catch (SqlException ex)
            {
                // but propagate other errors so that the Data Connector can retry later.
                return req.CreateResponse(HttpStatusCode.InternalServerError);
            }
        }
        using (SqlCommand cmd = new SqlCommand(upsertLatestEventQuery, conn))
        {
            // Add the parameters
            for (int i = 0; i < parameterValues.Length; i++)
            {
                cmd.Parameters.AddWithValue(parameterNames[i], parameterValues[i]);
            }

            try
            {
                // Execute query
                var rows = await cmd.ExecuteNonQueryAsync();
            }
            catch (SqlException ex) when (ex.Number == 2627)
            {
                // Ignore duplicate events...
            }
            catch (SqlException ex)
            {
                // but propagate other errors so that the Data Connector can retry later.
                return req.CreateResponse(HttpStatusCode.InternalServerError);
            }
        }
    }
    return req.CreateResponse(HttpStatusCode.OK);
}