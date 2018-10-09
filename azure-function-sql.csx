#r "System.Configuration"
#r "System.Data"

using System.Net;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;

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
    string eventQuery;
    string latestEventQuery;
    string[] parameterNames;
    string[] parameterValues;

    switch (eventType) 
    {
        case "temperature":
            eventQuery = "INSERT INTO temperature_events (event_id, device_id, timestamp, temperature) " +
                    "VALUES (@event_id, @device_id, @time, @temp);";
            latestEventQuery = "INSERT INTO temperature_events_latest (device_id, timestamp, temperature) " +
                    "VALUES(@device_id, @time, @temp) ON DUPLICATE KEY UPDATE timestamp=@time, temp=@temp";
            float temperature = body.@event.data.temperature.value;
            parameterNames = new string[] {"@event_id", "@device_id", "@time", "@temp"};
            parameterValues = new string[] {eventId, deviceId, timestamp, temperature.ToString()};
            break;
            
        case "touch":
            eventQuery = "INSERT INTO touch_events (event_id, device_id, timestamp) " +
                    "VALUES (@event_id, @device_id, @time);";
            latestEventQuery = "INSERT INTO touch_events_latest (device_id, timestamp) " +
                    "VALUES(@device_id, @time) ON DUPLICATE KEY UPDATE timestamp=@time";
            parameterNames = new string[] {"@event_id", "@device_id", "@time"};
            parameterValues = new string[] {eventId, deviceId, timestamp};
            break;

        case "objectPresent":
            eventQuery = "INSERT INTO prox_events (event_id, device_id, timestamp, state) " +
                    "VALUES (@event_id, @device_id, @time, @state);";
            latestEventQuery = "INSERT INTO prox_events_latest (device_id, timestamp, state) " +
                    "VALUES(@device_id, @time, @state) ON DUPLICATE KEY UPDATE timestamp=@time, state=@state";
            string state = body.@event.data.objectPresent.state;
            parameterNames = new string[] {"@event_id", "@device_id", "@time", "@state"};
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
        using (SqlCommand cmd = new SqlCommand(eventQuery, conn))
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
        using (SqlCommand cmd = new SqlCommand(latestEventQuery, conn))
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