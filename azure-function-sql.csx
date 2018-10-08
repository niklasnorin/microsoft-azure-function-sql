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
    string text;
    string[] parameterNames;
    string[] parameterValues;

    switch (eventType) 
    {
        case "temperature":
            text = "INSERT INTO Temperature_events (event_id, device_id, timestamp, temperature) " +
                    "VALUES (@event_id, @device_id, @time, @temp);";
            float temperature = body.@event.data.temperature.value;
            parameterNames = new string[] {"@event_id", "@device_id", "@time", "@temp"};
            parameterValues = new string[] {eventId, deviceId, timestamp, temperature.ToString()};
            break;
            
        case "touch":
            text = "INSERT INTO Touch_events (event_id, device_id, timestamp) " +
                    "VALUES (@event_id, @device_id, @time);";
            parameterNames = new string[] {"@event_id", "@device_id", "@time"};
            parameterValues = new string[] {eventId, deviceId, timestamp};
            break;

        case "objectPresent":
            text = "INSERT INTO Prox_events (event_id, device_id, timestamp, state) " +
                    "VALUES (@event_id, @device_id, @time, @state);";
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
        using (SqlCommand cmd = new SqlCommand(text, conn))
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