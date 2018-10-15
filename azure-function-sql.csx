#r "System.Configuration"
#r "System.Data"

using System.Net;
using System.Configuration;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Globalization;

public const string insertTemperatureEventQuery = @"
INSERT INTO temperature_events (event_id, device_id, timestamp, temperature, name, area, area2)
VALUES (@event_id, @device_id, @timestamp, @temperature, '', @area, @area2);
";

public const string insertTouchEventQuery = @"
INSERT INTO touch_events (event_id, device_id, timestamp, name, area, area2)
VALUES (@event_id, @device_id, @timestamp, @name, @area, @area2);
";

public const string insertProximityEventQuery = @"
INSERT INTO prox_events (event_id, device_id, timestamp, state, name, area, area2)
VALUES (@event_id, @device_id, @timestamp, @state, @name, @area, @area2);
";

public const string upsertLatestTemperatureQuery = @"
IF NOT EXISTS (SELECT * FROM temperature_events_latest WHERE device_id = @device_id)
    INSERT INTO temperature_events_latest (device_id, timestamp, temperature, name, area, area2)
    VALUES (@device_id, @timestamp, @temperature, @name, @area, @area2)
ELSE
    UPDATE temperature_events_latest
    SET timestamp = @timestamp, temperature = @temperature, name = @name, area = @area, area2 = @area2
    WHERE device_id = @device_id;
";

public const string upsertLatestTouchQuery = @"
IF NOT EXISTS (SELECT * FROM touch_events_latest WHERE device_id = @device_id)
    INSERT INTO touch_events_latest (device_id, timestamp, name, area, area2)
    VALUES (@device_id, @timestamp, @name, @area, @area2)
ELSE
    UPDATE touch_events_latest
    SET timestamp = @timestamp, name = @name, area = @area, area2 = @area2
    WHERE device_id = @device_id;
";

public const string upsertLatestProximityQuery = @"
IF NOT EXISTS (SELECT * FROM prox_events_latest WHERE device_id = @device_id)
    INSERT INTO prox_events_latest (device_id, timestamp, state, name, area, area2)
    VALUES (@device_id, @timestamp, @state, @name, @area, @area2)
ELSE
    UPDATE prox_events_latest
    SET timestamp = @timestamp, state = @state, name = @name, area = @area, area2 = @area2
    WHERE device_id = @device_id;
";


public static async Task<HttpResponseMessage> Run(HttpRequestMessage req, TraceWriter log)
{
    // ########## Section A: Get event data
    dynamic body = await req.Content.ReadAsAsync<object>();
    string eventId = body.@event.eventId;
    string targetName = body.@event.targetName;
    string deviceId = targetName.Substring(targetName.Length - 20);
    string eventType = body.@event.eventType;

    // Check for optional labels
    string name = body.labels.name;
    if (string.IsNullOrEmpty(name)) {
        name = "";
    }
    string area = body.labels.area;
    if (string.IsNullOrEmpty(area)) {
        area = "";
    }
    string area2 = body.labels.area2;
    if (string.IsNullOrEmpty(area2)) {
        area2 = "";
    }

    // Convert non-standard timestamp to C# DateTime for SQL database
    string dateTimePattern = "yyyy-MM-ddTHH:mm:ss.fffffff";
    var rawDateTimeString = body.@event.timestamp.ToString();
    var truncatedDateTimeString = rawDateTimeString.Substring(0, dateTimePattern.Length);
    DateTime dateTime = DateTime.ParseExact( truncatedDateTimeString,
                                    "yyyy-MM-dd'T'HH:mm:ss.fffffff",
                                    CultureInfo.InvariantCulture,
                                    DateTimeStyles.AssumeUniversal |
                                    DateTimeStyles.AdjustToUniversal);
    var dateTimeString = dateTime.ToString();


    // ########## Section B: Create the SQL query based on event type
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
            parameterNames = new string[] {"@event_id", "@device_id", "@timestamp", "@temperature", "@name", "@area", "@area2"};
            parameterValues = new string[] {eventId, deviceId, dateTimeString, temperature.ToString(), name, area, area2};
            break;
            
        case "touch":
            insertEventQuery = insertTouchEventQuery;
            upsertLatestEventQuery = upsertLatestTouchQuery;
            parameterNames = new string[] {"@event_id", "@device_id", "@timestamp", "@name", "@area", "@area2"};
            parameterValues = new string[] {eventId, deviceId, dateTimeString, name, area, area2};
            break;

        case "objectPresent":
            insertEventQuery = insertProximityEventQuery;
            upsertLatestEventQuery = upsertLatestProximityQuery;
            string state = body.@event.data.objectPresent.state;
            parameterNames = new string[] {"@event_id", "@device_id", "@timestamp", "@state", "@name", "@area", "@area2"};
            parameterValues = new string[] {eventId, deviceId, dateTimeString, state, name, area, area2};
            break;

        default:
            log.Error("Unsupported event type received, check Data Connector. EventType = " + eventType);
            return req.CreateResponse(HttpStatusCode.OK); // Return OK to prevent data connector from resending
    }

    // Log for debugging and information
    log.Info("Update: " + deviceId + ", " + eventType + ", " + dateTimeString);

    // ########## Section C: Insert into SQL database table(s)
    var str = Environment.GetEnvironmentVariable("SQLDB_CONNECTION");
    using (SqlConnection conn = new SqlConnection(str))
    {
        conn.Open();

        // Add to historical event table
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
                log.Error("SqlException when adding to historical event table" + ex.ToString());
                // but propagate other errors so that the Data Connector can retry later.
                return req.CreateResponse(HttpStatusCode.InternalServerError);
            }
        }

        // Add to "latest event" table
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
                log.Error("SqlException when adding to 'latest event' table" + ex.ToString());
                // but propagate other errors so that the Data Connector can retry later.
                return req.CreateResponse(HttpStatusCode.InternalServerError);
            }
        }
    }
    return req.CreateResponse(HttpStatusCode.OK);
}