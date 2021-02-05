using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisMatchmaker
{
    public static class BatchAddPlayers
    {
        [FunctionName("BatchAddPlayers")]
        public static async Task Run([EventHubTrigger(Constants.EventHubAddPlayers, Connection = "EVENTHUB_CONNECTION_STRING")] EventData[] events,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            var exceptions = new List<Exception>();

            // Connection to the Redis database - using Event Hubs to batch requests and reduce number of connections
            IDatabase cache = Helper.Connection.GetDatabase();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    log.LogInformation($"BatchAddUsers Event Hub trigger function processed a message: {messageBody}");

                    // Processing logic.
                    Player tempPlayer = JsonConvert.DeserializeObject<Player>(messageBody);

                    string playerGUID = tempPlayer.GUID;
                    string playerMatchmaking = tempPlayer.MatchmakingSettings;

                    if (playerMatchmaking == null)
                    {
                        return;
                    }

                    // Start unique player GUID orchestrator for match request handling
                    var instanceId = await starter.StartNewAsync("MatchRequest", playerGUID, Constants.MatchmakingTimeoutInSeconds); // TODO: pass match request information
                    
                    // Flush existing key
                    string playerKey = string.Format("{0}:{1}", Constants.PlayerHash, playerGUID);
                    cache.KeyDelete(playerKey);
                    log.LogInformation($"Player key flushed: {playerKey}");

                    // Create player Hash
                    cache.HashSet(playerKey, tempPlayer.ToHashEntryArray(), CommandFlags.FireAndForget);
                    log.LogInformation($"Hash created including the player information: {playerKey}");

                    // Add timestamp to the PlayersTimeStartedMatchmaking Sorted Set
                    cache.SortedSetAdd(Constants.PlayerTimestampsSortedSet, playerGUID, new DateTimeOffset(DateTime.UtcNow).ToUnixTimeSeconds(), CommandFlags.FireAndForget);
                    log.LogInformation($"Player matchmaking started timestamp added: {Constants.PlayerTimestampsSortedSet}");

                    // Find a suitable game session for the user and create it if doesn't exist
                    // SDIFF MatchMakingSessions and SessionsReady to find a suitable game session that is not full
                    // RedisValue[] SetCombine(SetOperation operation, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None);
                    string keySessionsPerMatchmaking = string.Format("{0}:{1}", Constants.SessionsPerMatchmakingSet, playerMatchmaking);
                    string sessionKey = null;
                    string sessionGUID = null;
                    int capacity = -1;

                    var results = cache.SetCombine(SetOperation.Difference, keySessionsPerMatchmaking, Constants.SessionsReadySet);

                    if (results != null && results.Length > 0)
                    {
                        RedisValue entry = results.First(); // Get a session from the set, optimization could be to leverage the timestamp picking the oldest one

                        sessionGUID = entry.ToString();
                        sessionKey = string.Format("{0}:{1}", Constants.SessionHash, sessionGUID);

                        capacity = Int32.Parse(cache.HashGet(sessionKey, Constants.CapacityField));

                        log.LogInformation($"Session found for the player: {sessionGUID}; capacity: {capacity.ToString()}");
                    }
                    else // Create session
                    {
                        Guid randomGuid = Guid.NewGuid();
                        sessionGUID = randomGuid.ToString();

                        sessionKey = string.Format("{0}:{1}", Constants.SessionHash, sessionGUID);
                        cache.KeyDelete(sessionKey);

                        var tempSession = new Session
                        {
                            //Initialization
                            GUID = sessionGUID,
                            Capacity = Constants.CapacityMax,
                            MatchmakingSettings = playerMatchmaking
                        };

                        var outputJson = JsonConvert.SerializeObject(tempSession);

                        log.LogInformation($"Session not found for the player, a new one was created: {sessionKey}");

                        // Add new game session to the Sessions Hash
                        cache.HashSet(sessionKey, tempSession.ToHashEntryArray(), CommandFlags.FireAndForget);
                        log.LogInformation($"New session added to the Sessions Hash: {sessionKey}");

                        // Add the session to the matchmaking bucket
                        cache.SetAdd(keySessionsPerMatchmaking, sessionGUID, CommandFlags.FireAndForget);
                        log.LogInformation($"New session added to the matchmaking bucket: {keySessionsPerMatchmaking}");

                        capacity = Constants.CapacityMax;
                    }

                    // Add player to the game session
                    sessionKey = string.Format("{0}:{1}", Constants.SessionPlayersSet, sessionGUID);
                    if (!cache.SetContains(sessionKey, playerGUID)) // Is the player in this game session already
                    {
                        cache.SetAdd(sessionKey, playerGUID, CommandFlags.FireAndForget);
                    }
                    log.LogInformation($"Player added to the session: {sessionKey}");

                    // Update the capacity for the session
                    capacity--;
                    sessionKey = string.Format("{0}:{1}", Constants.SessionHash, sessionGUID);
                    cache.HashSet(sessionKey, Constants.CapacityField, capacity.ToString());

                    if (capacity == 0)
                    {
                        // Add the session to the SessionsReady Set
                        cache.SetAdd(Constants.SessionsReadySet, sessionGUID, CommandFlags.FireAndForget);
                        log.LogInformation($"Session added to the sessions ready set: {Constants.SessionsReadySet}");
                    }
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.
            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
