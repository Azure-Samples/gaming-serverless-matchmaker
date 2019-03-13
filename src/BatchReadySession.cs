using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using StackExchange.Redis;
using System;
using System.Threading;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using System.Text;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using System.IO;

namespace RedisMatchmaker
{
    public static class BatchReadySession
    {
        [FunctionName("BatchReadySession")]
        public static async Task RunTimerTrigger(
            [TimerTrigger(Constants.ReadySessionTimer)]TimerInfo info,
            ILogger log)
        {
            log.LogInformation($"{DateTime.Now} Executing BatchReadySession Timer Function...");

            IDatabase cache = Helper.Connection.GetDatabase();
            
            log.LogInformation($"{DateTime.Now} Got Redis Cache connection...");
            
            // Goes through the SessionsReady Set
            IEnumerable<RedisValue> setSessions = cache.SetScan(Constants.SessionsReadySet);

            // Event Hub
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(Environment.GetEnvironmentVariable("EVENTHUB_CONNECTION_STRING"))
            {
                EntityPath = Constants.EventHubSessionReady
            };

            var ehSessionReady = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());
            
            foreach (var sessionEntry in setSessions)
            {
                string sessionGUID = sessionEntry.ToString();

                // Find a server for this game session
                string serverIPandPort = null;

                // Get a random server GUID and automatically remove it from the set
                var results = cache.SetPop(Constants.ServersAvailableSet);

                if (!results.IsNullOrEmpty)
                {
                    // Use the server GUID to get the connection details from the Servers Hash
                    serverIPandPort = cache.HashGet(Constants.ServersHash, results.ToString()).ToString();
                    log.LogInformation($"Server found: {serverIPandPort}");

                    // Get list of players from the game session
                    string sessionPlayersKey = string.Format("{0}:{1}", Constants.SessionPlayersSet, sessionGUID);

                    string[] setPlayers = cache.SetScan(sessionPlayersKey).Select(redisValue => redisValue.ToString()).ToArray();

                    // Send server information to the list of players included in the game session
                    try
                    {
                        var tempSessionReadyMessage = new SessionReadyMessage
                        {
                            //Initialization
                            Command = Constants.CommandSessionReady,
                            GUIDs = setPlayers,
                            ServerIPandPort = serverIPandPort
                        };

                        var outputJson = JsonConvert.SerializeObject(tempSessionReadyMessage);

                        log.LogInformation($"{DateTime.Now} > Sending server ready message: {outputJson}");

                        // Message sent to the Event Hub
                        await ehSessionReady.SendAsync(new EventData(Encoding.UTF8.GetBytes(outputJson)));

                        // Removes the session from the database as it has already been handled
                        FlushSession(sessionGUID);
                    }
                    catch (Exception exception)
                    {
                        log.LogInformation($"{DateTime.Now} > Exception: { exception.Message}");
                    }
                }
                else
                {
                    // Need more servers, cue to scale out request...
                    log.LogInformation("No servers available, cue to scale out request here...");
                }
            }
        }

        [FunctionName("FlushSession")]
        public static void FlushSession([ActivityTrigger] string sessionGUID)
        {
            // Connection to the Redis database
            IDatabase cache = Helper.Connection.GetDatabase();

            // Removes the session from the SessionsReady Set
            cache.SetRemoveAsync(Constants.SessionsReadySet, sessionGUID);

            // Removes the session from the SessionsPerMatchmaking Set
            string sessionKey = string.Format("{0}:{1}", Constants.SessionHash, sessionGUID);
            string matchmakingBucket = cache.HashGet(sessionKey, Constants.MatchmakingSettingsField);
            sessionKey = string.Format("{0}:{1}", Constants.SessionsPerMatchmakingSet, matchmakingBucket);
            cache.SetRemoveAsync(sessionKey, sessionGUID);

            // Removes the session from the Session Hash
            sessionKey = string.Format("{0}:{1}", Constants.SessionHash, sessionGUID);
            cache.KeyDelete(sessionKey);

            // Removes the players set from the session
            string sessionPlayersKey = string.Format("{0}:{1}", Constants.SessionPlayersSet, sessionGUID);
            cache.KeyDelete(sessionPlayersKey);
        }
    }
}