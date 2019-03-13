using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using StackExchange.Redis;

namespace RedisMatchmaker
{
    public static class MatchRequest
    {
        [FunctionName("MatchRequest")]
        public static async Task<object> RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context,
            ILogger log) // TODO: change object to some sort of actually defined match status obj
        {
            log.LogInformation($"MatchRequest sends to the player GUID {context.InstanceId} the IP:Port to connect to or the timed out message. It also removes the player from the database for future matchmakings");

            var timeoutSecs = context.GetInput<int>(); // Get timeout interval

            // Listen for events
            var timeoutCts = new CancellationTokenSource();

            var requestTimeoutAt = context.CurrentUtcDateTime.AddSeconds(timeoutSecs);
            
            var timeoutTask = context.CreateTimer(requestTimeoutAt, timeoutCts.Token);
            
            var serverReadyTask = context.WaitForExternalEvent<string>(Constants.EventNameServerReady);
            
            var nextEvent = await Task.WhenAny(timeoutTask, serverReadyTask);

            string serverInfo = String.Empty;
            if (nextEvent == serverReadyTask)
            {
                // Get server IP:Port from the event body
                serverInfo = serverReadyTask.Result;
            }
            else // Timeout happened
            {
                serverInfo = Constants.TimedOutMessage;
            }

            if (!timeoutTask.IsCompleted)
            {
                timeoutCts.Cancel();
            }

            // Remove the player from the database so s/he can do future matchmakings
            string playerGUID = context.InstanceId;
            await context.CallActivityAsync("FlushPlayer", playerGUID); // Durable Activity Function

            return serverInfo;
        }

        [FunctionName("FlushPlayer")]
        public static void FlushPlayer([ActivityTrigger] string playerGUID)
        {
            // Connection to the Redis database
            IDatabase cache = Helper.Connection.GetDatabase();

            // Remove player from the Players Hash
            string playerKey = string.Format("{0}:{1}", Constants.PlayerHash, playerGUID);
            cache.KeyDelete(playerKey);

            // Remove player from the Player Timestamps Sorted Set
            cache.SortedSetRemove(Constants.PlayerTimestampsSortedSet, playerGUID, CommandFlags.FireAndForget);
        }
    }
}