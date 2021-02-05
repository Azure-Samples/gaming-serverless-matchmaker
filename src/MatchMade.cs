using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace RedisMatchmaker
{
    public static class MatchMade
    {
        [FunctionName("MatchMade")]
        public static async Task Run([EventHubTrigger(Constants.EventHubSessionReady, Connection = "EVENTHUB_CONNECTION_STRING")] EventData[] events,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    var sessionReady = JsonConvert.DeserializeObject<SessionReadyMessage>(messageBody);
                    
                    foreach (var player in sessionReady.GUIDs)
                    {
                        log.LogInformation($"Sending connection details to player: {player.ToString()}");

                        await starter.RaiseEventAsync(player, Constants.EventNameServerReady, sessionReady.ServerIPandPort); // Leveraging the fact that the orchestrator instance ID is the player GUID
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
