using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace RedisMatchmaker
{
    public static class AddServer
    {
        [FunctionName("AddServer")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("AddServer HTTP trigger function processed a request.");
            log.LogInformation($"AddServer adds server(s) to the Servers Hash {Constants.ServersHash} and to the available servers Set {Constants.ServersAvailableSet}");

            /*
             * JSON sample to add a bunch of servers
             * { 
                 "command":"addServer",
                 "servers":
	                [
	                  {
		                "serverguid": "111111111111111111",
	                    "serveripandport": "192.168.1.1:50",
	                  },
	                  {
		                "serverguid": "222222222222222222",
	                    "serveripandport": "192.168.1.2:60",
	                  },
	                  {
		                "serverguid": "333333333333333333",
	                    "serveripandport": "192.168.1.3:70",
	                  },
	                  {
		                "serverguid": "444444444444444444",
	                    "serveripandport": "192.168.1.4:80",
	                  }
	                ]
                }
            */

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
  
            string command = data.command;

            if (command != null && !String.IsNullOrWhiteSpace((string)command))
            {
                if (command == Constants.CommandAddServer) // this is the request coming from the orchestrator
                {
                    // Connection to the Redis database
                    IDatabase cache = Helper.Connection.GetDatabase();

                    // Add server or servers
                    JArray servers = data.servers;
                    foreach (dynamic server in servers)
                    {
                        string serverGuid = server.serverguid;
                        string serverIPandPort = server.serveripandport;

                        // Logging
                        log.LogInformation($"Parameter received - Server GUID: {serverGuid}");
                        log.LogInformation($"Parameter received - Server IP and Port: {serverIPandPort}");
                                                
                        // Add new server to the Servers Hash
                        cache.HashSet(Constants.ServersHash, serverGuid, serverIPandPort, When.Always, CommandFlags.FireAndForget);
                        log.LogInformation($"Server added to the Servers Hash: {Constants.ServersHash}");

                        // Add new server to the set of servers available
                        cache.SetAdd(Constants.ServersAvailableSet, serverGuid, CommandFlags.FireAndForget);
                        log.LogInformation($"Server added to the set of available servers: {Constants.ServersAvailableSet}");
                    }
                }
            }

            return command != null
                ? (ActionResult)new OkObjectResult($"Command: {command}")
                : new BadRequestObjectResult("Please pass a command on the query string or in the request body.");
        }
    }

    public static class Helper
    {
        private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            // Replace these values with the values from your Azure Redis Cache instance.
            // For more information, see http://aka.ms/ConnectToTheAzureRedisCache
            string redisConnectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTION_STRING");
            
            return ConnectionMultiplexer.Connect(redisConnectionString);
        });

        public static ConnectionMultiplexer Connection
        {
            get
            {
                return lazyConnection.Value;

            }
        }

        public static HashEntry[] ToHashEntryArray(this object o)
        {
            PropertyInfo[] properties = o.GetType().GetProperties();

            return properties.Where(x => x.GetValue(o) != null).Select(property => new HashEntry(property.Name, property.GetValue(o).ToString())).ToArray();
        }
    }
}
