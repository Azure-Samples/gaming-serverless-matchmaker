using System;
using System.Collections.Generic;
using System.Text;

namespace RedisMatchmaker
{
    public static class Constants
    {
        // Redis types
        public const string ServersHash = "Servers";
        public const string ServersAvailableSet = "ServersAvailable";
        public const string PlayerHash = "Player";
        public const string PlayerTimestampsSortedSet = "PlayersTimeStartedMatchmaking";
        public const string SessionHash = "Session";
        public const string SessionPlayersSet = "SessionPlayers";
        public const string SessionsReadySet = "SessionsReady";
        public const string SessionsPerMatchmakingSet = "SessionsPerMatchmaking";
        public const string SessionTimestampsSortedSet = "SessionsCreationTime";

        public const string CapacityField = "Capacity";
        public const string MatchmakingSettingsField = "MatchmakingSettings";
        public const string TimedOutMessage = "Timed Out! Handle me graciously";

        // Event Hubs
        public const string EventHubAddPlayers = "ehrm-addusers";
        public const string EventHubSessionReady = "ehrm-sessionready";

        // Commands
        public const string CommandAddServer = "addServer";
        public const string CommandSessionReady = "sessionReady";
        public const string CommandMatchRequest = "matchRequest";

        // Events
        public const string EventNameServerReady = "serverReady";

        public const int CapacityMax = 4; // Capacity per multiplayer match
        public const string ReadySessionTimer = "*/15 * * * * *"; // Every 15 seconds
        public const int MatchmakingTimeoutInSeconds = 60; // Number of seconds before the matchmaking attempt times out
    }
    
    public class Player
    {
        public string GUID { get; set; }
        public string Name { get; set; }
        public string MatchmakingSettings { get; set; }
    }

    public class Session
    {
        public string GUID { get; set; }
        public int Capacity { get; set; }
        public string MatchmakingSettings { get; set; }
    }

    public class SessionReadyMessage
    {
        public string Command { get; set; }

        public string[] GUIDs { get; set; }
      
        public string ServerIPandPort { get; set; }
    }

}
