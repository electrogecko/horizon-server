using DotNetty.Common.Internal.Logging;
using Microsoft.Extensions.Logging;
using RT.Common;
using RT.Models;
using Server.Dme.PluginArgs;
using Server.Plugins.Interface;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Server.Dme.Models
{
    public class World : IDisposable
    {
        static readonly IInternalLogger Logger = InternalLoggerFactory.GetInstance<World>();

        public const int MAX_WORLDS = 256;
        public const int MAX_CLIENTS_PER_WORLD = 10;

         #region Id Management

        private static ConcurrentDictionary<int, World> _idToWorld = new ConcurrentDictionary<int, World>();
        private ConcurrentDictionary<int, bool> _pIdIsUsed = new ConcurrentDictionary<int, bool>();
        private static int _idCounter = 0;
        private static object _lock = new object();

        private void RegisterWorld()
        {
            int totalIdsCounted = 0;
            while (totalIdsCounted < MAX_WORLDS && _idToWorld.ContainsKey(_idCounter))
            {
                _idCounter = (_idCounter + 1) % MAX_WORLDS;
                ++totalIdsCounted;
            }

            // 
            if (totalIdsCounted == MAX_WORLDS)
                throw new InvalidOperationException("Max worlds reached!");

            // 
            WorldId = _idCounter++;
            _idToWorld.TryAdd(WorldId, this);
            Logger.Info($"Registered world with id {WorldId}");
        }

        private void FreeWorld()
        {
            _idToWorld.TryRemove(WorldId, out _);
            Logger.Info($"Unregistered world with id {WorldId}");
        }

        private bool TryRegisterNewClientIndex(out int index)
        {
            lock (_lock)
            {
                for (index = 0; index < _pIdIsUsed.Count; ++index)
                {
                    if (_pIdIsUsed.TryGetValue(index, out var isUsed) && !isUsed)
                    {
                        _pIdIsUsed[index] = true;
                        return true;
                    }
                }
            }

            return false;
        }

        public void UnregisterClientIndex(int index)
        {
            _pIdIsUsed[index] = false;
        }


        // PROBE ONLY: a joiner polls the host with 02-13 and waits for the host to answer
        // 02-12 before it will finish joining. Captured from a working staging join:
        //
        //   joiner -> 02-13 (x10, ~150ms apart)
        //   host   -> 00-0F, 00-04, 02-12-00-00-00-00
        //   joiner -> 02-13 (x4), host -> 00-03
        //   joiner -> 02-11            (joined)
        //
        // An in-game host never sends 02-12, because that responder lives in the staging
        // loop. The joiner then aborts after three polls (dropping RECV_BROADCAST and
        // sending 00-12-01 twice) and disconnects. Answer on the host's behalf and flush
        // immediately -- the joiner's whole attempt lasts ~10ms, so a queued reply that
        // waits for the next aggregation window would arrive after it has already given up.
        private static readonly byte[] HOTJOIN_POLL_ANSWER = new byte[] { 0x02, 0x12, 0x00, 0x00, 0x00, 0x00 };

        private void TryAnswerHotjoinPoll(ClientObject source, short targetDmeId, byte[] payload)
        {
            // Only a non-host polling the host, and only while the host is actually in-game
            // (_hotjoinInGame is set when we clamp an in-game roster for this world).
            if (!_hotjoinInGame || source == null || source.DmeId == 0 || targetDmeId != 0) return;
            if (payload == null || payload.Length < 2 || payload[0] != 0x02 || payload[1] != 0x13) return;

            var reply = new RT_MSG_CLIENT_APP_SINGLE()
            {
                TargetOrSource = 0,
                Payload = HOTJOIN_POLL_ANSWER
            };

            var tcp = source.Tcp;
            if (tcp == null || !tcp.Active) return;

            _ = tcp.WriteAndFlushAsync(reply);
            Logger.Info($"HOTJOIN PROBE: answered 02-13 poll from client {source.DmeId} with 02-12 for world {WorldId}");
        }

        #endregion

        public int WorldId { get; protected set; } = -1;

        public int ApplicationId { get; protected set; } = 0;

        public int MaxPlayers { get; protected set; } = 0;

        public bool SelfDestructFlag { get; protected set; } = false;

        public bool ForceDestruct { get; protected set; } = false;

        public bool Destroy => ((WorldTimer.Elapsed.TotalSeconds > Program.GetAppSettingsOrDefault(ApplicationId).GameTimeoutSeconds) || SelfDestructFlag) && Clients.Count == 0;
        public bool Destroyed { get; protected set; } = false;

        public Stopwatch WorldTimer { get; protected set; } = Stopwatch.StartNew();

        public ConcurrentDictionary<int, ClientObject> Clients = new ConcurrentDictionary<int, ClientObject>();

        public MediusManager Manager { get; } = null;
        
        public World(MediusManager manager, int appId, int maxPlayers)
        {
            Manager = manager;
            ApplicationId = appId;

            // populate collection of used player ids
            for (int i = 0; i < MAX_CLIENTS_PER_WORLD; ++i)
                _pIdIsUsed.TryAdd(i, false);

            RegisterWorld();
            this.MaxPlayers = maxPlayers;
        }

        public void Dispose()
        {
            FreeWorld();
            Destroyed = true;
        }

        public async Task Stop()
        {
            // Stop all clients
            await Task.WhenAll(Clients.Select(x => x.Value.Stop()));

            Dispose();
        }

        public Task HandleIncomingMessages()
        {
            List<Task> tasks = new List<Task>();

            // Process clients
            for (int i = 0; i < MAX_CLIENTS_PER_WORLD; ++i)
            {
                if (Clients.TryGetValue(i, out var client))
                {
                    tasks.Add(client.HandleIncomingMessages());
                }
            }

            return Task.WhenAll(tasks);
        }

        public async Task HandleOutgoingMessages()
        {
            // Process clients
            for (int i = 0; i < MAX_CLIENTS_PER_WORLD; ++i)
            {
                if (Clients.TryGetValue(i, out var client))
                {
                    if (client.Destroy || ForceDestruct || Destroyed)
                    {
                        await OnPlayerLeft(client);
                        Manager.RemoveClient(client);
                        _ = client.Stop();
                        Clients.TryRemove(i, out _);
                    }
                    else if (client.Timedout)
                    {
                        client.ForceDisconnect();
                    }
                    else if (client.IsAggTime)
                    {
                        client.HandleOutgoingMessages();
                    }
                }
            }

            // Remove
            if (Destroy)
            {
                if (!Destroyed)
                {
                    Logger.Info($"{this} destroyed.");
                    await Stop();
                }

                Manager.RemoveWorld(this);
            }
        }

        #region Hotjoin probe


        // PROBE ONLY: caches the staging name a client announces in its 00-0F broadcast,
        // keyed by DmeId, so we can fill it into a roster slot we inject below.
        private readonly ConcurrentDictionary<int, string> _hotjoinNames = new ConcurrentDictionary<int, string>();

        // Same names, keyed by session and shared across worlds. The per-world cache is
        // empty for the first injection into a NEW world, which is why a remade game or a
        // rejoin showed up on the joiner's screen as "Hotjoin1" while the very next
        // injection into the same world carried "Gtr4". Sessions survive a world change,
        // so this covers the gap; DmeIds do not, so this must NOT be keyed by DmeId.
        private static readonly ConcurrentDictionary<string, string> _hotjoinNamesBySession =
            new ConcurrentDictionary<string, string>();

        // Verified wire offsets inside the tNW_GameSettings blob (base = index of
        // "tNW_GameSetting" + 16). Only the first 0x100 bytes are confirmed to match
        // the in-memory GameSettings layout, so we touch nothing past PlayerStates.
        private const int GS_NAMES = 0x00;   // char[8][16]
        private const int GS_SKINS = 0xC0;   // short[8]
        private const int GS_TEAMS = 0xD0;   // short[8]
        private const int GS_CLIENTS = 0xE0; // short[8]
        private const int GS_STATES = 0xF0;  // short[8]

        // Set once we have seen this world's host report an in-game roster.
        private volatile bool _hotjoinInGame = false;

        private const int GS_PLAYER_STATE_READY = 6;
        private const int GS_PLAYER_STATE_GAME_START = 7;

        private static readonly byte[] GS_TYPENAME = Encoding.ASCII.GetBytes("tNW_GameSetting");

        private static int IndexOfPattern(byte[] haystack, byte[] needle)
        {
            for (int i = 0; i + needle.Length <= haystack.Length; ++i)
            {
                int j = 0;
                while (j < needle.Length && haystack[i + j] == needle[j]) ++j;
                if (j == needle.Length) return i;
            }
            return -1;
        }

        /// <summary>
        /// PROBE ONLY: rewrites a tNW_GameSettings payload on its way from the in-game host
        /// to a late joiner, inserting a roster slot for that joiner. Tests whether the
        /// joiner's immediate bail is caused by finding itself absent from the roster.
        /// Returns the original array unchanged if this is not a settings payload.
        /// </summary>
        private int _injectBailLogged = 0;

        // The host sends the FULL tNW_GameSettings snapshot (payload[3] == 0, ~470 bytes,
        // carrying the "tNW_GameSetting" type name) exactly ONCE per world, when it first
        // has someone to send it to. After that it sends only deltas (payload[3] == 1,
        // ~20-38 bytes) which contain no roster at all. A rejoining client therefore
        // arrives after the only full snapshot has already gone by, receives nothing it
        // can build a roster from, never establishes gs, and dies on the host's verdict 4.
        // Measured directly: `injected roster slot 1` once for the world, then
        // `inject bail (no tNW_GameSetting)` for every reconnect after it.
        //
        // The server has already seen that snapshot, so keep it and serve it again to any
        // client that has not had one this connection. Upgrading the first delta the host
        // aims at them is the natural moment: the host is already talking to them, the
        // cadence is right, and no new message has to be invented.
        private byte[] _lastFullSettings = null;
        private readonly HashSet<int> _fullSettingsSentTo = new HashSet<int>();

        // The same one-shot pattern applies one layer down, to the tNW_Player
        // registration (message 00-04). Measured across a session with two rejoins:
        //     joiner -> host   00-04 broadcast on EVERY connect  (18:42:44/43:46/44:28)
        //     host   -> joiner 00-04 sent ONCE                   (18:42:33, first join)
        // so a rejoiner never learns the host's identity: its nwp[0] keeps cid=-1 and
        // plist[0] stays NULL, which is exactly "I spawned in but we cannot see each
        // other". Cache the host's registration and replay it alongside the settings.
        private byte[] _lastPlayerReg = null;
        private int _substitutions = 0;

        private byte[] InjectBail(string why, byte[] payload)
        {
            // Rate-limited: the host sends settings continuously, so an unconditional log
            // would drown everything else.
            if (_injectBailLogged < 12)
            {
                _injectBailLogged++;
                Logger.Info($"HOTJOIN PROBE: inject bail ({why}) world {WorldId}");
            }
            return payload;
        }

        private byte[] TryInjectHotjoinSlot(byte[] payload, int slot)
        {
            if (payload == null || slot <= 0 || slot >= 8) return payload;
            if (payload.Length < 3 || payload[0] != 0x00 || payload[1] != 0x16 || payload[2] != 0x05) return payload;

            // Everything past this point IS a settings blob aimed at a real joiner slot,
            // so any bail below is worth knowing about. Measured on a rejoin: the host
            // keeps sending 00-16-05 and the server keeps relaying it, but the injection
            // ran exactly once for the world (at the first join) and never again -- so the
            // joiner receives a roster with no slot for itself, never establishes gs, and
            // the join dies on the host's verdict 4.
            int at = IndexOfPattern(payload, GS_TYPENAME);
            if (at < 0)
            {
                // A delta. If this client has never had a full snapshot, substitute the
                // cached one and inject into that instead.
                byte[] full = _lastFullSettings;
                if (full != null && !_fullSettingsSentTo.Contains(slot))
                {
                    Logger.Info($"HOTJOIN PROBE: substituting cached full settings ({full.Length} bytes) for slot {slot}, world {WorldId}");
                    _substitutions++;
                    return TryInjectHotjoinSlot(full, slot);
                }
                return InjectBail("no tNW_GameSetting", payload);
            }

            // A real snapshot -- keep it for the next joiner, and note this one has had it.
            _lastFullSettings = (byte[])payload.Clone();
            _fullSettingsSentTo.Add(slot);

            int gs = at + 16;
            if (gs + GS_STATES + (slot * 2) + 2 > payload.Length)
                return InjectBail($"short payload {payload.Length} slot {slot}", payload);

            // Only rewrite when the host is actually in-game. An in-game host reports
            // PlayerStates[0] == 8 (one past GS_PLAYER_STATE_GAME_START). Without this
            // guard the probe would also rewrite ordinary staging joins and corrupt them.
            int hostState = payload[gs + GS_STATES] | (payload[gs + GS_STATES + 1] << 8);
            if (hostState < 7) return InjectBail($"hostState {hostState} < 7, slot {slot}", payload);

            _hotjoinInGame = true;

            var patched = (byte[])payload.Clone();

            _hotjoinNames.TryGetValue(slot, out var name);
            if (string.IsNullOrEmpty(name)
                && Clients.TryGetValue(slot, out var slotClient)
                && !string.IsNullOrEmpty(slotClient?.SessionKey))
            {
                _hotjoinNamesBySession.TryGetValue(slotClient.SessionKey, out name);
            }
            if (string.IsNullOrEmpty(name)) name = "Hotjoin" + slot;
            var nameBytes = Encoding.ASCII.GetBytes(name);
            for (int i = 0; i < 16; ++i)
                patched[gs + GS_NAMES + (slot * 16) + i] = i < nameBytes.Length ? nameBytes[i] : (byte)0;

            void WriteShort(int arrayOff, short value)
            {
                patched[gs + arrayOff + (slot * 2)] = (byte)(value & 0xFF);
                patched[gs + arrayOff + (slot * 2) + 1] = (byte)((value >> 8) & 0xFF);
            }

            WriteShort(GS_SKINS, 0);
            WriteShort(GS_TEAMS, 0);
            WriteShort(GS_CLIENTS, (short)slot); // PlayerClients[slot] = the joiner's client id
            // Keep the joiner UNREADY. Presenting it as READY was tried and is strictly
            // worse: the staging screen then faults at 0x199c6c within two frames instead
            // of surviving staging and reaching the level load.
            WriteShort(GS_STATES, 0);            // GS_PLAYER_STATE_UNREADY

            // PlayerCount is NOT on the wire. The settings blob is 0x190 bytes and its
            // layout diverges from libuya's in-memory GameSettings after PlayerTypes --
            // decoded from a live capture (gs = index of "tNW_GameSetting" + 16):
            //
            //   0x000 names[8][16]   0x080 clantags   0x0C0 skins   0x0D0 teams
            //   0x0E0 clients        0x0F0 states     0x100 types
            //   0x110 game option/rule bytes (0x20)
            //   0x130 PlayerAccountIds[8]   (observed [5, -1 x7]; 5 is the host's account)
            //   0x150 PlayerRanks[8]        (observed [1350.0, -1.0 x7])
            //   0x170 PlayerRankDeviations[8] (observed [350.0, -1.0 x7])
            //   0x190 end
            //
            // In memory libuya has Ranks 0x110 / Deviations 0x130 / AccountIds 0x150 and
            // then GameStartTime, GameLoadStartTime, PlayerCount at 0x170/0x174/0x178 --
            // three fields the wire format simply does not carry. So the joiner computes
            // PlayerCount itself, and a guarded write here can never be right. The guard
            // did its job (wire read back as 0xBF800000 = -1.0f, a RankDeviation) and
            // nothing was written; the attempt is removed rather than left logging.

            // Clamp the in-game host's undocumented state 8 down to GAME_START (7), NOT
            // to READY. The joiner's staging handler decides whether to start loading with
            //     lh v0,240(a1) ; slti v0,v0,7 ; beqz v0 -> start-game path (jal 0x6c1930)
            // so state >= 7 IS the load trigger. Clamping to READY (6) held the joiner in
            // staging forever at "0/8" -- it was our own clamp suppressing the transition
            // we want. 7 satisfies the trigger while staying inside the documented enum.
            for (int i = 0; i < 8; ++i)
            {
                int off = gs + GS_STATES + (i * 2);
                int st = payload[off] | (payload[off + 1] << 8);
                if (st > GS_PLAYER_STATE_GAME_START)
                {
                    patched[off] = (byte)GS_PLAYER_STATE_GAME_START;
                    patched[off + 1] = 0;
                }
            }

            Logger.Info($"HOTJOIN PROBE: injected roster slot {slot} (\"{name}\") and clamped in-game states -> GAME_START for world {WorldId}");
            return patched;
        }

        #endregion

        #region Send

        public void BroadcastTcp(ClientObject source, byte[] Payload)
        {
            var msg = new RT_MSG_CLIENT_APP_SINGLE()
            {
                TargetOrSource = (short)source.DmeId,
                Payload = Payload
            };

            foreach (var client in Clients)
            {
                if (client.Value == source || !client.Value.IsAuthenticated || !client.Value.IsConnected || !client.Value.HasRecvFlag(RT_RECV_FLAG.RECV_BROADCAST))
                    continue;

                client.Value.EnqueueTcp(msg);
            }

            // PROBE ONLY: remember the staging name a joining client announces (00-0F),
            // so an injected roster slot can carry the real name.
            if (Payload != null && Payload.Length >= 40 && source.DmeId != 0
                && Payload[0] == 0x00 && Payload[1] == 0x0F)
            {
                int end = 24;
                while (end < Payload.Length && end < 24 + 16 && Payload[end] != 0) ++end;
                if (end > 24)
                {
                    var announced = Encoding.ASCII.GetString(Payload, 24, end - 24);
                    _hotjoinNames[source.DmeId] = announced;
                    if (!string.IsNullOrEmpty(source.SessionKey))
                        _hotjoinNamesBySession[source.SessionKey] = announced;
                }
            }

        }

        public void BroadcastUdp(ClientObject source, byte[] Payload)
        {
            var msg = new RT_MSG_CLIENT_APP_SINGLE()
            {
                TargetOrSource = (short)source.DmeId,
                Payload = Payload
            };

            foreach (var client in Clients)
            {
                if (client.Value == source || !client.Value.IsAuthenticated || !client.Value.IsConnected || !client.Value.HasRecvFlag(RT_RECV_FLAG.RECV_BROADCAST))
                //if (!client.Value.IsAuthenticated || !client.Value.IsConnected || !client.Value.HasRecvFlag(RT_RECV_FLAG.RECV_BROADCAST))
                    continue;

                client.Value.EnqueueUdp(msg);
            }
        }

        public void SendTcpAppList(ClientObject source, IEnumerable<int> targetDmeIds, byte[] Payload)
        {
            foreach (var targetId in targetDmeIds)
            {
                if (Clients.TryGetValue(targetId, out var client))
                {
                    if (client == null || !client.IsAuthenticated || !client.IsConnected || !client.HasRecvFlag(RT_RECV_FLAG.RECV_LIST))
                        continue;

                    client.EnqueueTcp(new RT_MSG_CLIENT_APP_SINGLE()
                    {
                        TargetOrSource = (short)source.DmeId,
                        Payload = Payload
                    });
                }
            }
        }

        public void SendUdpAppList(ClientObject source, IEnumerable<int> targetDmeIds, byte[] Payload)
        {
            foreach (var targetId in targetDmeIds)
            {
                if (Clients.TryGetValue(targetId, out var client))
                {
                    if (client == null || !client.IsAuthenticated || !client.IsConnected || !client.HasRecvFlag(RT_RECV_FLAG.RECV_LIST))
                        continue;

                    client.EnqueueUdp(new RT_MSG_CLIENT_APP_SINGLE()
                    {
                        TargetOrSource = (short)source.DmeId,
                        Payload = Payload
                    });
                }
            }
        }

        public void SendTcpAppSingle(ClientObject source, short targetDmeId, byte[] Payload)
        {
            var target = Clients.FirstOrDefault(x => x.Value.DmeId == targetDmeId).Value;

            // PROBE ONLY: when the in-game host answers a late joiner's handshake with a
            // tNW_GameSettings snapshot, that snapshot has no slot for the joiner. Inject one
            // on the way through and see whether the joiner still bails.
            var outgoing = Payload;
            int substBefore = _substitutions;
            if (source.DmeId == 0 && targetDmeId > 0)
            {
                // Remember the host's tNW_Player registration so it can be replayed.
                if (Payload != null && Payload.Length > 4 && Payload[0] == 0x00 && Payload[1] == 0x04)
                    _lastPlayerReg = (byte[])Payload.Clone();

                outgoing = TryInjectHotjoinSlot(Payload, targetDmeId);
            }

            if (target != null && target.IsAuthenticated && target.IsConnected && target.HasRecvFlag(RT_RECV_FLAG.RECV_SINGLE))
            {
                target.EnqueueTcp(new RT_MSG_CLIENT_APP_SINGLE()
                {
                    TargetOrSource = (short)source.DmeId,
                    Payload = outgoing
                });

                // A settings substitution means this client is being bootstrapped after
                // the host's one-shot messages have already gone by. The registration is
                // one-shot too, so replay it in the same breath.
                if (_substitutions != substBefore && _lastPlayerReg != null)
                {
                    Logger.Info($"HOTJOIN PROBE: replaying cached tNW_Player ({_lastPlayerReg.Length} bytes) to slot {targetDmeId}, world {WorldId}");
                    target.EnqueueTcp(new RT_MSG_CLIENT_APP_SINGLE()
                    {
                        TargetOrSource = 0,
                        Payload = (byte[])_lastPlayerReg.Clone()
                    });
                }
            }

            TryAnswerHotjoinPoll(source, targetDmeId, Payload);
        }

        public void SendUdpAppSingle(ClientObject source, short targetDmeId, byte[] Payload)
        {
            var target = Clients.FirstOrDefault(x => x.Value.DmeId == targetDmeId).Value;

            if (target != null && target.IsAuthenticated && target.IsConnected && target.HasRecvFlag(RT_RECV_FLAG.RECV_SINGLE))
            {
                target.EnqueueUdp(new RT_MSG_CLIENT_APP_SINGLE()
                {
                    TargetOrSource = (short)source.DmeId,
                    Payload = Payload
                });
            }
        }

        #endregion

        #region Message Handlers

        public void OnEndGameRequest(MediusServerEndGameRequest request)
        {
            SelfDestructFlag = true;
            ForceDestruct = request.BrutalFlag;
        }

        public async Task OnPlayerJoined(ClientObject player)
        {
            player.HasJoined = true;

            // Plugin
            await Program.Plugins.OnEvent(PluginEvent.DME_PLAYER_ON_JOINED, new OnPlayerArgs()
            {
                Player = player,
                Game = this
            });

            // Tell other clients
            foreach (var client in Clients)
            {
                if (!client.Value.HasJoined || client.Value == player || !client.Value.HasRecvFlag(RT_RECV_FLAG.RECV_NOTIFICATION))
                    continue;

                client.Value.EnqueueTcp(new RT_MSG_SERVER_CONNECT_NOTIFY()
                {
                    PlayerIndex = (short)player.DmeId,
                    ScertId = (short)player.ScertId,
                    IP = player.RemoteUdpEndpoint?.Address
                });
            }

            // Tell server
            Manager.Enqueue(new MediusServerConnectNotification()
            {
                MediusWorldUID = (uint)WorldId,
                PlayerSessionKey = player.SessionKey,
                ConnectEventType = MGCL_EVENT_TYPE.MGCL_EVENT_CLIENT_CONNECT
            });
        }

        public async Task OnPlayerLeft(ClientObject player)
        {
            player.HasJoined = false;
            // A reconnect under the same DmeId must be given the full snapshot again.
            _fullSettingsSentTo.Remove(player.DmeId);

            // Plugin
            await Program.Plugins.OnEvent(PluginEvent.DME_PLAYER_ON_LEFT, new OnPlayerArgs()
            {
                Player = player,
                Game = this
            });

            // Tell other clients
            foreach (var client in Clients)
            {
                if (!client.Value.HasJoined || client.Value == player || !client.Value.HasRecvFlag(RT_RECV_FLAG.RECV_NOTIFICATION))
                    continue;

                client.Value.EnqueueTcp(new RT_MSG_SERVER_DISCONNECT_NOTIFY()
                {
                    PlayerIndex = (short)player.DmeId,
                    ScertId = (short)player.ScertId,
                    IP = player.RemoteUdpEndpoint?.Address
                });
            }

            // Tell server
            Manager.Enqueue(new MediusServerConnectNotification()
            {
                MediusWorldUID = (uint)WorldId,
                PlayerSessionKey = player.SessionKey,
                ConnectEventType = MGCL_EVENT_TYPE.MGCL_EVENT_CLIENT_DISCONNECT
            });
        }

        public async Task<MediusServerJoinGameResponse> OnJoinGameRequest(MediusServerJoinGameRequest request)
        {
            ClientObject newClient;

            //
            await Task.Delay(100);

            // find existing client and reuse
            var existingClient = Clients.FirstOrDefault(x => x.Value.SessionKey == request.ConnectInfo.SessionKey);
            if (existingClient.Value != null)
            {
                // found existing
                return new MediusServerJoinGameResponse()
                {
                    MessageID = request.MessageID,
                    DmeClientIndex = existingClient.Value.DmeId,
                    AccessKey = existingClient.Value.Token,
                    Confirmation = MGCL_ERROR_CODE.MGCL_SUCCESS
                };
            }

            // If world is full then fail
            if (Clients.Count >= MAX_CLIENTS_PER_WORLD)
            {
                Logger.Warn($"Player attempted to join world {this} but there is no room!");
                return new MediusServerJoinGameResponse()
                {
                    MessageID = request.MessageID,
                    Confirmation = MGCL_ERROR_CODE.MGCL_UNSUCCESSFUL
                };
            }

            if (TryRegisterNewClientIndex(out var newClientIndex))
            {
                if (!Clients.TryAdd(newClientIndex, newClient = new ClientObject(request.ConnectInfo.SessionKey, this, newClientIndex)))
                {
                    UnregisterClientIndex(newClientIndex);
                    return new MediusServerJoinGameResponse()
                    {
                        MessageID = request.MessageID,
                        Confirmation = MGCL_ERROR_CODE.MGCL_UNSUCCESSFUL
                    };
                }
                else
                {
                    newClient.OnDestroyed += (client) => { UnregisterClientIndex(client.DmeId); };
                }
            }
            else
            {
                Logger.Warn($"Player attempted to join world {this} but unable to add player!");
                return new MediusServerJoinGameResponse()
                {
                    MessageID = request.MessageID,
                    Confirmation = MGCL_ERROR_CODE.MGCL_UNSUCCESSFUL
                };
            }

            // Add client to manager
            //newClient.BeginUdp();
            Manager.AddClient(newClient);

            return new MediusServerJoinGameResponse()
            {
                MessageID = request.MessageID,
                DmeClientIndex = newClient.DmeId,
                AccessKey = newClient.Token,
                Confirmation = MGCL_ERROR_CODE.MGCL_SUCCESS
            };
        }

        #endregion

        public override string ToString()
        {
            return $"WorldId:{WorldId}, ClientCount:{Clients.Count}";
        }

    }
}
