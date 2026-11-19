/**
 * Gerenciador de salas online via MQTT-over-WSS multi-broker (incluindo porta 443).
 * Funciona atrás de CGNAT 4G/5G e entre navegadores distintos sem depender de WebRTC/PeerJS.
 * Mantém autoridade do anfitrião (anti-trapaça: oculta valores das peças da mão do próprio cliente).
 */

import {
  DISCOVERY_TOPIC,
  FALLBACK_BROKER,
  PRIMARY_BROKERS,
  ROOM_CHANNEL_PREFIX,
  ROOMS_PREFIX,
  decodeMqttPayload,
  getBrokerById,
  randomClientId,
} from './mqttBrokers.js';

const ROOM_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const HEARTBEAT_INTERVAL_MS = 10000;
const PRESENCE_TIMEOUT_MS = 50000;
const FALLBACK_DELAY_MS = 3500;
const CONNECT_TIMEOUT_MS = 10000;
const JOIN_TIMEOUT_MS = 12000;
const JOIN_RETRY_MS = 1800;

export function generateRoomCode(len = 5) {
  let out = '';
  for (let i = 0; i < len; i++) {
    out += ROOM_CHARS[Math.floor(Math.random() * ROOM_CHARS.length)];
  }
  return out;
}

/**
 * Remove valores secretos antes de enviar o estado da partida para um convidado.
 */
export function sanitizeStateForPlayer(state, targetIndex, disputeReveal = null) {
  return {
    playerCount: state.playerCount,
    activeColors: state.activeColors,
    removedColor: state.removedColor,
    tilesPerPlayer: state.tilesPerPlayer,
    undistributedCount: state.undistributedCount,
    round: state.round,
    totalRounds: state.totalRounds,
    disputeNumber: state.disputeNumber,
    totalDisputesPerRound: state.totalDisputesPerRound,
    players: state.players.map((p) => {
      const isSelf = p.index === targetIndex;
      return {
        index: p.index,
        name: p.name,
        isHuman: p.isHuman,
        difficulty: p.difficulty,
        // Jogador NÃO recebe os valores numéricos da própria mão
        hand: isSelf
          ? p.hand.map((t) => ({ id: t.id, color: t.color, value: null, parity: t.parity }))
          : p.hand.map((t) => ({ ...t })),
        // Pontuação da rodada é aberta apenas para o próprio dono; fechada para os demais
        penaltyTiles: isSelf
          ? p.penaltyTiles.map((t) => ({ ...t }))
          : p.penaltyTiles.map((t) => ({ id: t.id, color: t.color, value: null, parity: t.parity })),
        roundScores: [...p.roundScores],
        totalScore: p.totalScore,
      };
    }),
    bagTiles: state.bagTiles.map((_, idx) => ({ id: `bag_${idx}` })),
    discardPile: state.discardPile.map((_, idx) => ({ id: `disc_${idx}` })),
    currentDispute: {
      leaderIndex: state.currentDispute.leaderIndex,
      turnIndex: state.currentDispute.turnIndex,
      plays: state.currentDispute.plays.map((pl) => ({ ...pl, tile: { ...pl.tile } })),
      requiredColor: state.currentDispute.requiredColor,
      forcedByTen: state.currentDispute.forcedByTen,
      lastPlayedWasTen: state.currentDispute.lastPlayedWasTen,
      pendingWinnerIndex: state.currentDispute.pendingWinnerIndex,
      lowestCandidates: state.currentDispute.lowestCandidates.map((t) => ({ ...t })),
    },
    lastDisputeResult: state.lastDisputeResult
      ? {
          ...state.lastDisputeResult,
          winningTile: { ...state.lastDisputeResult.winningTile },
          penaltyTile: { ...state.lastDisputeResult.penaltyTile },
          plays: state.lastDisputeResult.plays.map((pl) => ({ ...pl, tile: { ...pl.tile } })),
        }
      : null,
    lastRoundSummary: state.lastRoundSummary || null,
    phase: state.phase,
    log: state.log.slice(0, 15),
    winners: [...state.winners],
    disputeReveal,
  };
}

export class OnlineRoomManager {
  constructor(callbacks = {}) {
    this.callbacks = callbacks;
    this.role = null; // 'host' | 'guest'
    this.roomCode = null;
    this.password = '';
    this.maxPlayers = 4;
    this.hostName = '';
    this.visibility = 'private';
    this.roomName = '';
    this.myIndex = 0;
    this.inMatch = false;
    this.clientId = '';
    this.guests = []; // { clientId, name, index, brokerId, lastSeen }

    // Host state
    this.hostClients = new Map(); // brokerId -> { broker, client, connected, subReady }
    this._hostHeartbeatTimer = null;
    this._hostFallbackTimer = null;
    this._pingCycle = 0;
    this._listingSeq = 0;
    this._stateSeq = 0;
    this._roomCreatedAt = 0;
    this._processedActionIds = new Set();

    // Guest state
    this.guestClient = null;
    this.activeBrokerId = null;
    this.activeConnId = '';
    this.lastHostSeenAt = 0;
    this._lastStateSeq = 0;
    this._actionSeq = 0;
    this._guestPingTimer = null;
    this._probeClients = new Map();
    this._probeRetryTimer = null;
    this._probeFallbackTimer = null;
    this._probeTimeout = null;
    this._hostClosedHandled = false;

    this._sessionId = 0;
  }

  _ensureMqtt() {
    if (!window.mqtt || typeof window.mqtt.connect !== 'function') {
      throw new Error('Biblioteca de conexão online (MQTT) não carregada.');
    }
  }

  _isPublicListing() {
    return this.visibility === 'public' || this.visibility === 'public-pass';
  }

  _roomChannel(suffix) {
    return `${ROOM_CHANNEL_PREFIX}${this.roomCode}/${suffix}`;
  }

  _buildListingPayload() {
    return JSON.stringify({
      roomCode: this.roomCode,
      roomName: this.roomName,
      hostName: this.hostName,
      players: 1 + this.guests.length,
      maxPlayers: this.maxPlayers,
      hasPassword: Boolean(this.password),
      visibility: this.visibility,
      status: this.inMatch ? 'playing' : 'waiting',
      seq: ++this._listingSeq,
      createdAt: this._roomCreatedAt,
      updatedAt: Date.now(),
    });
  }

  _publishListingOnEntry(entry) {
    if (!this._isPublicListing() || this.inMatch || !this.roomCode) return;
    if (!entry?.client?.connected) return;
    try {
      entry.client.publish(`${ROOMS_PREFIX}${this.roomCode}`, this._buildListingPayload(), {
        retain: true,
        qos: 0,
      });
    } catch {}
  }

  _publishListingIfPublic() {
    if (!this._isPublicListing() || this.inMatch || !this.roomCode) return;
    const payload = this._buildListingPayload();
    const topic = `${ROOMS_PREFIX}${this.roomCode}`;
    for (const entry of this.hostClients.values()) {
      if (entry.connected && entry.client?.connected) {
        try {
          entry.client.publish(topic, payload, { retain: true, qos: 0 });
        } catch {}
      }
    }
  }

  _unpublishListing() {
    if (!this.roomCode) return;
    const topic = `${ROOMS_PREFIX}${this.roomCode}`;
    for (const entry of this.hostClients.values()) {
      if (entry.client?.connected) {
        try {
          entry.client.publish(topic, '', { retain: true, qos: 0 });
        } catch {}
      }
    }
  }

  _getAnyConnectedHostEntry(preferredBrokerId = null) {
    if (preferredBrokerId) {
      const pref = this.hostClients.get(preferredBrokerId);
      if (pref?.connected && pref.client?.connected) return pref;
    }
    for (const entry of this.hostClients.values()) {
      if (entry.connected && entry.client?.connected) return entry;
    }
    return null;
  }

  _publishHostToGuest(guest, payloadObj, preferredBrokerId = null) {
    const entry = this._getAnyConnectedHostEntry(preferredBrokerId || guest.brokerId);
    if (!entry) return;
    try {
      entry.client.publish(this._roomChannel(`to/${guest.clientId}`), JSON.stringify(payloadObj), {
        qos: 0,
        retain: false,
      });
    } catch {}
  }

  _publishHostToClientId(clientId, brokerId, payloadObj) {
    const entry = this._getAnyConnectedHostEntry(brokerId);
    if (!entry || !clientId) return;
    try {
      entry.client.publish(this._roomChannel(`to/${clientId}`), JSON.stringify(payloadObj), {
        qos: 0,
        retain: false,
      });
    } catch {}
  }

  _publishHostBroadcast(payloadObj) {
    const raw = JSON.stringify(payloadObj);
    const topic = this._roomChannel('broadcast');
    for (const entry of this.hostClients.values()) {
      if (entry.connected && entry.client?.connected) {
        try {
          entry.client.publish(topic, raw, { qos: 0, retain: false });
        } catch {}
      }
    }
  }

  /**
   * Cria uma sala como Anfitrião conectando em múltiplos brokers MQTT-over-WSS.
   */
  async createRoom({ hostName, maxPlayers, password = '', visibility = 'private', roomName = '' }) {
    this._ensureMqtt();
    this.destroy();

    const sessionId = ++this._sessionId;
    this.role = 'host';
    this.hostName = String(hostName || '').trim().slice(0, 16) || 'Anfitrião';
    this.maxPlayers = maxPlayers;
    this.password = password.trim();
    this.visibility = visibility;
    this.roomName = String(roomName || '').trim().slice(0, 28) || `Mesa de ${this.hostName}`;
    this.roomCode = generateRoomCode(5);
    this.clientId = randomClientId('h');
    this.myIndex = 0;
    this.inMatch = false;
    this.guests = [];
    this._pingCycle = 0;
    this._listingSeq = 0;
    this._stateSeq = 0;
    this._roomCreatedAt = Date.now();
    this._processedActionIds.clear();

    const isPublic = this._isPublicListing();
    const toHostTopic = this._roomChannel('to-host');
    const listingTopic = `${ROOMS_PREFIX}${this.roomCode}`;

    return new Promise((resolve, reject) => {
      let settled = false;

      const connectTimeout = setTimeout(() => {
        if (this._sessionId !== sessionId || settled) return;
        settled = true;
        this.destroy();
        reject(new Error('Não foi possível conectar ao servidor online. Tente novamente.'));
      }, CONNECT_TIMEOUT_MS);

      const notifyHostConnStatus = () => {
        if (this._sessionId !== sessionId) return;
        const anyConnected = [...this.hostClients.values()].some((e) => e.connected);
        this.callbacks.onConnectionStatus?.(anyConnected ? 'connected' : 'reconnecting');
      };

      const startHostBroker = (broker) => {
        if (this._sessionId !== sessionId || this.hostClients.has(broker.id)) return;

        const clientOpts = {
          ...broker.options,
          clientId: randomClientId(`h${broker.id[0]}`),
          clean: true,
          keepalive: 45,
          reconnectPeriod: 2500,
          connectTimeout: 8000,
        };
        if (isPublic) {
          clientOpts.will = {
            topic: listingTopic,
            payload: '',
            qos: 0,
            retain: true,
          };
        }

        const client = window.mqtt.connect(broker.url, clientOpts);
        const entry = { broker, client, connected: false, subReady: false };
        this.hostClients.set(broker.id, entry);

        client.on('connect', () => {
          if (this._sessionId !== sessionId || this.hostClients.get(broker.id) !== entry) return;
          entry.connected = true;
          notifyHostConnStatus();

          client.subscribe(toHostTopic, { qos: 0 }, (err) => {
            if (this._sessionId !== sessionId || this.hostClients.get(broker.id) !== entry) return;
            if (!err) {
              entry.subReady = true;
              if (!settled) {
                settled = true;
                clearTimeout(connectTimeout);
                if (this._hostFallbackTimer) {
                  clearTimeout(this._hostFallbackTimer);
                  this._hostFallbackTimer = null;
                }
                this._startHostHeartbeat();
                resolve({
                  roomCode: this.roomCode,
                  hasPassword: Boolean(this.password),
                  maxPlayers: this.maxPlayers,
                  visibility: this.visibility,
                  roomName: this.roomName,
                  members: this.getMembersSnapshot(),
                });
              }
            }
          });

          if (isPublic && !this.inMatch) {
            client.subscribe(DISCOVERY_TOPIC, { qos: 0 });
            this._publishListingOnEntry(entry);
          }
        });

        client.on('message', (topic, payload) => {
          if (this._sessionId !== sessionId || this.hostClients.get(broker.id) !== entry) return;
          const text = decodeMqttPayload(payload).trim();
          if (!text) return;

          let msg;
          try {
            msg = JSON.parse(text);
          } catch {
            return;
          }
          if (!msg || typeof msg !== 'object') return;

          if (topic === DISCOVERY_TOPIC) {
            if (msg.type === 'LIST_REQUEST' && this._isPublicListing() && !this.inMatch) {
              this._publishListingOnEntry(entry);
            }
            return;
          }

          if (topic === toHostTopic) {
            this._handleHostIncoming(broker.id, msg);
          }
        });

        client.on('reconnect', () => {
          if (this._sessionId !== sessionId) return;
          entry.connected = false;
          entry.subReady = false;
          notifyHostConnStatus();
        });

        client.on('offline', () => {
          if (this._sessionId !== sessionId) return;
          entry.connected = false;
          entry.subReady = false;
          notifyHostConnStatus();
        });

        client.on('close', () => {
          if (this._sessionId !== sessionId) return;
          entry.connected = false;
          entry.subReady = false;
          notifyHostConnStatus();
        });

        client.on('error', () => {
          if (this._sessionId !== sessionId) return;
          entry.connected = false;
          notifyHostConnStatus();
        });
      };

      for (const broker of PRIMARY_BROKERS) {
        startHostBroker(broker);
      }

      this._hostFallbackTimer = setTimeout(() => {
        if (this._sessionId !== sessionId) return;
        const anyReady = [...this.hostClients.values()].some((e) => e.subReady);
        if (!anyReady && FALLBACK_BROKER) {
          startHostBroker(FALLBACK_BROKER);
        }
      }, FALLBACK_DELAY_MS);
    });
  }

  _startHostHeartbeat() {
    if (this._hostHeartbeatTimer) clearInterval(this._hostHeartbeatTimer);
    this._hostHeartbeatTimer = setInterval(() => {
      if (this.role !== 'host' || !this.roomCode) return;

      this._pingCycle += 1;
      this._publishHostBroadcast({ type: 'HOST_PING', ts: Date.now() });

      if (this._pingCycle % 2 === 0 && this._isPublicListing() && !this.inMatch) {
        this._publishListingIfPublic();
      }

      const now = Date.now();
      const timedOut = this.guests.filter((g) => now - g.lastSeen > PRESENCE_TIMEOUT_MS);
      for (const g of timedOut) {
        this._removeGuest(g);
      }
    }, HEARTBEAT_INTERVAL_MS);
  }

  _handleHostIncoming(brokerId, msg) {
    if (!msg.type) return;

    if (msg.type === 'JOIN_REQUEST') {
      const guestCid = String(msg.clientId || '').trim();
      if (!guestCid) return;
      const probeConnId = msg.connId ? String(msg.connId) : '';

      const existing = this.guests.find((g) => g.clientId === guestCid);
      if (existing) {
        existing.lastSeen = Date.now();
        this._publishHostToClientId(existing.clientId, brokerId, {
          type: 'JOIN_ACCEPTED',
          roomCode: this.roomCode,
          hasPassword: Boolean(this.password),
          maxPlayers: this.maxPlayers,
          visibility: this.visibility,
          roomName: this.roomName,
          assignedIndex: existing.index,
          members: this.getMembersSnapshot(),
        });
        return;
      }

      if (this.inMatch) {
        this._publishHostToClientId(guestCid, brokerId, {
          type: 'JOIN_REJECTED',
          reason: 'ALREADY_STARTED',
          message: 'A partida já começou.',
        });
        return;
      }

      if (1 + this.guests.length >= this.maxPlayers) {
        this._publishHostToClientId(guestCid, brokerId, {
          type: 'JOIN_REJECTED',
          reason: 'ROOM_FULL',
          message: 'A sala já está cheia.',
        });
        return;
      }

      if (this.password && String(msg.password || '').trim() !== this.password) {
        this._publishHostToClientId(guestCid, brokerId, {
          type: 'JOIN_REJECTED',
          reason: 'WRONG_PASSWORD',
          message: msg.password ? 'Senha incorreta.' : 'Esta sala exige senha.',
        });
        return;
      }

      const guestIndex = 1 + this.guests.length;
      const cleanName = String(msg.name || '').trim().slice(0, 16) || `Jogador ${guestIndex + 1}`;
      const guest = {
        clientId: guestCid,
        connId: probeConnId,
        name: cleanName,
        index: guestIndex,
        brokerId,
        lastSeen: Date.now(),
      };
      this.guests.push(guest);

      this._publishHostToGuest(guest, {
        type: 'JOIN_ACCEPTED',
        roomCode: this.roomCode,
        hasPassword: Boolean(this.password),
        maxPlayers: this.maxPlayers,
        visibility: this.visibility,
        roomName: this.roomName,
        assignedIndex: guestIndex,
        members: this.getMembersSnapshot(),
      });

      this.broadcastRoomUpdate();
      this._publishListingIfPublic();
      this.callbacks.onRoomUpdate?.(this.getMembersSnapshot());
      return;
    }

    const guest = this.guests.find((g) => g.clientId === msg.clientId);
    if (!guest) return;

    if (msg.type === 'GUEST_LEAVE') {
      if (msg.connId && guest.connId && String(msg.connId) !== guest.connId) {
        return;
      }
      this._removeGuest(guest);
      return;
    }

    guest.lastSeen = Date.now();
    guest.brokerId = brokerId;
    if (msg.connId) {
      guest.connId = String(msg.connId);
    }

    if (msg.type === 'GUEST_PING') {
      return;
    }

    if (msg.actionId) {
      if (this._processedActionIds.has(msg.actionId)) return;
      this._processedActionIds.add(msg.actionId);
      if (this._processedActionIds.size > 120) {
        const oldest = this._processedActionIds.values().next().value;
        this._processedActionIds.delete(oldest);
      }
    }

    this.callbacks.onGuestAction?.(guest.index, msg);
  }

  _removeGuest(guest) {
    if (!this.guests.includes(guest)) return;
    const idx = guest.index;
    const name = guest.name;
    this.guests = this.guests.filter((g) => g !== guest);

    if (!this.inMatch) {
      this.guests.forEach((g, i) => {
        g.index = i + 1;
        this._publishHostToGuest(g, { type: 'REASSIGN_INDEX', assignedIndex: g.index });
      });
      this.broadcastRoomUpdate();
      this._publishListingIfPublic();
      this.callbacks.onRoomUpdate?.(this.getMembersSnapshot());
    } else {
      this.callbacks.onGuestDisconnectedInMatch?.(idx, name);
    }
  }

  getMembersSnapshot() {
    const list = [{ index: 0, name: this.hostName, isHost: true, isBot: false }];
    for (const g of this.guests) {
      list.push({ index: g.index, name: g.name, isHost: false, isBot: false });
    }
    return list;
  }

  broadcastRoomUpdate() {
    if (this.role !== 'host') return;
    const members = this.getMembersSnapshot();
    const payload = {
      type: 'ROOM_UPDATE',
      members,
      maxPlayers: this.maxPlayers,
      hasPassword: Boolean(this.password),
      visibility: this.visibility,
      roomName: this.roomName,
    };
    for (const g of this.guests) {
      this._publishHostToGuest(g, payload);
    }
  }

  broadcastGameState(state, { disputeReveal = null, toast = null, event = null } = {}) {
    if (this.role !== 'host') return;
    const firstEnter = !this.inMatch;
    this.inMatch = true;

    if (firstEnter) {
      this._unpublishListing();
    }

    const seq = ++this._stateSeq;
    for (const g of this.guests) {
      this._publishHostToGuest(g, {
        type: 'STATE_SYNC',
        seq,
        state: sanitizeStateForPlayer(state, g.index, disputeReveal),
        toast,
        event,
      });
    }
  }

  broadcastReturnToLobby() {
    if (this.role !== 'host') return;
    this.inMatch = false;
    for (const g of this.guests) {
      this._publishHostToGuest(g, { type: 'RETURN_TO_LOBBY' });
    }
    this._publishListingIfPublic();
  }

  /**
   * Conecta como Convidado em uma sala existente via código usando MQTT-over-WSS multi-broker.
   */
  joinRoom({ roomCode, playerName, password = '', preferredBrokerId = null }) {
    this._ensureMqtt();
    this.destroy();

    const cleanCode = String(roomCode || '').trim().toUpperCase();
    if (!cleanCode) {
      return Promise.reject(new Error('Informe o código da sala.'));
    }

    const sessionId = ++this._sessionId;
    this.role = 'guest';
    this.roomCode = cleanCode;
    this.clientId = randomClientId('g');
    this._lastStateSeq = 0;
    this._actionSeq = 0;
    this._hostClosedHandled = false;

    const toHostTopic = this._roomChannel('to-host');
    const directTopic = this._roomChannel(`to/${this.clientId}`);
    const broadcastTopic = this._roomChannel('broadcast');

    const initialBrokers = [...PRIMARY_BROKERS];
    if (preferredBrokerId) {
      const pref = getBrokerById(preferredBrokerId);
      if (pref) {
        const idx = initialBrokers.findIndex((b) => b.id === pref.id);
        if (idx >= 0) initialBrokers.splice(idx, 1);
        initialBrokers.unshift(pref);
      }
    }

    return new Promise((resolve, reject) => {
      let settled = false;
      let hadAnyConnected = false;

      const sendJoinOnProbe = (probe) => {
        if (!probe.client?.connected || !probe.subReady) return;
        try {
          probe.client.publish(
            toHostTopic,
            JSON.stringify({
              type: 'JOIN_REQUEST',
              clientId: this.clientId,
              connId: probe.connId,
              name: playerName,
              password,
            }),
            { qos: 0, retain: false }
          );
        } catch {}
      };

      const stopProbingExcept = (winnerId = null) => {
        if (this._probeRetryTimer) {
          clearInterval(this._probeRetryTimer);
          this._probeRetryTimer = null;
        }
        if (this._probeFallbackTimer) {
          clearTimeout(this._probeFallbackTimer);
          this._probeFallbackTimer = null;
        }
        if (this._probeTimeout) {
          clearTimeout(this._probeTimeout);
          this._probeTimeout = null;
        }
        for (const [id, probe] of this._probeClients.entries()) {
          if (id !== winnerId) {
            try {
              probe.client.end(false);
            } catch {}
            this._probeClients.delete(id);
          }
        }
      };

      const handleDirectOrBroadcastMsg = (probe, msg) => {
        if (!msg || typeof msg !== 'object' || !msg.type) return;

        if (!settled) {
          if (msg.type === 'JOIN_REJECTED') {
            settled = true;
            stopProbingExcept(null);
            this.destroy();
            reject(new Error(msg.message || 'Entrada recusada na sala.'));
            return;
          }

          if (msg.type === 'JOIN_ACCEPTED') {
            settled = true;
            this.guestClient = probe.client;
            this.activeBrokerId = probe.broker.id;
            this.activeConnId = probe.connId;
            stopProbingExcept(probe.broker.id);

            this.myIndex = msg.assignedIndex;
            this.maxPlayers = msg.maxPlayers;
            this.visibility = msg.visibility || 'private';
            this.roomName = msg.roomName || '';
            this.lastHostSeenAt = Date.now();
            this._sendGuestPing();
            this._startGuestHeartbeat();
            this.callbacks.onConnectionStatus?.('connected');
            resolve(msg);
            return;
          }
          return;
        }

        // Após entrar, aceita mensagens apenas do broker vencedor
        if (probe.broker.id !== this.activeBrokerId) return;

        if (msg.type === 'HOST_PING') {
          this.lastHostSeenAt = Date.now();
          return;
        }

        if (msg.type === 'HOST_CLOSED') {
          this._triggerHostDisconnected();
          return;
        }

        if (msg.type === 'REASSIGN_INDEX') {
          this.lastHostSeenAt = Date.now();
          this.myIndex = msg.assignedIndex;
          return;
        }

        if (msg.type === 'ROOM_UPDATE') {
          this.lastHostSeenAt = Date.now();
          this.callbacks.onRoomUpdate?.(msg.members, msg);
          return;
        }

        if (msg.type === 'STATE_SYNC') {
          this.lastHostSeenAt = Date.now();
          if (typeof msg.seq === 'number') {
            if (msg.seq <= this._lastStateSeq) return;
            this._lastStateSeq = msg.seq;
          }
          this.inMatch = true;
          this.callbacks.onStateSync?.(msg);
          return;
        }

        if (msg.type === 'RETURN_TO_LOBBY') {
          this.lastHostSeenAt = Date.now();
          this.inMatch = false;
          this.callbacks.onHostReturnedLobby?.();
        }
      };

      const startGuestProbe = (broker) => {
        if (this._sessionId !== sessionId || this._probeClients.has(broker.id)) return;

        const connId = Math.random().toString(36).slice(2, 8);
        const client = window.mqtt.connect(broker.url, {
          ...broker.options,
          clientId: randomClientId(`g${broker.id[0]}`),
          clean: true,
          keepalive: 45,
          reconnectPeriod: 2500,
          connectTimeout: 8000,
          will: {
            topic: toHostTopic,
            payload: JSON.stringify({ type: 'GUEST_LEAVE', clientId: this.clientId, connId }),
            qos: 0,
            retain: false,
          },
        });

        const probe = { broker, client, connId, connected: false, subReady: false };
        this._probeClients.set(broker.id, probe);

        client.on('connect', () => {
          if (this._sessionId !== sessionId) return;
          probe.connected = true;
          hadAnyConnected = true;

          client.subscribe([directTopic, broadcastTopic], { qos: 0 }, (err) => {
            if (this._sessionId !== sessionId) return;
            if (!err) {
              probe.subReady = true;
              if (!settled) {
                sendJoinOnProbe(probe);
              } else if (probe.broker.id === this.activeBrokerId) {
                this.callbacks.onConnectionStatus?.('connected');
                this._sendGuestPing();
              }
            }
          });
        });

        client.on('message', (topic, payload) => {
          if (this._sessionId !== sessionId) return;
          if (topic !== directTopic && topic !== broadcastTopic) return;
          const text = decodeMqttPayload(payload).trim();
          if (!text) return;
          try {
            const msg = JSON.parse(text);
            handleDirectOrBroadcastMsg(probe, msg);
          } catch {}
        });

        client.on('reconnect', () => {
          if (this._sessionId !== sessionId) return;
          probe.connected = false;
          probe.subReady = false;
          if (settled && probe.broker.id === this.activeBrokerId) {
            this.callbacks.onConnectionStatus?.('reconnecting');
          }
        });

        client.on('offline', () => {
          if (this._sessionId !== sessionId) return;
          probe.connected = false;
          if (settled && probe.broker.id === this.activeBrokerId) {
            this.callbacks.onConnectionStatus?.('reconnecting');
          }
        });
      };

      for (const broker of initialBrokers) {
        startGuestProbe(broker);
      }

      this._probeRetryTimer = setInterval(() => {
        if (this._sessionId !== sessionId || settled) return;
        for (const probe of this._probeClients.values()) {
          sendJoinOnProbe(probe);
        }
      }, JOIN_RETRY_MS);

      this._probeFallbackTimer = setTimeout(() => {
        if (this._sessionId !== sessionId || settled) return;
        if (FALLBACK_BROKER && !this._probeClients.has(FALLBACK_BROKER.id)) {
          startGuestProbe(FALLBACK_BROKER);
        }
      }, FALLBACK_DELAY_MS);

      this._probeTimeout = setTimeout(() => {
        if (this._sessionId !== sessionId || settled) return;
        settled = true;
        stopProbingExcept(null);
        this.destroy();
        reject(
          new Error(
            hadAnyConnected
              ? 'Sala não encontrada ou já encerrada. Confira o código e tente novamente.'
              : 'Sem conexão com o servidor online. Verifique sua internet.'
          )
        );
      }, JOIN_TIMEOUT_MS);
    });
  }

  _sendGuestPing() {
    if (this.role !== 'guest' || !this.guestClient?.connected || !this.roomCode) return;
    try {
      this.guestClient.publish(
        this._roomChannel('to-host'),
        JSON.stringify({
          type: 'GUEST_PING',
          clientId: this.clientId,
          connId: this.activeConnId || undefined,
        }),
        { qos: 0, retain: false }
      );
    } catch {}
  }

  _startGuestHeartbeat() {
    if (this._guestPingTimer) clearInterval(this._guestPingTimer);
    this._guestPingTimer = setInterval(() => {
      if (this.role !== 'guest' || !this.roomCode) return;
      this._sendGuestPing();
      if (Date.now() - this.lastHostSeenAt > PRESENCE_TIMEOUT_MS) {
        this._triggerHostDisconnected();
      }
    }, HEARTBEAT_INTERVAL_MS);
  }

  _triggerHostDisconnected() {
    if (this._hostClosedHandled) return;
    this._hostClosedHandled = true;
    this.callbacks.onHostDisconnected?.();
  }

  sendToHost(action) {
    if (this.role !== 'guest' || !this.guestClient || !this.roomCode) return;
    const actionId = `a_${this.myIndex}_${++this._actionSeq}_${Math.random().toString(36).slice(2, 6)}`;
    const payload = JSON.stringify({
      ...action,
      clientId: this.clientId,
      actionId,
    });
    try {
      if (!this.guestClient.connected && typeof this.guestClient.reconnect === 'function') {
        this.guestClient.reconnect();
      }
      this.guestClient.publish(this._roomChannel('to-host'), payload, { qos: 0, retain: false });
    } catch {}
  }

  /**
   * Garante reconexão automática após alternar entre aplicativos/abas no celular.
   */
  ensureConnected() {
    if (this.role === 'host' && this.roomCode) {
      for (const entry of this.hostClients.values()) {
        if (!entry.client.connected && !entry.client.reconnecting && typeof entry.client.reconnect === 'function') {
          try {
            entry.client.reconnect();
          } catch {}
        }
      }
      if (!this.inMatch) {
        this._publishListingIfPublic();
      }
      this._publishHostBroadcast({ type: 'HOST_PING', ts: Date.now() });
    } else if (this.role === 'guest' && this.guestClient && this.roomCode) {
      if (!this.guestClient.connected && !this.guestClient.reconnecting && typeof this.guestClient.reconnect === 'function') {
        try {
          this.guestClient.reconnect();
        } catch {}
      }
      this._sendGuestPing();
    }
  }

  destroy() {
    this._sessionId += 1;

    if (this._hostHeartbeatTimer) {
      clearInterval(this._hostHeartbeatTimer);
      this._hostHeartbeatTimer = null;
    }
    if (this._hostFallbackTimer) {
      clearTimeout(this._hostFallbackTimer);
      this._hostFallbackTimer = null;
    }
    if (this._guestPingTimer) {
      clearInterval(this._guestPingTimer);
      this._guestPingTimer = null;
    }
    if (this._probeRetryTimer) {
      clearInterval(this._probeRetryTimer);
      this._probeRetryTimer = null;
    }
    if (this._probeFallbackTimer) {
      clearTimeout(this._probeFallbackTimer);
      this._probeFallbackTimer = null;
    }
    if (this._probeTimeout) {
      clearTimeout(this._probeTimeout);
      this._probeTimeout = null;
    }

    if (this.role === 'host' && this.roomCode) {
      const listingTopic = `${ROOMS_PREFIX}${this.roomCode}`;
      const broadcastTopic = this._roomChannel('broadcast');
      const isPublic = this._isPublicListing();
      const clientsToClose = [...this.hostClients.values()];
      this.hostClients.clear();

      for (const entry of clientsToClose) {
        try {
          if (entry.client?.connected) {
            if (isPublic) {
              entry.client.publish(listingTopic, '', { retain: true, qos: 0 });
            }
            entry.client.publish(broadcastTopic, JSON.stringify({ type: 'HOST_CLOSED' }), { qos: 0, retain: false });
            setTimeout(() => {
              try {
                entry.client.end(false);
              } catch {}
            }, 150);
          } else {
            entry.client?.end(true);
          }
        } catch {}
      }
    } else {
      for (const entry of this.hostClients.values()) {
        try {
          entry.client?.end(true);
        } catch {}
      }
      this.hostClients.clear();
    }

    if (this.role === 'guest' && this.guestClient && this.roomCode) {
      const client = this.guestClient;
      const toHostTopic = this._roomChannel('to-host');
      const cid = this.clientId;
      const connId = this.activeConnId;
      this.guestClient = null;
      this.activeConnId = '';
      try {
        if (client.connected) {
          client.publish(toHostTopic, JSON.stringify({ type: 'GUEST_LEAVE', clientId: cid, connId }), {
            qos: 0,
            retain: false,
          });
          setTimeout(() => {
            try {
              client.end(false);
            } catch {}
          }, 120);
        } else {
          client.end(true);
        }
      } catch {}
    } else if (this.guestClient) {
      try {
        this.guestClient.end(true);
      } catch {}
      this.guestClient = null;
    }

    for (const probe of this._probeClients.values()) {
      try {
        probe.client?.end(false);
      } catch {}
    }
    this._probeClients.clear();

    this.guests = [];
    this.role = null;
    this.roomCode = null;
    this.password = '';
    this.visibility = 'private';
    this.roomName = '';
    this.inMatch = false;
    this.clientId = '';
    this.activeBrokerId = null;
    this.activeConnId = '';
    this._hostClosedHandled = false;
  }
}
