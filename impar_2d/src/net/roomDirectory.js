/**
 * Diretório em tempo real de salas públicas via MQTT-over-WSS multi-broker.
 * Combina mensagens retidas, descoberta ativa (LIST_REQUEST) e suporte a porta 443.
 * Sem filtro por relógio do remetente (clock skew) para funcionar entre quaisquer dispositivos.
 */

import {
  DISCOVERY_TOPIC,
  FALLBACK_BROKER,
  PRIMARY_BROKERS,
  ROOMS_PREFIX,
  decodeMqttPayload,
  randomClientId,
} from './mqttBrokers.js';

const LOCAL_TTL_MS = 55000;
const FALLBACK_DELAY_MS = 3500;

export class RoomDirectoryManager {
  constructor() {
    this.clients = new Map(); // brokerId -> { broker, client, connected }
    this.brokerRooms = new Map(); // brokerId -> Map(roomCode -> { data, receivedAt })
    this.pruneTimer = null;
    this.fallbackTimer = null;
    this.onRoomsChange = null;
    this.onStatusChange = null;
    this._browseSessionId = 0;
  }

  startBrowsing({ onRoomsChange, onStatusChange } = {}) {
    this.stopBrowsing();
    const sessionId = ++this._browseSessionId;
    this._browseStartedAt = Date.now();
    this.onRoomsChange = onRoomsChange || null;
    this.onStatusChange = onStatusChange || null;
    this.brokerRooms.clear();

    if (!window.mqtt || typeof window.mqtt.connect !== 'function') {
      this.onStatusChange?.('offline', 'Serviço de lista indisponível.');
      this._emitRooms();
      return;
    }

    this.onStatusChange?.('connecting', 'Buscando salas abertas...');

    const startBroker = (broker) => {
      if (this._browseSessionId !== sessionId || this.clients.has(broker.id)) return;

      const client = window.mqtt.connect(broker.url, {
        ...broker.options,
        clientId: randomClientId(`br_${broker.id[0]}`),
        clean: true,
        keepalive: 30,
        reconnectPeriod: 3000,
        connectTimeout: 8000,
      });

      const entry = { broker, client, connected: false };
      this.clients.set(broker.id, entry);
      if (!this.brokerRooms.has(broker.id)) {
        this.brokerRooms.set(broker.id, new Map());
      }

      const sendListRequest = () => {
        if (!client.connected) return;
        try {
          client.publish(
            DISCOVERY_TOPIC,
            JSON.stringify({ type: 'LIST_REQUEST', ts: Date.now() }),
            { qos: 0, retain: false }
          );
        } catch {}
      };

      client.on('connect', () => {
        if (this._browseSessionId !== sessionId || this.clients.get(broker.id) !== entry) return;
        entry.connected = true;
        this._emitStatus();

        client.subscribe(`${ROOMS_PREFIX}+`, { qos: 0 }, (err) => {
          if (this._browseSessionId !== sessionId || this.clients.get(broker.id) !== entry) return;
          if (!err) {
            sendListRequest();
            this._emitRooms();
          }
        });
      });

      client.on('message', (topic, payload) => {
        if (this._browseSessionId !== sessionId || this.clients.get(broker.id) !== entry) return;
        if (!topic || !topic.startsWith(ROOMS_PREFIX)) return;

        const roomCode = topic.slice(ROOMS_PREFIX.length);
        if (!roomCode || roomCode.includes('/')) return;

        const roomMap = this.brokerRooms.get(broker.id);
        const text = decodeMqttPayload(payload).trim();
        if (!text) {
          if (roomMap?.delete(roomCode)) {
            this._emitRooms();
          }
          return;
        }

        try {
          const data = JSON.parse(text);
          if (
            !data ||
            data.roomCode !== roomCode ||
            data.status !== 'waiting' ||
            typeof data.maxPlayers !== 'number' ||
            data.maxPlayers < 3 ||
            data.maxPlayers > 5
          ) {
            if (roomMap?.delete(roomCode)) {
              this._emitRooms();
            }
            return;
          }

          roomMap?.set(roomCode, { data, receivedAt: Date.now() });
          this._emitRooms();
        } catch {
          // Ignora payloads malformados
        }
      });

      client.on('reconnect', () => {
        if (this._browseSessionId !== sessionId) return;
        entry.connected = false;
        this._emitStatus();
      });

      client.on('offline', () => {
        if (this._browseSessionId !== sessionId) return;
        entry.connected = false;
        this._emitStatus();
      });

      client.on('close', () => {
        if (this._browseSessionId !== sessionId) return;
        entry.connected = false;
        this._emitStatus();
      });

      client.on('error', () => {
        if (this._browseSessionId !== sessionId) return;
        entry.connected = false;
        this._emitStatus();
      });
    };

    for (const broker of PRIMARY_BROKERS) {
      startBroker(broker);
    }

    this.fallbackTimer = setTimeout(() => {
      if (this._browseSessionId !== sessionId) return;
      const anyConnected = [...this.clients.values()].some((e) => e.connected);
      if (!anyConnected && FALLBACK_BROKER) {
        startBroker(FALLBACK_BROKER);
      }
    }, FALLBACK_DELAY_MS);

    this.pruneTimer = setInterval(() => {
      const now = Date.now();
      let changed = false;
      for (const roomMap of this.brokerRooms.values()) {
        for (const [code, item] of roomMap.entries()) {
          if (now - item.receivedAt > LOCAL_TTL_MS) {
            roomMap.delete(code);
            changed = true;
          }
        }
      }
      if (changed) this._emitRooms();
      this._emitStatus();
    }, 10000);
  }

  _emitStatus() {
    const entries = [...this.clients.values()];
    if (entries.some((e) => e.connected)) {
      this.onStatusChange?.('connected', 'Conectado');
    } else if (this._browseStartedAt && Date.now() - this._browseStartedAt > 9000) {
      this.onStatusChange?.('offline', 'Não foi possível conectar à lista pública agora.');
    } else if (entries.some((e) => e.client?.reconnecting)) {
      this.onStatusChange?.('connecting', 'Reconectando...');
    } else if (entries.length > 0) {
      this.onStatusChange?.('connecting', 'Buscando salas abertas...');
    } else {
      this.onStatusChange?.('offline', 'Não foi possível conectar à lista pública agora.');
    }
  }

  stopBrowsing() {
    this._browseSessionId += 1;
    if (this.pruneTimer) {
      clearInterval(this.pruneTimer);
      this.pruneTimer = null;
    }
    if (this.fallbackTimer) {
      clearTimeout(this.fallbackTimer);
      this.fallbackTimer = null;
    }
    for (const entry of this.clients.values()) {
      try {
        entry.client?.end(false);
      } catch {}
    }
    this.clients.clear();
    this.brokerRooms.clear();
    this.onRoomsChange = null;
    this.onStatusChange = null;
  }

  refresh() {
    const now = Date.now();
    for (const roomMap of this.brokerRooms.values()) {
      for (const [code, item] of roomMap.entries()) {
        if (now - item.receivedAt > LOCAL_TTL_MS) {
          roomMap.delete(code);
        }
      }
    }
    this._emitRooms();

    const connectedEntries = [...this.clients.values()].filter((e) => e.connected && e.client?.connected);
    if (connectedEntries.length > 0) {
      const reqPayload = JSON.stringify({ type: 'LIST_REQUEST', ts: Date.now() });
      const topic = `${ROOMS_PREFIX}+`;
      for (const entry of connectedEntries) {
        try {
          entry.client.publish(DISCOVERY_TOPIC, reqPayload, { qos: 0, retain: false });
          entry.client.unsubscribe(topic, () => {
            if (entry.client?.connected) {
              entry.client.subscribe(topic, { qos: 0 });
            }
          });
        } catch {}
      }
      this._emitStatus();
    } else if (this.onRoomsChange || this.onStatusChange) {
      this.startBrowsing({
        onRoomsChange: this.onRoomsChange,
        onStatusChange: this.onStatusChange,
      });
    }
  }

  ensureConnected() {
    if (this.onRoomsChange || this.onStatusChange) {
      this.refresh();
    }
  }

  removeLocalRoom(roomCode) {
    if (!roomCode) return;
    let changed = false;
    for (const roomMap of this.brokerRooms.values()) {
      if (roomMap.delete(roomCode)) {
        changed = true;
      }
    }
    if (changed) this._emitRooms();
  }

  updateRoomPlayers() {}

  unpublishRoom() {}

  _emitRooms() {
    const bestByCode = new Map();
    for (const [brokerId, roomMap] of this.brokerRooms.entries()) {
      for (const [roomCode, entry] of roomMap.entries()) {
        if (!entry.data || entry.data.status !== 'waiting') continue;
        const prev = bestByCode.get(roomCode);
        const curSeq = entry.data.seq || 0;
        const prevSeq = prev?.data?.seq || 0;
        if (
          !prev ||
          curSeq > prevSeq ||
          (curSeq === prevSeq && entry.receivedAt > prev.receivedAt)
        ) {
          bestByCode.set(roomCode, { ...entry, brokerId });
        }
      }
    }

    const list = [...bestByCode.values()].map((entry) => ({
      ...entry.data,
      brokerId: entry.brokerId,
    }));

    list.sort((a, b) => {
      const aOpen = (a.players || 1) < a.maxPlayers ? 0 : 1;
      const bOpen = (b.players || 1) < b.maxPlayers ? 0 : 1;
      if (aOpen !== bOpen) return aOpen - bOpen;
      return (b.createdAt || 0) - (a.createdAt || 0);
    });

    this.onRoomsChange?.(list);
  }
}
