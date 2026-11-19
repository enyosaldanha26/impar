/**
 * Diretório em tempo real de salas públicas via MQTT over WSS (sem necessidade de backend).
 * Usa mensagens retidas + LWT (Last Will and Testament) para manter apenas salas ativas.
 */

const BROKER_URL = 'wss://broker.emqx.io:8084/mqtt';
const TOPIC_PREFIX = 'impar-2d-enyo-v1/rooms/';
const HEARTBEAT_MS = 20000;
const LOCAL_TTL_MS = 75000;
const CLOCK_STALE_MS = 180000;

function decodePayload(payload) {
  if (typeof payload === 'string') return payload;
  if (!payload) return '';
  if (typeof TextDecoder !== 'undefined') {
    return new TextDecoder('utf-8').decode(payload);
  }
  return String(payload);
}

export class RoomDirectoryManager {
  constructor() {
    this.browseClient = null;
    this.hostClient = null;
    this.rooms = new Map();
    this.hostedRoom = null;
    this.heartbeatTimer = null;
    this.pruneTimer = null;
    this.onRoomsChange = null;
    this.onStatusChange = null;
  }

  startBrowsing({ onRoomsChange, onStatusChange } = {}) {
    this.stopBrowsing();
    this.onRoomsChange = onRoomsChange || null;
    this.onStatusChange = onStatusChange || null;
    this.rooms.clear();

    if (!window.mqtt || typeof window.mqtt.connect !== 'function') {
      this.onStatusChange?.('offline', 'Serviço de lista indisponível.');
      this._emitRooms();
      return;
    }

    this.onStatusChange?.('connecting', 'Buscando salas abertas...');

    const clientId = `impar_br_${Math.random().toString(36).slice(2, 10)}`;
    const client = window.mqtt.connect(BROKER_URL, {
      clientId,
      clean: true,
      keepalive: 25,
      reconnectPeriod: 4000,
      connectTimeout: 8000,
    });
    this.browseClient = client;

    client.on('connect', () => {
      if (this.browseClient !== client) return;
      client.subscribe(`${TOPIC_PREFIX}+`, { qos: 0 }, (err) => {
        if (this.browseClient !== client) return;
        if (err) {
          this.onStatusChange?.('offline', 'Erro ao buscar salas.');
          return;
        }
        this.onStatusChange?.('connected', 'Conectado');
        this._emitRooms();
      });
    });

    client.on('message', (topic, payload) => {
      if (this.browseClient !== client) return;
      if (!topic || !topic.startsWith(TOPIC_PREFIX)) return;

      const roomCode = topic.slice(TOPIC_PREFIX.length);
      if (!roomCode || roomCode.includes('/')) return;

      const text = decodePayload(payload).trim();
      if (!text) {
        if (this.rooms.delete(roomCode)) {
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
          if (this.rooms.delete(roomCode)) {
            this._emitRooms();
          }
          return;
        }

        if (typeof data.updatedAt === 'number') {
          const age = Date.now() - data.updatedAt;
          if (age > CLOCK_STALE_MS && age < 86400000) {
            if (this.rooms.delete(roomCode)) {
              this._emitRooms();
            }
            return;
          }
        }

        this.rooms.set(roomCode, { data, receivedAt: Date.now() });
        this._emitRooms();
      } catch {
        // Ignora payloads malformados
      }
    });

    client.on('reconnect', () => {
      if (this.browseClient !== client) return;
      this.onStatusChange?.('connecting', 'Reconectando...');
    });

    client.on('error', () => {
      if (this.browseClient !== client) return;
      this.onStatusChange?.('offline', 'Não foi possível conectar à lista pública agora.');
    });

    client.on('offline', () => {
      if (this.browseClient !== client) return;
      this.onStatusChange?.('offline', 'Sem conexão com a lista pública.');
    });

    this.pruneTimer = setInterval(() => {
      const now = Date.now();
      let changed = false;
      for (const [code, entry] of this.rooms.entries()) {
        if (now - entry.receivedAt > LOCAL_TTL_MS) {
          this.rooms.delete(code);
          changed = true;
        }
      }
      if (changed) this._emitRooms();
    }, 10000);
  }

  stopBrowsing() {
    if (this.pruneTimer) {
      clearInterval(this.pruneTimer);
      this.pruneTimer = null;
    }
    if (this.browseClient) {
      try {
        this.browseClient.end(true);
      } catch {}
      this.browseClient = null;
    }
    this.onRoomsChange = null;
    this.onStatusChange = null;
  }

  refresh() {
    const now = Date.now();
    for (const [code, entry] of this.rooms.entries()) {
      if (now - entry.receivedAt > LOCAL_TTL_MS) {
        this.rooms.delete(code);
      }
    }
    this._emitRooms();

    if (this.browseClient && this.browseClient.connected) {
      this.onStatusChange?.('connecting', 'Atualizando...');
      const topic = `${TOPIC_PREFIX}+`;
      this.browseClient.unsubscribe(topic, () => {
        if (!this.browseClient) return;
        this.browseClient.subscribe(topic, { qos: 0 }, (err) => {
          if (!this.browseClient) return;
          if (err) {
            this.onStatusChange?.('offline', 'Erro ao atualizar lista.');
          } else {
            this.onStatusChange?.('connected', 'Conectado');
          }
        });
      });
    } else if (this.onRoomsChange || this.onStatusChange) {
      this.startBrowsing({
        onRoomsChange: this.onRoomsChange,
        onStatusChange: this.onStatusChange,
      });
    }
  }

  removeLocalRoom(roomCode) {
    if (!roomCode) return;
    if (this.rooms.delete(roomCode)) {
      this._emitRooms();
    }
  }

  publishRoom(roomInfo) {
    this.unpublishRoom();

    if (!window.mqtt || typeof window.mqtt.connect !== 'function') {
      return Promise.resolve({ ok: false, error: 'MQTT indisponível.' });
    }

    const now = Date.now();
    this.hostedRoom = {
      roomCode: roomInfo.roomCode,
      roomName: roomInfo.roomName,
      hostName: roomInfo.hostName,
      players: roomInfo.players ?? 1,
      maxPlayers: roomInfo.maxPlayers,
      hasPassword: Boolean(roomInfo.hasPassword),
      visibility: roomInfo.visibility || 'public',
      status: 'waiting',
      createdAt: now,
      updatedAt: now,
    };

    const topic = `${TOPIC_PREFIX}${this.hostedRoom.roomCode}`;
    const clientId = `impar_hs_${Math.random().toString(36).slice(2, 10)}`;

    return new Promise((resolve) => {
      let settled = false;
      const finish = (res) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        resolve(res);
      };

      const client = window.mqtt.connect(BROKER_URL, {
        clientId,
        clean: true,
        keepalive: 20,
        reconnectPeriod: 4000,
        connectTimeout: 4500,
        will: {
          topic,
          payload: '',
          qos: 0,
          retain: true,
        },
      });
      this.hostClient = client;

      const timeout = setTimeout(() => {
        if (!settled) {
          if (this.hostClient === client) {
            try {
              client.end(true);
            } catch {}
            this.hostClient = null;
          }
          finish({ ok: false, error: 'Tempo esgotado ao publicar no diretório.' });
        }
      }, 4500);

      client.on('connect', () => {
        if (this.hostClient !== client || !this.hostedRoom) {
          finish({ ok: false });
          return;
        }
        this.hostedRoom.updatedAt = Date.now();
        client.publish(topic, JSON.stringify(this.hostedRoom), { retain: true, qos: 0 });

        if (!this.heartbeatTimer) {
          this.heartbeatTimer = setInterval(() => {
            if (this.hostClient === client && client.connected && this.hostedRoom) {
              this.hostedRoom.updatedAt = Date.now();
              client.publish(topic, JSON.stringify(this.hostedRoom), { retain: true, qos: 0 });
            }
          }, HEARTBEAT_MS);
        }

        finish({ ok: true });
      });

      client.on('error', () => {
        if (!settled) {
          if (this.hostClient === client) {
            try {
              client.end(true);
            } catch {}
            this.hostClient = null;
          }
          finish({ ok: false, error: 'Falha ao publicar sala no diretório.' });
        }
      });
    });
  }

  updateRoomPlayers(count) {
    if (!this.hostedRoom) return;
    this.hostedRoom.players = count;
    this.hostedRoom.updatedAt = Date.now();
    if (this.hostClient && this.hostClient.connected) {
      const topic = `${TOPIC_PREFIX}${this.hostedRoom.roomCode}`;
      this.hostClient.publish(topic, JSON.stringify(this.hostedRoom), { retain: true, qos: 0 });
    }
  }

  unpublishRoom() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }

    const client = this.hostClient;
    const room = this.hostedRoom;
    this.hostClient = null;
    this.hostedRoom = null;

    if (client && room) {
      const topic = `${TOPIC_PREFIX}${room.roomCode}`;
      try {
        if (client.connected) {
          client.publish(topic, '', { retain: true, qos: 0 });
          setTimeout(() => {
            try {
              client.end(false);
            } catch {}
          }, 150);
        } else {
          client.end(true);
        }
      } catch {}
    }
  }

  _emitRooms() {
    const list = [];
    for (const entry of this.rooms.values()) {
      if (entry.data && entry.data.status === 'waiting') {
        list.push(entry.data);
      }
    }
    list.sort((a, b) => {
      const aOpen = (a.players || 1) < a.maxPlayers ? 0 : 1;
      const bOpen = (b.players || 1) < b.maxPlayers ? 0 : 1;
      if (aOpen !== bOpen) return aOpen - bOpen;
      return (b.createdAt || 0) - (a.createdAt || 0);
    });
    this.onRoomsChange?.(list);
  }
}
