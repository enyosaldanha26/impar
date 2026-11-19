/**
 * Configuração compartilhada de brokers MQTT-over-WSS para descoberta de salas e partidas online.
 * Inclui porta padrão HTTPS 443 para funcionar atrás de firewalls e redes móveis CGNAT.
 */

export const TOPIC_ROOT = 'impar-2d-enyo-v2';
export const ROOMS_PREFIX = `${TOPIC_ROOT}/rooms/`;
export const DISCOVERY_TOPIC = `${TOPIC_ROOT}/discovery`;
export const ROOM_CHANNEL_PREFIX = `${TOPIC_ROOT}/r/`;

export const MQTT_BROKERS = [
  {
    id: 'emqx',
    label: 'EMQX',
    url: 'wss://broker.emqx.io:8084/mqtt',
    options: {},
  },
  {
    id: 'shiftr',
    label: 'Porta 443',
    url: 'wss://public.cloud.shiftr.io:443',
    options: { username: 'public', password: 'public' },
  },
  {
    id: 'mosquitto',
    label: 'Mosquitto',
    url: 'wss://test.mosquitto.org:8081/mqtt',
    options: {},
  },
];

export const PRIMARY_BROKERS = MQTT_BROKERS.slice(0, 2);
export const FALLBACK_BROKER = MQTT_BROKERS[2];

export function getBrokerById(id) {
  return MQTT_BROKERS.find((b) => b.id === id) || null;
}

export function decodeMqttPayload(payload) {
  if (typeof payload === 'string') return payload;
  if (!payload) return '';
  if (typeof TextDecoder !== 'undefined') {
    return new TextDecoder('utf-8').decode(payload);
  }
  return String(payload);
}

export function randomClientId(prefix = 'c') {
  const rand = Math.random().toString(36).slice(2, 10);
  return `impar_${prefix}_${rand}`;
}
