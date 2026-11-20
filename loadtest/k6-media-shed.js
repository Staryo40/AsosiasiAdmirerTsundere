import http from "k6/http";
import { check } from "k6";

const base = __ENV.API_BASE || "http://192.168.56.10:30080";

export const options = {
  vus: Number(__ENV.VUS || 500),
  duration: "60s",
};

export function setup() {
  const fieldSecret = __ENV.FIELD_SECRET;
  const mediaSecret = __ENV.MEDIA_SECRET;
  if (!fieldSecret || !mediaSecret) {
    throw new Error("set FIELD_SECRET and MEDIA_SECRET");
  }
  const field = http.post(`${base}/auth/login`, JSON.stringify({ client_id: "field-team", client_secret: fieldSecret }), {
    headers: { "Content-Type": "application/json" },
  });
  const media = http.post(`${base}/auth/login`, JSON.stringify({ client_id: "media", client_secret: mediaSecret }), {
    headers: { "Content-Type": "application/json" },
  });
  return { field: field.json().access_token, media: media.json().access_token };
}

export default function (data) {
  const mediaRes = http.get(`${base}/v1/hazards?limit=5`, {
    headers: { Authorization: `Bearer ${data.media}` },
  });
  const fieldRes = http.get(`${base}/v1/hazards?limit=5`, {
    headers: { Authorization: `Bearer ${data.field}` },
  });
  check(mediaRes, { "media may shed 429": (r) => r.status === 200 || r.status === 429 });
  check(fieldRes, { "field stays 200": (r) => r.status === 200 });
}