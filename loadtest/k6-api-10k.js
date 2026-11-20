import http from "k6/http";
import { check, sleep } from "k6";

const base = __ENV.API_BASE || "http://192.168.56.10:30080";
const clientID = __ENV.FIELD_ID || "field-team";
const clientSecret = __ENV.FIELD_SECRET || "";

export const options = {
  scenarios: {
    sustained_10k: {
      executor: "constant-vus",
      vus: Number(__ENV.VUS || 10000),
      duration: __ENV.DURATION || "120s",
    },
  },
  thresholds: {
    http_req_failed: [{ threshold: "rate<0.01", abortOnFail: false }],
  },
};

function login() {
  const res = http.post(
    `${base}/auth/login`,
    JSON.stringify({ client_id: clientID, client_secret: clientSecret }),
    { headers: { "Content-Type": "application/json" } }
  );
  check(res, { "login 200": (r) => r.status === 200 });
  return res.json().access_token;
}

export function setup() {
  if (!clientSecret) {
    throw new Error("set FIELD_SECRET");
  }
  return { token: login() };
}

export default function (data) {
  const res = http.get(`${base}/v1/hazards?limit=10`, {
    headers: { Authorization: `Bearer ${data.token}` },
  });
  check(res, { "list 200": (r) => r.status === 200 || r.status === 429 });
  sleep(0.01);
}