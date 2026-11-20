import http from "k6/http";
import { check, sleep } from "k6";

const base = __ENV.API_BASE || "http://192.168.56.10:30080";
const clientID = __ENV.FIELD_ID || "field-team";
const clientSecret = __ENV.FIELD_SECRET || "";

export const options = {
  scenarios: {
    sustained: {
      executor: "constant-vus",
      vus: Number(__ENV.VUS || 200),
      duration: __ENV.DURATION || "120s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<5000"],
  },
};

function login() {
  const res = http.post(
    `${base}/auth/login`,
    JSON.stringify({ client_id: clientID, client_secret: clientSecret }),
    { headers: { "Content-Type": "application/json" }, tags: { name: "login" } }
  );
  check(res, { "login ok": (r) => r.status === 200 });
  const body = res.json();
  return body.access_token;
}

export function setup() {
  if (!clientSecret) {
    throw new Error("set FIELD_SECRET to BNPB_FIELD_CLIENT_SECRET");
  }
  return { token: login() };
}

export default function (data) {
  const res = http.get(`${base}/v1/hazards?limit=20`, {
    headers: { Authorization: `Bearer ${data.token}` },
    tags: { name: "list_hazards" },
  });
  check(res, { "list 200": (r) => r.status === 200 });
  sleep(0.05);
}