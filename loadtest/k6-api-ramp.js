// Autoscaling demo: load rises, holds, then stops so the HPAs in k8s/autoscaling scale up and back down.
import http from "k6/http";
import { check, sleep } from "k6";

const base = __ENV.API_BASE || "http://192.168.56.10:30080";
const clientID = __ENV.FIELD_ID || "field-team";
const clientSecret = __ENV.FIELD_SECRET || "";
const peak = Number(__ENV.PEAK_VUS || 2000);

export const options = {
  scenarios: {
    ramp: {
      executor: "ramping-vus",
      startVUs: 0,
      stages: [
        { duration: __ENV.RAMP_UP || "60s", target: peak },
        { duration: __ENV.HOLD || "120s", target: peak },
        { duration: __ENV.RAMP_DOWN || "30s", target: 0 },
        // Idle tail so the scale-down after the load shows up in the same run.
        { duration: __ENV.IDLE || "180s", target: 0 },
      ],
      gracefulRampDown: "10s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
  },
};

function login() {
  const res = http.post(
    `${base}/auth/login`,
    JSON.stringify({ client_id: clientID, client_secret: clientSecret }),
    { headers: { "Content-Type": "application/json" }, tags: { name: "login" } }
  );
  check(res, { "login ok": (r) => r.status === 200 });
  return res.json().access_token;
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
