import http from "k6/http";
import { check, fail, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";

const NODES = [
  "http://raft-1:8080",
  "http://raft-2:8080",
  "http://raft-3:8080",
  "http://raft-4:8080",
  "http://raft-5:8080",
];

const FAILOVER = __ENV.FAILOVER === "1";

const opFailed = new Rate("op_failed");

const TIMEOUT = "3s";
const opDuration = new Trend("op_duration", true);

export const options = {
  scenarios: {
    heavy: {
      executor: "ramping-arrival-rate",
      startRate: 200,
      timeUnit: "1s",
      preAllocatedVUs: 200,
      maxVUs: 1000,
      stages: [
        { duration: "10s", target: 500 },
        { duration: "20s", target: 500 },
        { duration: "10s", target: 1000 },
        { duration: "20s", target: 1000 },
        { duration: "10s", target: 1500 },
        { duration: "20s", target: 1500 },
        { duration: "10s", target: 2000 },
        { duration: "20s", target: 2000 },
        { duration: "10s", target: 0 },
      ],
    },
  },
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
  thresholds: FAILOVER
    ? { op_failed: ["rate<0.05"] }
    : {
        op_failed: [
          { threshold: "rate<0.01", abortOnFail: true, delayAbortEval: "10s" },
        ],
        op_duration: [
          { threshold: "p(95)<100", abortOnFail: true, delayAbortEval: "10s" },
        ],
      },
};

export function setup() {
  for (const node of NODES) {
    const res = http.get(`${node}/leader`, { tags: { name: "leader" } });
    if (res.status === 200) {
      return { runId: Date.now() };
    }
  }
  fail("no leader elected yet");
}

function send(name, request) {
  const start = Date.now();
  let node = Math.floor(Math.random() * NODES.length);
  let res;

  for (let attempt = 0; attempt < NODES.length * 2; attempt++) {
    res = request(NODES[node]);
    if (res.status !== 0 && res.status !== 503) {
      break;
    }

    node = (node + 1) % NODES.length;
    sleep(0.1);
  }

  opDuration.add(Date.now() - start, { name });
  opFailed.add(res.status === 0 || res.status === 503, { name });

  return res;
}

export default function ({ runId }) {
  const key = `k6-${__VU}`;
  const value = `${__ITER}`;

  const write = {
    key,
    value,
    client_id: `k6-${runId}-${__VU}`,
    seq: __ITER + 1,
  };
  const set = send("set", (node) =>
    http.post(`${node}/set`, JSON.stringify(write), {
      tags: { name: "set" },
      timeout: TIMEOUT,
    }),
  );
  check(set, { "set is 201": (r) => r.status === 201 });

  const get = send("get", (node) =>
    http.get(`${node}/get?key=${key}`, {
      tags: { name: "get" },
      timeout: TIMEOUT,
    }),
  );
  check(get, {
    "get is 200": (r) => r.status === 200,
    "get returns the value just set": (r) =>
      r.status === 200 && r.json("value") === value,
  });
}
