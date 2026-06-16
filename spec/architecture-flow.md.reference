# Architecture Flow: Agent → Slots → Coordinator → Modes

How a prompt travels through the Swarm Matrix stack, and how the serving layer
(agents grouped into model-server slots) relates to the orchestration layer
(coordinator modes).

## 1. Serving topology — agents grouped into slots

The proxy reads `swarm-config.json`, groups agents **by model path only** (the
`server_group` field is just a UI label for llama backends), and spawns one
inference server per model. Each server runs `--parallel N` continuous-batching
slots, where `N` = the number of agents in that group (unless overridden). The
KV per slot is `ctx_cap ÷ slots`.

```
swarm-config.json (13 agents)            PROXY  :3002
 each agent: model, context,      ──▶  POST /api/configure
 max_tokens, ctx_cap                    groups agents by MODEL PATH only
                                        (server_group is a UI label) and
                                        spawns one server per model:
                                        slots = --parallel = #agents in group
                                        per-slot KV = ctx_cap ÷ slots
        │
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│  llama-server / mlx processes  (one model each, N continuous-batch slots)│
│                                                                          │
│  :8085  Meta-Llama-3.1-8B    x5  [database foreman frontend synthesis    │
│         ctx_cap 15360 → 3072/slot                              tester]   │
│         ┌slot0┐┌slot1┐┌slot2┐┌slot3┐┌slot4┐                              │
│  :8086  qwen2.5-coder-7b     x4  [architect debugger optimizer programmer]│
│  :8084  gemma-2-9b           x2  [reviewer security]                     │
│  :8087  gemma-2-2b           x1  [scout]                                 │
│  :8083  MLX Llama-3.2-1B     x1  [mlx-scout]                             │
└───────────────────────────────────────────────────────────────────────┘
        ▲  each agent → a slot on its model's server (agents share a server)
```

Ports above reflect a representative configure; the exact port assignment is
chosen by the proxy at `/api/configure` time.

## 2. Request flow — mode → coordinator → agents → slots → back

```
            client (browser / curl)
                 │  POST /api/architect/stream {prompt}
                 ▼
        ┌─────────────────────────  COORDINATOR  :8000  ─────────────────────────┐
        │  active mode  (POST /api/modes/active)                                  │
        │                                                                         │
        │   ┌── FLAT ──────── broadcast → all agents, no reducer                  │
        │   ├── PIPELINE ──── A → B → C (each gets prev output) → synthesizer      │
        │   ├── ROUTER ────── classifier(foreman) picks subset → broadcast subset  │
        │   └── CASCADE ───── MoA: broadcast proposers ║ then synthesizer reduces  │
        │                                                                         │
        │   stream_parallel_agents():  one std::thread per agent                  │
        │   ┌──────────┬──────────┬──────────┬─────── … ──────────┐               │
        │   ▼          ▼          ▼          ▼                    ▼               │
        │  agent    agent      agent      agent                agent             │
        │ thread    thread     thread     thread               thread            │
        │   │          │          │          │                    │               │
        │   │  stream_agent → POST /v1/chat/completions (stream)  │               │
        │   │  ⏱ per-agent deadline (MATRIX_CASCADE_AGENT_         │               │
        │   │     DEADLINE_SECS=90): cancel a straggler, keep      │               │
        │   │     partial output, release the join() barrier       │               │
        └───┼──────────┼──────────┼──────────┼────────────────────┼───────────────┘
            ▼          ▼          ▼          ▼                    ▼
          :8086      :8085      :8085      :8084                :8087   ← server slots
         (slot)     (slot)     (slot)     (slot)               (slot)   generate tokens
            │          │          │          │                    │
            └──────────┴────── on_chunk ─────┴────────────────────┘
                              │  enqueue frame (no blocking write)
                              ▼
                    ┌───────────────────────────┐
                    │  event queue (std::deque)  │  ← DECOUPLED so the network
                    └───────────────────────────┘     write never backpressures
                              │                        the llama read-loops
                              ▼  single writer thread
                    sink.write → SSE → client
                    events: token · agent_done · synthesis_start · metrics · done
```

## 3. Cascade barrier + synthesis

Cascade is single-layer Mixture-of-Agents: proposers broadcast in parallel, then
a synthesizer reduces their outputs into one answer. The `join()` barrier waits
for every proposer, so the per-agent deadline is what guarantees a slow/runaway
proposer can't stall the run.

```
 proposers run in parallel ───────────────┐
   database ─ done                         │
   foreman  ─ done                         │  threads.join()  ← barrier:
   architect─ done                         ├─ waits for ALL proposers
   security ─ cut at 90s (deadline) ───────┤   (deadline guarantees release)
   programmer─ cut at 90s (deadline) ──────┘
                       │
                       ▼
            build_cascade_synthesis_prompt()
            budget = synthesis.context − max_tokens − 768
                   = 3072 − 1024 − 768 = 1280 tok   (was floored at 256)
                       │
                       ▼
              synthesis agent  (:8085 slot)
              streams ONE consolidated answer → client
```

## Key relationships

- **Agent ↔ slot is many-to-one**, keyed by *model path* — agents sharing a
  GGUF share one server's slots; you cannot split them apart, and each slot gets
  `ctx_cap ÷ parallel` tokens of KV.
- **Coordinator ↔ agent is one-thread-per-agent**, bounded by the per-agent
  deadline so the cascade `join()` barrier always releases.
- **Mode is the orchestration policy** over that same fan-out: flat = broadcast,
  pipeline = chain, router = subset, cascade = broadcast + synthesize.

## Relevant knobs

| Knob | Where | Effect |
|------|-------|--------|
| `context`, `ctx_cap` | `swarm-config.json` per agent | per-slot KV (`ctx_cap ÷ parallel`); synthesizer prompt budget |
| `max_tokens` | `swarm-config.json` per agent | per-request generation cap (all modes) |
| `MATRIX_CASCADE_AGENT_DEADLINE_SECS` | `scripts/matrix-env.sh` | per-agent cascade/flat wall-clock budget (default 90, 0 = off) |
| active mode | `POST /api/modes/active` | flat \| pipeline \| router \| cascade |

## Source map

| Concern | File |
|---------|------|
| Agent→model grouping, port/slot assignment | `yyyyy/cpp_core/src/proxy_configure_ports_assign.h`, `proxy_configure_ports_build.cpp` |
| Server spawn args (`--parallel`, ctx, ctx_cap) | `yyyyy/cpp_core/src/proxy_configure_spawn_args.h` |
| SSE stream entry, decoupled writer | `yyyyy/cpp_core/src/coordinator_routes_architect_stream.cpp` |
| Parallel broadcast + per-agent deadline | `yyyyy/cpp_core/src/coordinator_routes_architect_stream_modes.cpp` |
| Per-agent HTTP stream + cancel | `yyyyy/cpp_core/src/agent_stream_llama.h` |
| Synthesis prompt budget | `yyyyy/cpp_core/src/synthesis_budget.cpp`, `synthesis_budget_assemble.cpp` |
| Mode registration | `yyyyy/cpp_core/src/coordinator_routes_modes.cpp` |
