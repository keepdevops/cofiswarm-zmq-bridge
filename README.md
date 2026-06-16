# cofiswarm-zmq-bridge

Cofiswarm component: `zmq-bridge`.

- Layout: [REPO-STANDARD-LAYOUT](https://github.com/keepdevops/cofiswarmdev/blob/main/docs/REPO-STANDARD-LAYOUT.md)
- Migration: [MIGRATION-SPRINTS](https://github.com/keepdevops/cofiswarmdev/blob/main/docs/MIGRATION-SPRINTS.md)

## FHS paths

| Path | Purpose |
|------|---------|
| `/etc/cofiswarm/zmq-bridge/` | config |
| `/var/lib/cofiswarm/zmq-bridge/` | state |
| `/var/log/cofiswarm/zmq-bridge/` | logs |

## Test

```bash
./test/scripts/assert-layout.sh zmq-bridge
```
