# Elastic Resource Manager

A lightweight HPC scheduler that launches and elastically scales MPI-style and Workflow jobs across a static pool of nodes.

- FCFS queueing with support for elastic expand/shrink
- Optional backfilling
- PRRTE DVM management (prte/prun)
- Hostfile-based node allocation
- JSON job file watcher

## Requirements

- Linux
- Python 3.9+
- PRRTE/PMIx installed and in PATH (prte, prun)
- Your MPI app(s)
- Optional: Parsl inside jobs (set PARSL_RUN_DIR per job to avoid collisions)

## Quick start

```bash
# 1) Create and activate a virtualenv
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip

# 2) Install this project (editable if you plan to modify)
pip install -e .

# 3) Prepare inputs in your working dir (e.g., tests/)
# hostfile: one hostname per line
cat > hostfile <<'EOF'
st33
st34
st35
st36
st37
EOF

# jobs.json: list your jobs
cat > jobs.json <<'EOF'
{
  "jobs": [
    {,
      "command": "python /path/to/app.py --nodelist NODES --job_id ID --nnodes NNODES --wt 00:30:00",
      "min_nodes": 2,
      "max_nodes": 2,
      "elastic": false,
      "walltime": "00:30:00"
    },
    {
      "command": "python /path/to/app.py --nodelist NODES --job_id ID --nnodes NNODES --wt 00:30:00",
      "min_nodes": 2,
      "max_nodes": 3,
      "elastic": true,
      "walltime": "00:30:00"
    },
    {
      "command": "python /path/to/app.py --nodelist NODES --job_id ID --nnodes NNODES --wt 00:10:00",
      "min_nodes": 2,
      "max_nodes": 2,
      "elastic": false,
      "walltime": "00:10:00"
    }
  ]
}
EOF

# policy.json: initially empty or minimal
echo '{ "jobs": [] }' > policy.json

# 4) Run
python -m elastic_scheduler.main -H hostfile -j jobs.json -N 5 -p policy.json -s 0
```

Flags:
- -H: path to hostfile
- -j: path to jobs.json
- -N: total nodes to manage (optional if hostfile is authoritative)
- -p: policy file (elastic decisions written here)
- -s: schedule type (0=min_nodes FCFS, 1=max_nodes)

## How it works

- NodeManager reads/writes a hostfile for free/allocated nodes and manages a PRRTE DVM.
- JobMonitor watches jobs.json and enqueues new jobs.
- Scheduler thread:
  - Starts the head job if enough nodes are available.
  - If not enough nodes:
    - Attempts elastic expansion for running elastic-capable jobs.
    - Optionally backfills a smaller job that can finish before the head job’s reservation (EASY).
  - When jobs finish, nodes are freed and the scheduler is notified.

## Job command adaptation

The scheduler enriches your command at launch time:
- prun: adds DVM URI, host mapping, binding, and -n (#nodes)
<!-- - --nodelist/--job_id/--nnodes/--wt: values filled from the allocation
- EXAMOL-style envs can be exported: NUM_NODES_EXAMOL, NODES_EXAMOL, JOB_ID_EXAMOL, WALLTIME_EXAMOL -->

Tip: use placeholders in your commands (e.g., NODES/ID/NNODES) or standard flags (e.g., --nodelist).

## Elastic policy

Policy entries are appended to policy.json, e.g.:

```json
{
  "jobs": [
    { "id": "2", "scale": "expand", "num_nodes": 1, "nodes": "st37", "start_after": 0 },
    { "id": "2", "scale": "shrink", "num_nodes": 1, "nodes": "st37", "start_after": 0 }
  ]
}
```

Your app or external orchestrator can act on these entries to reconfigure running jobs.

## Logging

- Default: INFO
- Enable DEBUG by editing main to set logging.basicConfig(level=logging.DEBUG) or by adding a CLI flag if provided.
- Useful traces:
  - “Scheduler waiting/woke up”
  - “Scheduling attempt”
  - “Nodes Allocated/Freed”
  - “[Elastic Scaling] …”

## Troubleshooting

- Parsl run dir collisions / FileExistsError:
  - Export a unique PARSL_RUN_DIR per job in the child environment.

- resource_tracker leaked semaphores:
  - Suppress with PYTHONWARNINGS=ignore:resource_tracker:UserWarning or upgrade Python/multiprocessing.

- PRRTE cleanup:
  - If orphaned prte/prun processes remain, kill them:
    pkill -u "$USER" -f 'prsl|prte|parsl: HTEX interchange' (use with care).