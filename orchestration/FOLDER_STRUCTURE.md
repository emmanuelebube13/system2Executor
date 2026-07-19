# FOLDER STRUCTURE (canonical — structure-coherency owns this)

> Every new path is approved against this tree. No back-reference into `scalable-brain/`.
> Status: **[x]** created in Phase 0 · **[ ]** to be created by the owning EXEC task.

```
system-2-execution-engine/
├── [x] MASTER_ORCHESTRATION_PROMPT.md / _v2.md   # seed
├── [x] ARCHITECTURE.md                            # Phase 0
├── [ ] RUNBOOK.md                                 # DevOps (§16), incl. Emergency Rollback
├── [ ] requirements.txt                           # pruned (incl. alembic)  — DevOps
├── [ ] Dockerfile / docker-compose.yml            # optional (§16)
├── [x] .gitignore
├── config/
│   └── [x] .env.system2.template                  # names only
├── [ ] migrations/                                # Alembic (EXEC-003/006)
├── orchestration/
│   ├── [x] progress_ledger.json / .md
│   ├── [x] DECISIONS_LOG.md
│   ├── [x] CHECKPOINT.md / checkpoint.json
│   ├── [x] FOLDER_STRUCTURE.md
│   ├── [x] AGENT_FLEET_TOPOLOGY.md
│   └── [x] CONTINUATION_PROMPT.md
├── docs/
│   ├── [x] README.md  00-dependencies-and-prerequisites.md
│   ├── [x] tasks/01..09-*.md
│   ├── [x] skills/*.md (8)
│   ├── [x] SYSTEM_BOUNDARY.md
│   ├── [x] STORAGE_AND_QUEUE_ABSTRACTION.md
│   ├── [x] FILE_MIGRATION_MANIFEST.md
│   └── [ ] SCHEMA_ADDITIONS.md                    # EXEC-006
├── [x] logs/                                       # git-ignored
├── src/system2/
│   ├── common/        [ ] db.py storage_backend.py queue_backend.py secrets.py logging.py
│   ├── artifact_sync/ [ ] downloader.py live_regime.py  [x] tests/
│   ├── execution/     [ ] pipeline.py outbound_consumer.py fill_producer.py safety.py validation.py lifecycle.py  [x] tests/
│   ├── broker/        [ ] oanda_adapter.py position_manager.py  [x] tests/
│   └── telemetry/     [ ] api/ frontend/  [x] tests/
├── [x] tests/                                       # cross-module drills
└── [x] state/                                       # git-ignored
        model-cache/  last_known_good/  queue/  offsets/  dlq/  control/
```

**Rules:** ask structure-coherency before inventing a path; additive + feature-flagged; no edits to
monolith originals; no import that reaches back into `scalable-brain/`.
