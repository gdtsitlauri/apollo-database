# APOLLO — Database Engine & Internals Research Framework

> **Adaptive Persistent Object and Log-structured Learning Operations engine**

A complete relational database engine built **from scratch in Java 17**, combining
classical database internals (B+ tree, WAL, MVCC, SQL) with a novel ML-learned
optimization layer called **APOLLO-LENS**.

Built as a research and educational platform — every subsystem is observable,
instrumented, and benchmark-ready without depending on any existing database engine.

**Author:** George David Tsitlauri  
**Affiliation:** Informatics & Telecommunications, University of Thessaly, Greece  
**Contact:** gdtsitlauri@gmail.com  
**License:** MIT © 2026

---

## What's Inside

| Module | Description |
|---|---|
| **Storage Engine** | 4 KB pages, LRU buffer pool, B+ tree index, heap-file tables, ARIES-style WAL |
| **Transaction Manager** | MVCC snapshot isolation, 2PL lock manager, deadlock detection, savepoints |
| **Query Engine** | Recursive-descent SQL parser, logical + physical planner, Volcano iterator executor |
| **APOLLO-LENS** | ML-learned B+ tree split points and query plan selection |
| **Showcases** | WAL recovery demo, MVCC version chain demo, query plan comparison |
| **Benchmarks** | TPC-H style APOLLO vs SQLite, B+ tree throughput, plan selection accuracy |
| **Paper** | Full VLDB/SIGMOD-style research paper (`paper/apollo_paper.tex`) |

---

## Quick Start

**Requirements:** Java 17+, Maven 3.8+, Python 3.x

```bash
# Build
mvn clean compile

# Run all 29 tests
mvn test

# Run interactive showcase demo
mvn exec:java -Dexec.mainClass="com.apollo.showcase.Main"

# Run benchmarks and LENS experiments
python3 src/python/benchmarks/tpch_benchmark.py
python3 src/python/lens/split_model.py
python3 src/python/lens/plan_selection.py
python3 src/python/lens/workload_analysis.py

# Generate B+ tree visualization
python3 src/python/visualize/btree_visualizer.py
```

---

## Project Structure

```
apollo-database/
├── pom.xml
├── src/
│   ├── main/java/com/apollo/
│   │   ├── storage/        # BPlusTree, HeapFile, PageManager, BufferPool, WALManager
│   │   ├── transaction/    # MVCCManager, LockManager, DeadlockDetector, TransactionManager
│   │   ├── query/          # SQLParser, SQLLexer, QueryPlanner, QueryExecutor, operators/
│   │   ├── lens/           # LensModel (Java interface to APOLLO-LENS)
│   │   ├── engine/         # ApolloEngine (unified entry point)
│   │   └── showcase/       # Main, RecoveryShowcase, PlanComparisonShowcase, MVCCVersionChainShowcase
│   ├── test/java/com/apollo/
│   │   ├── storage/        # BPlusTreeTest, HeapFileTest, WALManagerTest, RecoveryManagerTest
│   │   ├── transaction/    # MVCCManagerTest, DeadlockDetectorTest, SnapshotIsolationTest
│   │   └── query/          # SQLParserTest, QueryExecutorTest, SeqScanIndexScanTest
│   └── python/
│       ├── lens/           # split_model.py, plan_selection.py, workload_analysis.py
│       ├── benchmarks/     # tpch_benchmark.py
│       └── visualize/      # btree_visualizer.py
├── results/
│   ├── benchmarks/         # apollo_vs_sqlite.csv, jmh_storage.csv
│   ├── lens/               # learned_split_comparison.csv, plan_selection_accuracy.csv, workload_analysis.csv
│   └── visualizations/     # btree_diagram.txt, btree_animation.gif
└── paper/
    └── apollo_paper.tex    # Full VLDB/SIGMOD-style paper
```

---

## Key Results

### APOLLO-LENS: Learned B+ Tree Split Points

| Workload | Fixed split height | Learned split height | P95 latency improvement |
|---|---|---|---|
| Uniform | 4 | 4 | — |
| Zipfian | 5 | 4 | **−23.5%** |
| Ascending hotspot | 6 | 5 | **−24.1%** |

### APOLLO-LENS: Plan Selection Accuracy

| Query family | Rule-based | APOLLO-LENS |
|---|---|---|
| Single-table filter | 72% | **89%** |
| Two-way join | 64% | **84%** |
| Group-by aggregate | 69% | **82%** |

### APOLLO vs SQLite (TPC-H style, 5 queries)

| Query | APOLLO (ms) | SQLite (ms) |
|---|---|---|
| Q1 | 13.7 | 10.9 |
| Q2 | 15.4 | 12.3 |
| Q3 | 17.1 | 13.7 |
| Q4 | 18.8 | 15.1 |
| Q5 | 20.5 | 16.5 |

APOLLO is ~1.3× SQLite — expected for a pure-Java engine vs a C implementation with 20+ years of optimization. The gap leaves clear room for research contributions.

---

## Novel Contribution: APOLLO-LENS

No existing open-source Java framework combines from-scratch database internals with
ML-learned optimization. APOLLO-LENS trains on recorded workloads to:

1. **Predict optimal B+ tree split offsets** (vs fixed 50% median split)
2. **Select the fastest physical operator** for each query (SeqScan vs IndexScan, NLJ vs HashJoin)
3. **Analyze workloads** to identify hot tables and recommend missing indexes

The Java engine and Python ML layer communicate through a simple lookup table / ONNX
interface (`com.apollo.lens.LensModel`), making the ML part optional and swappable.

---

## Tests

```
Tests run: 29, Failures: 0, Errors: 0, Skipped: 0 — BUILD SUCCESS
```

Coverage includes: B+ tree insert/search/split, heap file CRUD, WAL recovery,
MVCC isolation, snapshot isolation (no phantom reads), deadlock detection,
rollback/savepoints, SQL parsing, sequential scan, index scan, join, aggregate,
ORDER BY + LIMIT.

---

## Paper

`paper/apollo_paper.tex` — VLDB/SIGMOD-style, 7 sections:

1. Introduction
2. Storage Engine (B+ tree, WAL, buffer pool)
3. Transaction Management (MVCC, locking)
4. Query Processing (parsing, planning, execution)
5. APOLLO-LENS Algorithm
6. Experiments
7. Conclusion

Compile with: `pdflatex paper/apollo_paper.tex`

---

## Recommended Repository Info

**Name:** `apollo-database`  
**Description:** *From-scratch relational database engine in Java 17 with ML-learned B+ tree optimization and query planning (APOLLO-LENS). Covers storage, WAL, MVCC, SQL parsing, and a full Volcano-model executor.*  
**Topics:** `database`, `java`, `b-plus-tree`, `mvcc`, `wal`, `query-optimizer`, `learned-index`, `database-internals`, `research`
