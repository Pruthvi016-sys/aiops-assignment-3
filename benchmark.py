"""Benchmark harness: runs <framework>_clean.py, samples system-wide CPU%/memory every second
(what `top` shows), records wall-clock, verifies the output fingerprint, then deletes the output."""
import argparse, ast, csv, json, os, shutil, subprocess, sys, threading, time
import psutil
from compare_outputs import fingerprint

ap = argparse.ArgumentParser()
ap.add_argument("--framework", required=True, choices=["spark", "ray"])
ap.add_argument("--udf-mode", default="python", choices=["python", "native"])
ap.add_argument("--max-files", type=int, default=0, help="0 = all files")
ap.add_argument("--run", type=int, default=1)
ap.add_argument("--dedup", default=None, help="ray only: on|off|hash|twopass")
ap.add_argument("--keep", action="store_true", help="keep the output directory")
a = ap.parse_args()

scope = "full" if a.max_files == 0 else f"n{a.max_files}"
tag = f"{a.framework}_{a.udf_mode}_{scope}_run{a.run}"
if a.dedup:
    tag = f"{a.framework}_{a.udf_mode}_{scope}_dedup{a.dedup}_run{a.run}"
out = f"output/bench_{a.framework}"
shutil.rmtree(out, ignore_errors=True)
os.makedirs("results/bench", exist_ok=True)
cmd = [sys.executable, f"{a.framework}_clean.py", "--max-files", str(a.max_files),
       "--udf-mode", a.udf_mode, "--out", out]
if a.dedup:
    cmd += ["--dedup", a.dedup]

GIB = 2 ** 30
base_mem = psutil.virtual_memory().used / GIB
rows, stop = [], threading.Event()
t0 = time.time()


def sampler():
    psutil.cpu_percent(None)
    n = 0
    while not stop.wait(1.0):
        cpu, mem = psutil.cpu_percent(None), psutil.virtual_memory().used / GIB
        rows.append((round(time.time() - t0, 1), cpu, round(mem, 3)))
        n += 1
        if n % 30 == 0:
            print(f"  ... {n}s elapsed | cpu {cpu:.0f}% | mem {mem:.1f} GiB", flush=True)


th = threading.Thread(target=sampler, daemon=True)
th.start()
print(f"[{tag}] running: {' '.join(cmd)}", flush=True)
p = subprocess.run(cmd, capture_output=True, text=True)
stop.set(); th.join()
total = time.time() - t0

if p.returncode != 0:
    print("PIPELINE FAILED. Last lines of output:")
    print("\n".join((p.stdout + p.stderr).splitlines()[-30:]))
    sys.exit(1)

m = None
for line in p.stdout.splitlines():
    if line.startswith("METRICS:"):
        m = ast.literal_eval(line[len("METRICS:"):].strip())
if m is None:
    print("no METRICS line found"); print(p.stdout[-2000:]); sys.exit(1)

with open(f"results/bench/{tag}_usage.csv", "w", newline="") as fh:
    w = csv.writer(fh); w.writerow(["t_sec", "cpu_pct", "mem_used_gib"]); w.writerows(rows)

nrows, h, _, sums = fingerprint(out, per_col=False)       # verification: NOT timed
res = {"tag": tag, "framework": a.framework, "udf_mode": a.udf_mode, "files": m["files"],
       "pipeline_wall_sec": m["wall_clock_sec"], "process_total_sec": round(total, 2),
       "output_rows": m["output_rows"], "verified_rows": nrows, "row_hash": f"{h:016x}", "sums": sums,
       "peak_cpu_pct": max(r[1] for r in rows) if rows else None,
       "mean_cpu_pct": round(sum(r[1] for r in rows) / len(rows), 1) if rows else None,
       "baseline_mem_gib": round(base_mem, 2),
       "peak_mem_gib": max(r[2] for r in rows) if rows else None,
       "peak_mem_delta_gib": round(max(r[2] for r in rows) - base_mem, 2) if rows else None,
       "logical_cores": os.cpu_count()}
json.dump(res, open(f"results/bench/{tag}.json", "w"), indent=2)
if not a.keep:
    shutil.rmtree(out, ignore_errors=True)
print(json.dumps(res, indent=2))
print("ROWS CONSISTENT:", nrows == m["output_rows"])
