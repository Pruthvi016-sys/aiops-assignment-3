"""Regenerates report figures (bars, CPU/memory timelines, dedup chart) from results/bench/*.json and *_usage.csv.
Run from the repo root: python scripts/make_figures.py   (writes PNGs into report/)"""
import json,glob,statistics as st,pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
J={}
for f in glob.glob("results/bench/*.json"):
    d=json.load(open(f)); J[f.split("/")[-1][:-5]]=d
def g(prefix):
    return [d for k,d in sorted(J.items()) if k.startswith(prefix) and not k.endswith("_usage")]
def w(prefix): return [d["pipeline_wall_sec"] for d in g(prefix)]
groups={"spark_py11":"spark_python_n11_run","spark_nat11":"spark_native_n11_run","spark_off11":"spark_python_n11_dedupoff",
"ray_py11":"ray_python_n11_dedupperfile","ray_nat11":"ray_native_n11_dedupperfile","ray_off11":"ray_python_n11_dedupoff"}
for k,p in groups.items():
    v=w(p); print(k,len(v),round(st.mean(v),1),round(st.stdev(v),1) if len(v)>1 else 0, v)
# ray_py11 includes run5 (n11 perfile) fine
for t in ["spark_python_full_run1","ray_python_full_dedupperfile_run1","spark_python_n11_run1","ray_python_n11_dedupon_run1","ray_python_n11_deduphash_run2"]:
    d=J[t]; print(t,{k:d[k] for k in ["pipeline_wall_sec","mean_cpu_pct","peak_cpu_pct","peak_mem_delta_gib","peak_mem_gib","baseline_mem_gib"]})
for t in ["spark_python_n11_run1","spark_native_n11_run1","ray_python_n11_dedupperfile_run1","ray_native_n11_dedupperfile_run1","ray_python_n11_dedupoff_run1","spark_python_n11_dedupoff_run1"]:
    d=J[t]; print(t,d["mean_cpu_pct"],d["peak_cpu_pct"],d["peak_mem_delta_gib"])
# timeline plots
def tl(t): return pd.read_csv(f"results/bench/{t}.csv" if t.endswith("_usage") else f"results/bench/{t}_usage.csv")
fig,ax=plt.subplots(1,2,figsize=(10,3.4))
for t,l,c in [("spark_python_full_run1","Spark","tab:orange"),("ray_python_full_dedupperfile_run1","Ray","tab:blue")]:
    d=tl(t); ax[0].plot(d.t_sec,d.cpu_pct,label=l,color=c,lw=1); ax[1].plot(d.t_sec,d.mem_used_gib,label=l,color=c,lw=1)
ax[0].set_title("System CPU % (full, 44 files)");ax[1].set_title("System memory used, GiB (full, 44 files)")
for a in ax: a.set_xlabel("seconds"); a.legend(); a.grid(alpha=.3)
plt.tight_layout(); plt.savefig("report/timeline_full.png",dpi=160); plt.close()
fig,ax=plt.subplots(1,2,figsize=(10,3.4))
for t,l,c in [("spark_python_n11_run1","Spark","tab:orange"),("ray_python_n11_dedupperfile_run1","Ray","tab:blue")]:
    d=tl(t); ax[0].plot(d.t_sec,d.cpu_pct,label=l,color=c,lw=1); ax[1].plot(d.t_sec,d.mem_used_gib,label=l,color=c,lw=1)
ax[0].set_title("System CPU % (11 files)");ax[1].set_title("System memory used, GiB (11 files)")
for a in ax: a.set_xlabel("seconds"); a.legend(); a.grid(alpha=.3)
plt.tight_layout(); plt.savefig("report/timeline_n11.png",dpi=160); plt.close()
# bar: full + 11 files
import numpy as np
fig,ax=plt.subplots(1,2,figsize=(10,3.4))
ax[0].bar(["Spark","Ray"],[371.89,182.10],color=["tab:orange","tab:blue"]);ax[0].set_title("Full dataset (44 files), wall s")
for i,v in enumerate([371.89,182.10]): ax[0].text(i,v+4,f"{v:.1f}",ha="center")
labs=["Spark\npython","Spark\nnative","Ray\npython","Ray\nnative"]
vals=[st.mean(w(groups[k])) for k in ["spark_py11","spark_nat11","ray_py11","ray_nat11"]]
ax[1].bar(labs,vals,color=["tab:orange","moccasin","tab:blue","lightsteelblue"]);ax[1].set_title("UDF overhead, 11 files, wall s (mean)")
for i,v in enumerate(vals): ax[1].text(i,v+1.5,f"{v:.1f}",ha="center")
plt.tight_layout(); plt.savefig("report/bars.png",dpi=160); plt.close()
# dedup finding
fig,ax=plt.subplots(figsize=(6,3.2))
n=["Ray dedup off","Ray per-file\n(no shuffle)","Ray global\n(groupby cols)","Ray global\n(hash-part.)"]
v=[st.mean(w(groups["ray_off11"])),st.mean(w(groups["ray_py11"])),J["ray_python_n11_dedupon_run1"]["pipeline_wall_sec"],J["ray_python_n11_deduphash_run2"]["pipeline_wall_sec"]]
ax.bar(n,v,color=["gray","tab:blue","tab:red","tab:red"]); 
for i,x in enumerate(v): ax.text(i,x+6,f"{x:.0f}",ha="center")
ax.set_title("Ray dedup strategy, 11 files, wall s"); plt.tight_layout(); plt.savefig("report/dedup.png",dpi=160)
