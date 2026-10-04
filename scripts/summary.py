import json, glob
for f in sorted(glob.glob('results/bench/*.json')):
    d = json.load(open(f))
    print(d['tag'], d['pipeline_wall_sec'], d['output_rows'], d['row_hash'], d['mean_cpu_pct'], d['peak_mem_delta_gib'])
