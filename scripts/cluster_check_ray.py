import ray, ray.data
ray.init(address="auto")
print("nodes alive:", sum(n["Alive"] for n in ray.nodes()), "| CPUs:", ray.cluster_resources().get("CPU"))
ds = ray.data.range(5_000_000).map_batches(lambda b: {"y": b["id"] * 2}, batch_format="numpy")
print("RAY SUM:", ds.sum("y"))
input("Open http://127.0.0.1:8265 -> Cluster tab, take the screenshot, then press Enter...")
