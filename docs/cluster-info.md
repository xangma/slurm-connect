# Cluster info

The **Get cluster info** button queries your login host to discover available Slurm partitions and their limits
(nodes, CPUs, memory, GPUs). It also tries to read the available modules list. The results are used to populate
the dropdowns and suggestions in the UI so you can pick valid values quickly. You can still type values manually,
and the fetched data is cached per login host to speed up the next load.

When cluster info is available, the UI shows warning hints if selected resources exceed partition limits or (when free-resource filtering is on) the currently free capacity. Warnings are advisory and do not block connecting.

When you click **Get cluster info**, Slurm Connect also checks for existing persistent sessions. If any are found,
an **Existing sessions** selector appears. Choosing one disables the resource fields and attaches the connection
to that allocation when you click **Connect**.

## Free-resource filtering (default on)
When enabled, the UI filters suggestions to **currently free** resources. This is computed from the same SSH
cluster-info call (no extra prompts) by combining:
- `sinfo -h -N -o "%n|%c|%t|%P|%G"` for per-node totals + state
- `squeue -h -o "%t|%C|%b|%N"` for running job usage

Bad nodes (down/drain/maint) are treated as unavailable, and pending jobs are ignored. The filter limits:
- **Partition list** to partitions with any free CPU/GPU capacity.
- **Nodes** to the count of nodes with free CPU.
- **CPUs per task** to the largest free CPU block on a single node.
- **GPU type/count** to free GPU/MIG slices currently available.

Get cluster info always collects the free-resource inputs in the single SSH call; the toggle just switches whether the UI filters suggestions.
Toggle in the UI or via `slurmConnect.filterFreeResources`.
