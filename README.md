# parsimon-eval


### Path simulations are good enough?

cargo run --release -- help

### Step 1
- Pick a mix from Fig. 8.
- Run it with ns-3.

	```bash
	cargo run --release -- mixes your_mix.json ns3
	```

- Select a path (a source/destination pair) P.
- Query metrics from all flows along that path.
- That’s ground-truth G.
### Step 2
- Use the same mix from Fig. 8.
- For the workload, select only flows that traverse any link in P.
	- Construct a Parsimon SimNetwork.
	- Query for a particular path. (i.e., feed the src/dst pair to the SimNetwork and return a list of links (FlowChannel)
	- For each link, get the flows that traverse it.
- Run it again.
- Query metrics for all flows on P.
### Compare results to G.
