from collections import defaultdict

# Function to parse the flow info log
def parse_flow_info_log(flow_info_file):
    flows = {}
    with open(flow_info_file, 'r') as file:
        for line in file:
            parts = line.strip().split()
            flow_id = int(parts[0])
            flows[flow_id] = {
                'start_time': int(parts[6]),
                'end_time': int(parts[6]) + int(parts[7]),  # end_time = start_time + fct
                'links': set()
            }
    return flows

# Function to parse the link info log
def parse_link_info_log(link_info_file):
    link_flows = defaultdict(set)
    with open(link_info_file, 'r') as file:
        num_links = int(file.readline().strip())
        for _ in range(num_links):
            link_info = list(map(int, file.readline().strip().split(',')))
            flow_ids = list(map(int, file.readline().strip().split(',')[:-1]))
            assert link_info[3] == len(flow_ids)
            link = (link_info[1], link_info[2])
            link_flows[link].update(flow_ids)
    return link_flows

# Optimized function to assign links to flows
def assign_links_to_flows(flows, link_flows):
    for link, flow_ids in link_flows.items():
        for flow_id in flow_ids:
            if flow_id in flows:
                flows[flow_id]['links'].add(link)
    return flows

# Function to update bipartite graph and calculate busy periods
def update_bipartite_graph_and_calculate_busy_periods(flows):
    active_graphs = {}  # Dictionary to hold multiple bipartite graphs with graph_id as key
    busy_periods = []  # List to store busy periods
    events = []

    # Precompute events
    for flow_id, flow in flows.items():
        events.append((flow['start_time'], 'start', flow_id, flow['links']))
        events.append((flow['end_time'], 'end', flow_id, flow['links']))

    events.sort()

    link_to_graph = {}  # Map to quickly find which graph a link belongs to
    graph_id_new = 0  # Unique identifier for each graph

    for time, event, flow_id, links in events:
        if event == 'start':
            # Find all graphs involved with the new flow's links
            involved_graph_ids = set()
            for link in links:
                if link in link_to_graph:
                    involved_graph_ids.add(link_to_graph[link])

            new_links = defaultdict(set)
            new_flows = set()
            new_all_flows = set()
            
            if involved_graph_ids:
                for gid in involved_graph_ids:
                    graph = active_graphs[gid]
                    new_links.update(graph['active_links'])
                    new_flows.update(graph['active_flows'])
                    new_all_flows.update(graph['all_flows'])
                    time=graph['start_time']
                    
                    for link in graph['active_links']:
                        del link_to_graph[link]
                    del active_graphs[gid]
            
            for link in links:
                new_links[link].add(flow_id)
            new_flows.add(flow_id)
            new_all_flows.add(flow_id)
            for link in new_links:
                assert link not in link_to_graph, f"Link {link} already in a graph"
                link_to_graph[link] = graph_id_new
            active_graphs[graph_id_new] = {
                'active_links': new_links,
                'active_flows': new_flows,
                'all_flows': new_all_flows,
                'start_time': time
            }
            graph_id_new += 1

        elif event == 'end':
            graph = None
            for link in links:
                if link in link_to_graph:
                    graph_id = link_to_graph[link]
                    graph = active_graphs[graph_id]
                    break

            if graph:
                for link in links:
                    if flow_id in graph['active_links'][link]:
                        graph['active_links'][link].remove(flow_id)
                    else:
                        assert False, f"Flow {flow_id} not found in link {link} of graph {graph_id}"
                if flow_id in graph['active_flows']:
                    graph['active_flows'].remove(flow_id)
                else:
                    assert False, f"Flow {flow_id} not found in active flows of graph {graph_id}"
                if not graph['active_flows']:  # If no active flows left in the graph
                    busy_periods.append((graph['start_time'], time, list(graph['active_links'].keys()), list(graph['all_flows'])))
                    del active_graphs[graph_id]
                    for link in graph['active_links']:
                        if link in link_to_graph:
                            del link_to_graph[link]
            else:
                assert False, f"Flow {flow_id} has no active graph"
    return busy_periods

# Main function to run the analysis
def main():
    root_dir = '../data_test/'  # Update with the path to your root directory
    mix_list = [20, 179, 12]
    for mix_id in mix_list:
        flow_info_file = f"{root_dir}{mix_id}/ns3-config/0/fct_topology_flows_dctcp.txt"
        link_info_file = f"{root_dir}{mix_id}/mlsys-test/path_0.txt"

        # Parse logs
        flows = parse_flow_info_log(flow_info_file)
        print(f'Parsed {len(flows)} flows')
        link_flows = parse_link_info_log(link_info_file)
        print(f'Parsed {len(link_flows)} links')
        
        # Assign links to flows
        flows = assign_links_to_flows(flows, link_flows)
        
        # Update bipartite graph and calculate busy periods
        busy_periods = update_bipartite_graph_and_calculate_busy_periods(flows)
        
        # Print results
        for start_time, end_time, links, all_flows in busy_periods:
            print(f'Busy Period: Start = {start_time}, End = {end_time}')
            print(f'  Links: {list(links)}')
            print(f'  Flows: {list(all_flows)}')

if __name__ == '__main__':
    main()