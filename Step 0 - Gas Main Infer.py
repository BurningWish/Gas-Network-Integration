import nx_expand
import networkx as nx
import fiona

id = 9
nodeSubsys = 27

pipe_path = 'exp_csep//Expand_%d.shp' % id
csep_path = 'exp_csep//CSEP_%d.shp' % id

my_dict = {}
my_dict['newFacilityNa'] = id * 1000
my_dict['newNodeName'] = id * 1000


"""
=============   First Let's Read the expand and reg site  =====================
"""
G = nx_expand.read_shp(pipe_path, csep_path)

# Find the reg_node
reg_node = [0, 0]
for n in G.nodes():
    if 'nodeName' in G.node[n].keys():
        reg_node = n

# Get the pressure
pressure = G.node[reg_node]['pressure']

# assign node name and facility name
for n in G.nodes():
    if 'nodeName' not in G.node[n].keys():
        G.node[n]['nodeName'] = 'synn' + '_' + str(my_dict['newNodeName']).zfill(9)
        my_dict['newNodeName'] += 1

for edge in G.edges():
    f = edge[0]
    t = edge[1]
    G.edge[f][t]['facilityNa'] = 'synp' + '_' + str(my_dict['newFacilityNa']).zfill(9)
    my_dict['newFacilityNa'] += 1

# assign nodeSubsys to each node
for n in G.nodes():
    if 'nodeSubsys' not in G.node[n].keys():
        G.node[n]['nodeSubsys'] = 1
    
# assign Coords to each node
for n in G.nodes():
    if 'Coords' not in G.node[n].keys():
        G.node[n]['Coords'] = [n[0], n[1]]

"""
==================   calculate direction for each edge   ====================
"""

for edge in G.edges():
    n1 = edge[0]
    n2 = edge[1]
    dist1 = nx.shortest_path_length(G, source=reg_node, target=n1, weight='Length')  # NOQA
    dist2 = nx.shortest_path_length(G, source=reg_node, target=n2, weight='Length')  # NOQA

    if dist1 <= dist2:
        G.edge[n1][n2]['facilityFr'] = G.node[n1]['nodeName']
        G.edge[n1][n2]['facilityTo'] = G.node[n2]['nodeName']
    else:
        G.edge[n1][n2]['facilityFr'] = G.node[n2]['nodeName']
        G.edge[n1][n2]['facilityTo'] = G.node[n1]['nodeName']

    G.edge[n1][n2]['flow'] = 1.11

"""
=================   Write our result into Shp  ==============================
"""

print("start writing!!!")
# Write the edges first
sourceDriver = 'ESRI Shapefile'
sourceCrs = {'y_0': -100000, 'units': 'm', 'lat_0': 49,
             'lon_0': -2, 'proj': 'tmerc', 'k': 0.9996012717,
             'no_defs': True, 'x_0': 400000, 'datum': 'OSGB36'}

result_folder = "result//Edges//"

# write the network edges
sourceSchema = {'properties': {'PipeLength': 'float:19.11',
                               'PipeDiamet': 'float: 19.11',
                               'PipeMateri': 'str',
                               'FacilityNa': 'str',
                               'FacilityFr': 'str',
                               'FacilityTo': 'str',
                               'FacilityFl': 'float:19.11'},
                'geometry': 'LineString'}  # NOQA

fileName = result_folder + 'Edges_exp%d.shp' % id

with fiona.open(fileName,
                'w',
                driver=sourceDriver,
                crs=sourceCrs,
                schema=sourceSchema) as source:
    for edge in G.edges():
        startNode = edge[0]
        endNode = edge[1]
        record = {}
        thisEdge = G.edge[startNode][endNode]
        record['geometry'] = {'coordinates': thisEdge['Coords'], 'type': 'LineString'}  # NOQA
        record['properties'] = {'PipeLength': thisEdge['Length'],
                                'PipeDiamet': 20.2,
                                'PipeMateri': 'SY',
                                'FacilityNa': thisEdge['facilityNa'],
                                'FacilityFr': thisEdge['facilityFr'],
                                'FacilityTo': thisEdge['facilityTo'],
                                'FacilityFl': thisEdge['flow']}  # NOQA
        source.write(record)



# write the nodes then
sourceDriver = 'ESRI Shapefile'
sourceCrs = {'y_0': -100000, 'units': 'm', 'lat_0': 49,
             'lon_0': -2, 'proj': 'tmerc', 'k': 0.9996012717,
             'no_defs': True, 'x_0': 400000, 'datum': 'OSGB36'}

result_folder = "result//Nodes//"

sourceSchema = {'properties': {'Pressure': 'float:19.11',
                               'NodeName': 'str',
                               'NodeSubsys': 'int'},
                'geometry': 'Point'}  # NOQA

fileName = result_folder + 'Nodes_exp%d.shp' %id
with fiona.open(fileName,
                'w',
                driver=sourceDriver,
                crs=sourceCrs,
                schema=sourceSchema) as source:
    for node in G.nodes():
        if node != reg_node:
            """
            ==============  WE DON'T OUTPUT the REG SITE ======================
            """
            thisNode = G.node[node]
            record = {}
            record['geometry'] = {'coordinates': thisNode['Coords'], 'type': 'Point'}  # NOQA
            record['properties'] = {'Pressure': pressure,
                                    'NodeName': thisNode['nodeName'],
                                    'NodeSubsys': thisNode['nodeSubsys']}  # NOQA
            source.write(record)
