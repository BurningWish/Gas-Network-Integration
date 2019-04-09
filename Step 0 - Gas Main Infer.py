import math
import nx_expand
import networkx as nx
import fiona
import psycopg2
from shapely.wkt import loads
from shapely.geometry import LineString

def calculate_dir(G, source):
    
    for edge in G.edges():
        n1 = edge[0]
        n2 = edge[1]
        dist1 = nx.shortest_path_length(G, source=source, target=n1, weight='Length')  # NOQA
        dist2 = nx.shortest_path_length(G, source=source, target=n2, weight='Length')  # NOQA
    
        if dist1 <= dist2:
            G.edge[n1][n2]['facilityFr'] = G.node[n1]['nodeName']
            G.edge[n1][n2]['facilityTo'] = G.node[n2]['nodeName']
        else:
            G.edge[n1][n2]['facilityFr'] = G.node[n2]['nodeName']
            G.edge[n1][n2]['facilityTo'] = G.node[n1]['nodeName']
    
        G.edge[n1][n2]['flow'] = 1.11
    
def graph_distance(g1, g2):
    distance = 9999999
    nodes = [0, 0]
    for n1 in g1.nodes():
        for n2 in g2.nodes():
            temp_dist = LineString([n1, n2]).length
            if temp_dist < distance:
                distance = temp_dist
                nodes = [n1, n2]


dbname = "NGN_Network"
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % dbname)  # NOQA
cur = conn.cursor()

all_bids = []
cur.execute("select bid from buildings order by bid asc")
results = cur.fetchall()
for result in results:
    all_bids.append(result[0])


temp_bids = []
cur.execute("select distinct on (b.bid), b.bid, p.pid\
            from pipes as p, buildings as b \
            where st_distance(p.geom, b.geom) < 50")
results = cur.fetchall()
for result in results:
    temp_bids.append(result[0])

bids = list(set(all_bids).difference(set(temp_bids)))

G = nx.Graph()
cur.execute("select distinct on(rid) r.geom, r.rid from roads as r, buildings as b \
            where b.bid in %s \
            order by st_distance(r.geom, b.geom)" % bids)
results = cur.fetchall()
for result in results:
    wkt = result[0]
    line = loads(wkt)
    coords = list(line.coords)
    G.add_edge(coords[0], coords[-1])
    G.edge[coords[0]][coords[-1]]['geom'] = wkt

road_G = nx.Graph()
cur.execute("select st_astext(geom) from roads")
results = cur.fetchall()
for result in results:
    wkt = result[0]
    coords = list(loads(wkt).coords)
    road_G.add_edge(coords[0], coords[1])


G_list = list(nx.connected_component_subgraphs(G))

for g in G_list:
    nodes = graph_distance(road_G, g)
    g.add_edge(nodes[0], nodes[1])
    calculate_dir(g, nodes[0])



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
            """
            ==============  WE DON'T OUTPUT the REG SITE ======================
            """
            thisNode = G.node[node]
            record = {}
            record['geometry'] = {'coordinates': thisNode['Coords'], 'type': 'Point'}  # NOQA
            record['properties'] = {'Pressure': 1.11,
                                    'NodeName': thisNode['nodeName'],
                                    'NodeSubsys': thisNode['nodeSubsys']}  # NOQA
            source.write(record)
