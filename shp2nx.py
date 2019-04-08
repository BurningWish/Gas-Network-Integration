import networkx as nx
import fiona
import shapely
from shapely.geometry import LineString, Point

def read_shp(edge_path, node_path):
    network = nx.Graph()    
    
    with fiona.open(edge_path, 'r') as c:
        for record in c:
            old_coords = record['geometry']['coordinates']
            new_coords = []
            for coord in old_coords:
                x = round(coord[0], 3)
                y = round(coord[1], 3)
                new_coords.append((x, y))
            new_line = LineString(new_coords)
            startNode = new_coords[0]
            endNode = new_coords[-1]
            network.add_edge(startNode, endNode)
            network.edge[startNode][endNode]['wkt']= new_line.wkt
            network.edge[startNode][endNode]['pipeDiam'] = record['properties']['PipeDiamet']
            network.edge[startNode][endNode]['material'] = record['properties']['PipeMateri']
            network.edge[startNode][endNode]['facilityNa'] = record['properties']['FacilityNa']
            network.edge[startNode][endNode]['facilityFr'] = record['properties']['FacilityFr']
            network.edge[startNode][endNode]['facilityTo'] = record['properties']['FacilityTo']
            network.edge[startNode][endNode]['flow'] = record['properties']['FacilityFl']
            network.edge[startNode][endNode]['Length'] = record['properties']['PipeLength']
            network.edge[startNode][endNode]['Coords'] = new_coords

    with fiona.open(node_path, 'r') as c:
        for record in c:
            point = record['geometry']['coordinates']
            node = (round(point[0], 3), round(point[1], 3))
            network.node[node]['nodeName'] = record['properties']['NodeName']
            network.node[node]['nodeSubsys'] = record['properties']['NodeSubsys']
            network.node[node]['nodeType']= record['properties']['NodeBoun_1']
            network.node[node]['pressure'] = record['properties']['Pressure']
            network.node[node]['Coords'] = [node[0], node[1]]

    # Now use the nodeSubsys to update edgeSubsys
    # And also make sure all the pipes are main
    for edge in network.edges():
        startNode = edge[0]
        endNode = edge[1]
        network.edge[startNode][endNode]['edgeSubsys'] = network.node[startNode]['nodeSubsys']  # NOQA
        network.edge[startNode][endNode]['edgeType'] = 'main'
        

    return network
