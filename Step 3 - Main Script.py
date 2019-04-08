import sys
import psycopg2
import pg_read
import shp2nx
import copy
import networkx as nx
import fiona
from shapely.geometry import LineString, Point
from math import acos, degrees
import pickle


def roundNode(coordinate):
    return round(coordinate[0], 3), round(coordinate[1], 3)


def roundPoint(point):
    return roundNode((point.x, point.y))


# Cut a line using a break point, with the help of Shapely library
def cut(line, point):
    distance = line.project(point)
    if distance <= 0.0 or distance >= line.length:
        return [LineString(line)]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return [
                LineString(coords[:i + 1]),
                LineString(coords[i:])]
        if pd > distance:
            return [
                LineString(coords[:i] + [(point.x, point.y)]),
                LineString([(point.x, point.y)] + coords[i:])]


# using a list of points to cut the line segment
def multiCut(line, points):

    cutLines = []
    referenceToPoint = {}  # dictionary to map the refererence to point on this line  # NOQA
    references = []

    for point in points:
        reference = line.project(point)
        referenceToPoint[reference] = point
        references.append(reference)

    tempReferences = []  # remove the duplicate references
    for reference in references:
        if reference not in tempReferences:
            tempReferences.append(reference)

    references = tempReferences
    references.sort()  # sort the references

    untilEnd = False  # it's used to indicate if the last break point just hits the end vertex of the line  # NOQA
    tempLine = line  # Let's start to do multi cut at this stage
    while len(references) != 0:
        reference = references.pop(0)
        breakPoint = referenceToPoint[reference]
        currentReference = tempLine.project(breakPoint)
        if currentReference != 0 and currentReference != tempLine.length:
            cutLine, tempLine = cut(tempLine, breakPoint)
            cutLines.append(cutLine)
        elif currentReference == 0:
            pass
        elif currentReference == tempLine.length:
            cutLines.append(tempLine)
            untilEnd = True

    if untilEnd == False:  # the last break point doesn't hit the end vertex of the original line  # NOQA
        cutLines.append(tempLine)

    return cutLines


def modifyGraph(graph, cutLines, extraNodeList):
    startNode = roundNode(list(cutLines[0].coords)[0])
    endNode = roundNode(list(cutLines[-1].coords)[-1])

    startPres = mainNet.node[startNode]['pressure']
    endPres = mainNet.node[endNode]['pressure']
    presDelta = (endPres - startPres) / len(cutLines)

    """
    This is where we need to remember attributes of original pipes
    """
    subSys = mainNet.edge[startNode][endNode]['edgeSubsys']
    pipeDiam = mainNet.edge[startNode][endNode]['pipeDiam']
    material = mainNet.edge[startNode][endNode]['material']
    facilityNa = mainNet.edge[startNode][endNode]['facilityNa']
    flow = mainNet.edge[startNode][endNode]['flow']
    facilityFr = mainNet.edge[startNode][endNode]['facilityFr']
    facilityTo = mainNet.edge[startNode][endNode]['facilityTo']

    flowFlag = True
    if facilityFr == mainNet.node[startNode]['nodeName'] and facilityTo == mainNet.node[endNode]['nodeName']:  # NOQA
        flowFlag = True
    else:
        flowFlag = False

    if(startNode, endNode) in graph.edges():
        graph.remove_edge(startNode, endNode)
    elif(endNode, startNode) in graph.edges():
        graph.remove_edge(endNode, startNode)
    else:
        print("faliure to remove pipe_edge whose pid is %d" % pid)  # NOQA

    breakNodeList = []
    new_pipe = 0
    segment = 0
    for line in cutLines:
        segment += 1
        new_pipe += 1
        coordinate = list(line.coords)
        tempStartNode = roundNode(coordinate[0])
        tempStartGeom = coordinate[0]
        tempEndNode = roundNode(coordinate[-1])
        tempEndGeom = coordinate[-1]

        graph.add_edge(tempStartNode, tempEndNode)
        # Now let's check if the tempStartNode / tempEndNode needs nodeName or not  # NOQA
        if 'nodeName' not in graph.node[tempStartNode].keys():
            graph.node[tempStartNode]['nodeName'] = 'new' + '_' + str(my_dict['newNodeName']).zfill(10)  # NOQA
            my_dict['newNodeName'] += 1

        if 'nodeName' not in graph.node[tempEndNode].keys():
            graph.node[tempEndNode]['nodeName'] = 'new' + '_' + str(my_dict['newNodeName']).zfill(10)  # NOQA
            my_dict['newNodeName'] += 1

        # Deal with node attributes
        graph.node[tempStartNode]['Coords'] = tempStartGeom
        graph.node[tempStartNode]['nodeSubsys'] = subSys
        graph.node[tempStartNode]['nodeType'] = 0

        graph.node[tempEndNode]['Coords'] = tempEndGeom
        graph.node[tempEndNode]['nodeSubsys'] = subSys
        graph.node[tempEndNode]['nodeType'] = 0
        graph.node[tempEndNode]['pressure'] = round(startPres + segment * presDelta, 4)  # NOQA

        # Deal with edge attributes
        graph.edge[tempStartNode][tempEndNode]['edgeSubsys'] = subSys
        graph.edge[tempStartNode][tempEndNode]['edgeType'] = 'main'
        graph.edge[tempStartNode][tempEndNode]['Coords'] = coordinate
        graph.edge[tempStartNode][tempEndNode]['Length'] = line.length
        graph.edge[tempStartNode][tempEndNode]['pipeDiam'] = pipeDiam
        graph.edge[tempStartNode][tempEndNode]['material'] = material
        graph.edge[tempStartNode][tempEndNode]['facilityNa'] = facilityNa + '_' + str(new_pipe)  # NOQA
        graph.edge[tempStartNode][tempEndNode]['flow'] = flow

        if flowFlag:
            graph.edge[tempStartNode][tempEndNode]['facilityFr'] = graph.node[tempStartNode]['nodeName']  # NOQA
            graph.edge[tempStartNode][tempEndNode]['facilityTo'] = graph.node[tempEndNode]['nodeName']  # NOQA
        else:
            graph.edge[tempStartNode][tempEndNode]['facilityFr'] = graph.node[tempEndNode]['nodeName']  # NOQA
            graph.edge[tempStartNode][tempEndNode]['facilityTo'] = graph.node[tempStartNode]['nodeName']  # NOQA

        breakNodeList.append(tempEndNode)
    breakNodeList.pop(-1)  # we don't need the last node

    for extraNode in extraNodeList:  # what if we have at most two extra nodes here?  # NOQA
        breakNodeList.append(extraNode)

    return breakNodeList


def writeGraph(graph):
    """
    please be careful, for london, no need to generate feeders:)
    """
    # Write the edges first
    sourceDriver = 'ESRI Shapefile'
    sourceCrs = {'y_0': -100000, 'units': 'm', 'lat_0': 49,
                 'lon_0': -2, 'proj': 'tmerc', 'k': 0.9996012717,
                 'no_defs': True, 'x_0': 400000, 'datum': 'OSGB36'}

    result_folder = "result//"

    # write the network edges
    sourceSchema = {'properties': {'Length': 'float:19.11',
                                   'FacilityNa': 'str',
                                   'PipeDiamet': 'float:19.11',
                                   'PipeMateri': 'str',
                                   'FacilityFr': 'str',
                                   'FacilityTo': 'str',
                                   'Flow': 'float:19.11',
                                   'EdgeType': 'str',
                                   'EdgeSubsys': 'int'},
                    'geometry': 'LineString'}  # NOQA

    fileName = result_folder + 'Edges.shp'

    with fiona.open(fileName,
                    'w',
                    driver=sourceDriver,
                    crs=sourceCrs,
                    schema=sourceSchema) as source:
        for edge in graph.edges():
            startNode = edge[0]
            endNode = edge[1]
            record = {}
            thisEdge = graph.edge[startNode][endNode]
            record['geometry'] = {'coordinates': thisEdge['Coords'], 'type': 'LineString'}  # NOQA
            record['properties'] = {'Length': thisEdge['Length'],
                                    'FacilityNa': thisEdge['facilityNa'],
                                    'PipeDiamet': thisEdge['pipeDiam'],
                                    'PipeMateri': thisEdge['material'],
                                    'FacilityFr': thisEdge['facilityFr'],
                                    'FacilityTo': thisEdge['facilityTo'],
                                    'Flow': thisEdge['flow'],
                                    'EdgeType': thisEdge['edgeType'],
                                    'EdgeSubsys': thisEdge['edgeSubsys']}  # NOQA
            source.write(record)

    # write the nodes then
    sourceDriver = 'ESRI Shapefile'
    sourceCrs = {'y_0': -100000, 'units': 'm', 'lat_0': 49,
                 'lon_0': -2, 'proj': 'tmerc', 'k': 0.9996012717,
                 'no_defs': True, 'x_0': 400000, 'datum': 'OSGB36'}

    result_folder = "result//"

    # write the network edges
    sourceSchema = {'properties': {'Pressure': 'float:19.11',
                                   'NodeName': 'str',
                                   'NodeType': 'str',
                                   'NodeSubsys': 'int',
                                   'Toid': 'str'},
                    'geometry': 'Point'}  # NOQA

    fileName = result_folder + 'Nodes.shp'
    with fiona.open(fileName,
                    'w',
                    driver=sourceDriver,
                    crs=sourceCrs,
                    schema=sourceSchema) as source:
        for node in graph.nodes():
            thisNode = graph.node[node]
            record = {}
            record['geometry'] = {'coordinates': thisNode['Coords'], 'type': 'Point'}  # NOQA
            record['properties'] = {'Pressure': thisNode['pressure'],
                                    'NodeName': thisNode['nodeName'],
                                    'NodeType': thisNode['nodeType'],
                                    'NodeSubsys': thisNode['nodeSubsys'],
                                    'Toid': thisNode['toid']}  # NOQA
            source.write(record)


"""
===============================================================================
                     From here is the main programme
===============================================================================
"""

dbname = "00_NCL_GAS_TEMP"

"""
================  First let's read all the data into python =============
"""
edge_path = 'Input - Complete/NCL_Gas_Pipes_Complete.shp'
node_path = 'Input - Complete/NCL_Gas_Nodes_Complete.shp'
mainNet = shp2nx.read_shp(edge_path, node_path)

"""
Read each road into RoadList
"""

pipeList = pg_read.readPipe()
print("%d pipes have been read\n" % len(pipeList))

"""
Read each building into buildingList
"""
buildingList = pg_read.readBuilding()
print("%d buildings have been read\n" % len(buildingList))


print("=====================================================")
print("input data reading is completed......................")
print("=====================================================")
print("\n")

"""
========= Now we will relate different class objects with each other ==========
"""
conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % dbname)  # NOQA
cur = conn.cursor()

"""
Map each road to the network edges
"""
for pipe in pipeList:
    startNodeX = round(list(pipe.geom.coords)[0][0], 3)
    startNodeY = round(list(pipe.geom.coords)[0][1], 3)
    endNodeX = round(list(pipe.geom.coords)[-1][0], 3)
    endNodeY = round(list(pipe.geom.coords)[-1][1], 3)

    # road.edge: a tuple, it's the network edge representation of this road segment   # NOQA
    pipe.edge = ((startNodeX, startNodeY), (endNodeX, endNodeY))
print("Mapping each pipe to the network edges done \n")


"""
For each building, find the nearest pipe
"""
cur.execute("select distinct on (bid) bid, pid, st_distance(b.cp, p.geom) \
            from buildings as b, gas_pipes as p \
            where st_dwithin(b.cp, p.geom, 500) \
            order by bid, st_distance(b.cp, p.geom)")
results = cur.fetchall()
if len(results) != len(buildingList):
    print("error happens when assigning each building a pipe")
    sys.exit()

for result in results:
    bid = result[0]
    pid = result[1]
    building = buildingList[bid]
    pipe = pipeList[pid]
    buildingList[bid].pid = pid
    reference = pipe.geom.project(buildingList[bid].centroid)
    buildingList[bid].accessPoint = pipe.geom.interpolate(reference)

print("linking buildings with pipes done!")


"""
===============================================================================
      Now it is time to connect trunk pipe network with buildings
===============================================================================
"""
pidToBreakPoints = {}
for building in buildingList:
    pid = building.pid
    if pid not in pidToBreakPoints.keys():
        pidToBreakPoints[pid] = []
    if building.accessPoint not in pidToBreakPoints[pid]:
        pidToBreakPoints[pid].append(building.accessPoint)

# Now the really tricky part, use all the BreakPoints to modify necessary edges
edgeGetNewNodes = {}

baseNet = copy.deepcopy(mainNet)


print("before modifying, the baseNet has %d edges" % baseNet.number_of_edges())

my_dict = {}
my_dict['newNodeName'] = 0

key_id = 0

for pid in pidToBreakPoints.keys():
    print("pid: %d, processed: %d, all: %d" % (pid, key_id, len(pidToBreakPoints.keys())))  # NOQA
    key_id += 1
    extraNodeList = []
    breakPoints = pidToBreakPoints[pid]
    for breakPoint in breakPoints:
        testNode = roundPoint(breakPoint)
        if testNode in pipeList[pid].edge:
            extraNodeList.append(testNode)
    cutLines = multiCut(pipeList[pid].geom, breakPoints)
    edgeGetNewNodes[pipeList[pid].edge] = modifyGraph(baseNet, cutLines, extraNodeList)  # NOQA
print("after modyfing, the baseNet has %d edges" % baseNet.number_of_edges())

my_dict['servPipeNa'] = 0
# Now derive edges from buildings/reg sites to the baseNetwork as well
for building in buildingList:
    startNode = roundPoint(building.centroid)
    endNode = roundPoint(building.accessPoint)
    coords1 = (building.centroid.x, building.centroid.y)
    coords2 = (building.accessPoint.x, building.accessPoint.y)
    baseNet.add_edge(startNode, endNode)

    # Deal with building node attributes
    baseNet.node[startNode]['nodeName'] = 'new' + '_' + str(my_dict['newNodeName']).zfill(10)  # NOQA
    my_dict['newNodeName'] += 1
    baseNet.node[startNode]['pressure'] = baseNet.node[endNode]['pressure'] - 0.01  # NOQA
    baseNet.node[startNode]['Coords'] = [startNode[0], startNode[1]]
    baseNet.node[startNode]['nodeType'] = 3
    baseNet.node[endNode]['nodeType'] = 2
    baseNet.node[startNode]['nodeSubsys'] = baseNet.node[endNode]['nodeSubsys']
    baseNet.node[startNode]['toid'] = building.toid

    # Deal with service pipe attributes
    baseNet.edge[startNode][endNode]['Coords'] = [coords1, coords2]
    baseNet.edge[startNode][endNode]['Length'] = LineString([coords1, coords2]).length  # NOQA
    baseNet.edge[startNode][endNode]['pipeDiam'] = 10.0
    baseNet.edge[startNode][endNode]['material'] = 'SV'
    baseNet.edge[startNode][endNode]['facilityNa'] = 'serv' + '_' + str(my_dict['servPipeNa']).zfill(10)  # NOQA
    baseNet.edge[startNode][endNode]['facilityFr'] = baseNet.node[endNode]['nodeName']  # NOQA
    baseNet.edge[startNode][endNode]['facilityTo'] = baseNet.node[startNode]['nodeName']  # NOQA
    baseNet.edge[startNode][endNode]['flow'] = 1.0
    baseNet.edge[startNode][endNode]['edgeType'] = 'service'
    baseNet.edge[startNode][endNode]['edgeSubsys'] = baseNet.node[endNode]['nodeSubsys']  # NOQA

print("after deriving addtional edges, the baseNet has %d edges" % baseNet.number_of_edges())  # NOQA

"""
Make Sure the source nodes are still intact
"""
for node in mainNet.nodes():
    if mainNet.node[node]['nodeType'] == 1:
        baseNet.node[node]['nodeType'] = 1

"""
Then make all the nodeTypes readable and sort out toid
"""
typeRef = ['distribution', 'source', 'access', 'building']
for node in baseNet.nodes():
    typeIndex = baseNet.node[node]['nodeType']
    baseNet.node[node]['nodeType'] = typeRef[typeIndex]
    if 'toid' not in baseNet.node[node]:
        baseNet.node[node]['toid'] = '0'

writeGraph(baseNet)
