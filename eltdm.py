import numpy
import matplotlib.pyplot
data = numpy.loadtxt("./Data/test.txt", dtype='uint16')
print("Input data is:", data)
print("Input data type is:", type(data))

#Make a list of adjacency lists (first element is the node of origin)
nodeId = numpy.unique(data)
nrow = len(data)
adj_list = []

for n in nodeId:
    adj = []
    for i in range(nrow):
        if data[i, 0] == n:
            adj.append(data[i, 1])
        if data[i, 1] == n:
            adj.append(data[i, 0])
    adj = list(set(adj))
    adj.insert(0, n)
    adj_list.append(adj)
print('adj_list is:', adj_list)

class tuples():
    def __init__(self, targetId, sourceId, distance, status, weight, pathInfo, adjList) :
        self.targetId = targetId
        self.sourceId = sourceId
        self.distance = distance
        self.status = status
        self.weight = weight
        self.pathInfo = pathInfo
        self.adjList = adjList

network = []
for n in nodeId:
    v = tuples(targetId = n, sourceId = n, distance = 0, status = 'a', weight = 1, pathInfo = [], adjList = [])
    for i in range(len(adj_list)):
        if adj_list[i][0] == n:
            v.adjList = adj_list[i][1:len(adj_list[i])]
            break
    network.append([v.targetId, v.sourceId, v.distance, v.status, v.weight, v.pathInfo, v.adjList])
print("Network is:")
for i in range(len(network)):
    print(network[i])

#Parallerization Part

from pyspark import SparkContext
from copy import deepcopy
from operator import itemgetter
from itertools import groupby

sc = SparkContext()
print("SparkContext version is:")
sc.version

#Set a dataset and create key-value pairs
rdd = sc.parallelize(network)
rdd = rdd.map(lambda x: (x[0], x[1:]))
print("rdd content is:")
rdd.collect()

print("Stage 1 - Map")

def stage1_map(p):
    result = []
    if p[1][2] == 'a':
        p[1][2] = 'i'
        p[1][1] += 1
        p[1][4].append(p[0])
        x = deepcopy(p)
        result.append(x)
        temp = p[1][5].copy()
        for i in range(len(temp)):
            k = temp[i]
            p[1][2] = 'a'
            p[1][5] = []
            y = deepcopy(p)
            result.append((k, y[1]))
    return result
pos_map1 = rdd.flatMap(stage1_map)
pos_map1.collect()
test = pos_map1.collect()
b = (4, [4, 2, 'a', 1, [2], []])
c = (4, [4, 1, 'a', 1, [3], [1, 3]])
d = (4, [4, 25, 'a', 1, [2], []])
e = (4, [4, 5, 'a', 1, [2], []])
f = (4, [4, 1, 'a', 1, [3], [1, 3]])
g = (4, [4, 1, 'a', 1, [5], [1, 6]])
h = (4, [4, 1, 'a', 1, [2], [18]])

test.append(b)
test.append(c)
test.append(d)
test.append(e)
test.append(f)
test.append(g)
test.append(h)

print("We add test tuples to see if Reduce works well:")
for i in range(len(test)):
    print(test[i])
test = sc.parallelize(test)

print("Input to Stage 1 Reduce is (groupby done):")
test.groupByKey().map(lambda x : (x[0], (list(x[1])))).collect()

print("Stage 1 - Reduce")

pre_red1 = test.groupByKey().map(lambda x : (x[0], (list(x[1]))))

res = []

def stage1_reduce(p):
    p1 = sorted(list(p[1]), key=itemgetter(0))
    for i in range(len(p1)):
        print(p1[i])

    j = 0
    dlist = []
    for k, g in groupby(p1, key=itemgetter(0)):
    #    print("SourceId:" , k)
        i = 0
        m = []
        for value in g:
            print(value)
            m.append(value[1])
            if i == 0:
                mini = value[1]
                tempmin = j
                cp1 = value.copy()
                minList = [cp1]
                minList_index = [j]

            if value[1] > mini:
                dlist.append(j)
            elif value[1] < mini:
                mini = value[1]
                dlist.append(tempmin)
                cp2 = value.copy()
                minList = [cp2]
                minList_index = [j]
                tempmin = j
            else:
                cp3 = value.copy()
                if i != 0:
                    minList.append(cp3)
                    minList_index.append(j)
            i += 1
            j += 1
        for index in minList_index:
            p1[index][3] = len(minList)
    if len(dlist) > 0:
        del_f = lambda x, dlist: [v for ind, v in enumerate(x) if ind not in dlist]
        p2 = del_f(p1, dlist)
    else:
        p2 = p1.copy()
    for i in range(len(p2)):
        res.append((p[0], p2[i]))
    return res

pos_red1 = pre_red1.flatMap(stage1_reduce)
print("Stage 1 Reduce result (after first loop) is:")
pos_red1.collect()
