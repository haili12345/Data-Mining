import networkx as nx
import numpy as np
import time
import sys
ratings_text=sys.argv[1]
k=int(sys.argv[2])
ratings_text=np.loadtxt(ratings_text,dtype=np.int,delimiter=" ")
p=list(ratings_text)
graph=nx.Graph(p)
li=[]
for i in range(k-1):
    Agraph = nx.to_numpy_matrix(graph,nodelist=sorted(graph.nodes()))
    nodes=sorted(graph.nodes())
    D=np.zeros((Agraph.shape[0],Agraph.shape[1]))
    for i in graph.degree():
        index=nodes.index(i[0])
        D[index][index]=i[1]
    L=D-Agraph
    eigvalues,eigvactors=np.linalg.eig(L)
    index=np.argpartition(eigvalues, 1)[1]##find the 2rd smallest
    eigvactor=eigvactors[:,index]
    cluster1=np.argwhere(eigvactor>0)[:,0]
    cluster1=np.array(nodes)[cluster1]
    cluster2=np.argwhere(eigvactor<0)[:,0]
    cluster2=np.array(nodes)[cluster2]
    if len(list(cluster1))>len(list(cluster2)):
        remove=cluster2
        li.append(cluster2)
    else:
        remove=cluster1
        li.append(cluster1)
    graph.remove_nodes_from(remove)
li.append(np.array(sorted(graph.nodes())))
f=open(sys.argv[3],'w')
outlis=[]
for i in range(0,k):
    out=''
    for j in li[i]:
        out+='%d'%j+','
    outlis.append(out)
for i in outlis:
    f.write(i+'\n')
# f.write(str(list(li[0]))+'\n'+str(list(li[1]))+'\n'+str(list(li[2])))
f.close()