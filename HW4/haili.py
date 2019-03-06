import sys
import numpy as np
from sympy import * 
from numpy.random import rand
from numpy import matrix
import pandas as pd
import math
import time
from pandas import DataFrame 
import re

sample_data=np.loadtxt(sys.argv[1],dtype=np.str)
a=0
points = list(np.loadtxt(sys.argv[1],delimiter=','))

k = int(sys.argv[3])
n = int(sys.argv[4])
p= float(sys.argv[5])

def updatedis(vector1,vector2):
    vector1=vector1.split(",")
    vector2=vector2.split(",")
    x1=float(vector1[0])
    x2=float(vector2[0])
    y1=float(vector1[1])
    y2=float(vector2[1])
    x=abs(x1-x2)
    y=abs(y1-y2)
    distance=sqrt(x**2+y**2)/float(2)
    return distance
time1=time.time()
df=DataFrame(np.zeros((len(points),len(points))),index=sample_data,columns=sample_data) 
for i in df.index:
    a+=1
    for j in df.columns[a:]:
        df.loc[[i],[j]]=updatedis(i,j)
M=df.replace(0,np.nan)
while len(points)>k:
    lis=[]
    minimal=np.nanmin(M.values)
    b=M==minimal
    ix=M[b==True].first_valid_index()
    lis.append(ix)
    loc_index=M.index.get_loc(ix)
    co=M.columns[(M == minimal).iloc[loc_index]]
    loc_column=M.columns.get_loc(co[0])
    ix+='\n'+co[0]
    i=ix.split('\n')
    ix2=re.split('\n|,',ix) 
    new=np.array(np.array_split(ix2, len(i)))
    new_cluster=new.astype(float)
    if loc_index>loc_column:
        del points[loc_index]
        del points[loc_column]
    else:
        del points[loc_column]
        del points[loc_index]
    new_distances=[]
    for i in range(len(points)):
        lef_index=points[i]
        verTable = np.tile(new_cluster,(lef_index.shape[0],1,1))
        verTable2 = np.tile(lef_index,(new_cluster.shape[0],1,1))
        horTable = np.transpose(verTable2,(1,0,2))
        disTable = np.sqrt(np.sum((verTable - horTable)**2,axis=2))
        mins_dis=disTable.min()
        new_distances.append(mins_dis)
    points.append(new_cluster)
    M.drop(M.columns[[loc_index,loc_column]], axis=1,inplace=True)
    M.drop(M.index[[loc_index,loc_column]],inplace=True)
    M[ix]=new_distances
    
    M.loc[len(M)]=np.NaN
    M.index = M.index[:-1].append(pd.Index([ix]))
    
centroids = np.zeros((k,2))
centroidIndex = 0
repres2 = np.empty((k,n,2))
repre2Count = 0

for i in points:
    centroids[centroidIndex] = np.mean(i,axis=0)
    centroidIndex+=1
    repres=np.zeros((n,2))
    minx=np.min(i[:,0],axis=0)
    idex=np.where(i[:,0]==minx)
    b=i[idex[0],:]
    miny=np.min(b[:,1],axis=0)
    m=np.array([[minx,miny]])
    repres[0]=m[0]
    delete_index=np.where(np.all(i==m[0],axis=1))
    i=np.delete(i, delete_index[0][0], axis=0) 
    verTable = np.tile(m,(i.shape[0],1,1))
    verTable2 = np.tile(i,(m.shape[0],1,1))
    horTable = np.transpose(verTable2,(1,0,2))
    disTable = np.sqrt(np.sum((verTable - horTable)**2,axis=2))
    max_dis_index=np.where(disTable==disTable.max())
    repres[1]=i[max_dis_index[0]]
    delete_index1=np.where(np.all(i==repres[1],axis=1))
    i=np.delete(i, delete_index1[0][0], axis=0) 
    represent=np.where(repres==0,np.NaN,repres)
    for z in range(0,len(represent)-2):
        mask = np.all(np.isnan(represent) | np.equal(represent, 0), axis=1)
        represent1=represent[~mask]
        verTable = np.tile(represent1,(i.shape[0],1,1))
        verTable2 = np.tile(i,(represent1.shape[0],1,1))
        horTable = np.transpose(verTable2,(1,0,2))
        disTable = np.sqrt(np.sum((verTable - horTable)**2,axis=2)) 
        maxpoint=max(disTable.min(axis=1))
        maxpoint_index=np.where(disTable==maxpoint)[0][0]
        represent[z+2]=i[maxpoint_index]
    
    repres2[repre2Count] = represent
    repre2Count +=1

repres = repres2
for pp in repres.tolist():
    print(pp)

for i in range(repres.shape[0]):
    for j in range(repres[i].shape[0]):
        repres[i][j] = (repres[i][j]-centroids[i])*(1-p)+centroids[i]

fullData = np.loadtxt(sys.argv[2],delimiter=',')
arrangement = []
for point in fullData:
    table1 = np.tile(point,(k,n,1))
    arrangement.append(np.argmin(np.min(np.sum((table1 - repres)**2,axis=2),axis=1)))

outputData = np.empty((len(arrangement),fullData.shape[1]+1))
outputData[:,:-1] = fullData
outputData[:,-1] = arrangement
np.savetxt(sys.argv[6],outputData,delimiter=',',fmt='%f,%f,%d')
