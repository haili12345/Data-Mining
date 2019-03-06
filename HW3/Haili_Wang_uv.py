import numpy as np
from sympy import * 
from numpy.random import rand
from numpy import matrix
import math
import time
import sys

##n is the number of  rows (users) of the matrix, while m is the number of columns (movies).f is the number of dimensions/factors in the factor model. 
##That is, U is n-by-f matrix, while V is f-by-m matrix.
##k is the number of iterations.
ratings = sys.argv[1]
n=int(sys.argv[2]) 
m=int(sys.argv[3])
f=int(sys.argv[4])
k=int(sys.argv[5])
U = np.ones( (n,f), dtype=np.float )
V = np.ones( (f,m), dtype=np.float )
U = np.ones( (n,f), dtype=np.float )
V = np.ones( (f,m), dtype=np.float )
M = np.zeros( (n,m), dtype=np.float )
ratings_text=np.loadtxt(ratings,dtype=np.str,delimiter=",")
data=ratings_text[1:,0:3].astype(np.float)
for i in range(0,k):
	pointer=0
	for usrID in data[:,0]:
	    pointer+=1
	    X_index=int(data[pointer-1:pointer,1:2]-1)
	    if X_index<m:
	        M[int(usrID-1)][int(X_index)]=float(data[pointer-1:pointer,2:3])
	M=np.where(M==0,np.NaN,M)
	def updateU(U,V,M,i,n,m):
	    A=np.ones( (n,m), dtype=np.float )
	    C = np.dot(U[:,:i],V[:i,:]) + np.dot(U[:,i+1:],V[i+1:,:])
	    M = M - C
	    XA = A*V[i,:]
	    d=np.argwhere(np.isnan(M))
	    for a in d:
	        XA[a[0],a[1]]=0
	    X2A = np.sum(XA**2,axis=1)
	    X1A=-2*XA*M
	    X1A=np.nan_to_num(X1A)
	    X1A=np.sum(X1A,axis=1)
	    X = -X1A/(2*X2A)
	    return X
	def updateV(U,V,M,i,n,m):
	    A=np.ones( (n,m), dtype=np.float )
	    C = np.dot(U[:,:i],V[:i,:]) + np.dot(U[:,i+1:],V[i+1:,:])
	    M = M - C
	    XA=A*U[:,i:i+1]
	    d=np.argwhere(np.isnan(M))
	    for a in d:
	            XA[a[0],a[1]]=0
	    X2A = np.sum(XA**2,axis=0)
	    X1A=-2*XA*M
	    X1A=np.nan_to_num(X1A)
	    X1A=np.sum(X1A,axis=0)
	    X1A=np.where(X1A==0,2,X1A)
	    X2A=np.where(X2A==0,-1,X2A)
	    X=-X1A/(2*X2A)
	    return X
	for i in range(1,int(U.shape[1])):
	    U[:,i-1:i]=updateU(U,V,M,i-1,n,m).reshape(n,1)
	for j in range(1,int(V.shape[0])):
	    V[j-1:j,:]=updateV(U,V,M,j-1,n,m)
	R=(M-np.dot(U,V))**2
	RM=np.nan_to_num(R)
	k1=np.nan_to_num(M)
	k2=np.count_nonzero(k1)
	b=np.sum(RM)
	R=math.sqrt(float(b)/float(k2))
	print "%.4f" %R