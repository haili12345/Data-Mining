#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -*- coding: utf-8 -*-
"""
This is an example implementation of ALS for learning how to use Spark. Please refer to
pyspark.ml.recommendation.ALS for more conventional use.

This example requires numpy (http://www.numpy.org/)
"""
from __future__ import print_function

import sys
import numpy as np
from numpy.random import rand
from numpy import matrix
from pyspark.sql import SparkSession

LAMBDA = 0.01   # regularization
np.random.seed(42)


def rmse(R, ms, us):
    U=ms * us.T
    R=np.array(R)
    index=np.argwhere(np.isnan(R))
    for i in index:
        U[i[0],i[1]]=0
    R=np.nan_to_num(R)
    k=np.count_nonzero(R)
    diff = R - U
    return np.sqrt(np.sum(np.power(diff, 2)) / (k))


def update(i, mat, ratings):
    uu = mat.shape[0]
    ff = mat.shape[1]
    rat1=ratings[i, :]
    rat1=np.array(rat1)
    jb=np.nan_to_num(rat1)
    s=np.sum(jb,axis=1)
    if s[0]==0:
        return matrix(rand(ff,1))
    else:
        index=np.argwhere(np.isnan(rat1))
        l=[]
        for i in index:
            l.append(i[1])
        r=np.delete(rat1,l,axis=1)
        v=np.delete(mat,l,axis=0)
        XtX = v.T * v
        Xty = v.T * r.T

        for j in range(ff):
            XtX[j, j] += LAMBDA * uu

        return np.linalg.solve(XtX, Xty)


if __name__ == "__main__":

    """
    Usage: als [M] [U] [F] [iterations] [partitions]"
    """

    print("""WARN: This is a naive implementation of ALS and is given as an
      example. Please use pyspark.ml.recommendation.ALS for more
      conventional use.""", file=sys.stderr)

    spark = SparkSession\
        .builder\
        .appName("PythonALS")\
        .getOrCreate()

    sc = spark.sparkContext
    rating_file = sys.argv[1]
    ratings_text=np.loadtxt(rating_file,dtype=np.str,delimiter=",")
    M = int(sys.argv[2]) if len(sys.argv) > 1 else 100##M usr
    U = int(sys.argv[3]) if len(sys.argv) > 2 else 500##U movie
    F = int(sys.argv[4]) if len(sys.argv) > 3 else 10
    ITERATIONS = int(sys.argv[5]) if len(sys.argv) > 4 else 5
    partitions = int(sys.argv[6]) if len(sys.argv) > 5 else 2
    R = np.zeros( (M,U), dtype=np.float )

    print("Running ALS with M=%d, U=%d, F=%d, iters=%d, partitions=%d\n" %
          (M, U, F, ITERATIONS, partitions))

    data=ratings_text[1:,0:3].astype(np.float)
    pointer=0
    for usrID in data[:,0]:
        pointer+=1
        X_index=int(data[pointer-1:pointer,1:2]-1)
        if X_index<M:
            R[int(usrID-1)][int(X_index)]=float(data[pointer-1:pointer,2:3])
    R=np.where(R==0,np.NaN,R)

    R = matrix(R)
    ms = matrix(rand(M, F))
    us = matrix(rand(U, F))
    Rb = sc.broadcast(R)
    msb = sc.broadcast(ms)
    usb = sc.broadcast(us)
    out=''
    for i in range(ITERATIONS):
        ms = sc.parallelize(range(M), partitions) \
               .map(lambda x: update(x, usb.value, Rb.value)) \
               .collect()
        # collect() returns a list, so array ends up being
        # a 3-d array, we take the first 2 dims for the matrix
        ms = matrix(np.array(ms)[:, :, 0])
        msb = sc.broadcast(ms)

        us = sc.parallelize(range(U), partitions) \
               .map(lambda x: update(x, msb.value, Rb.value.T)) \
               .collect()
        us = matrix(np.array(us)[:, :, 0])
        usb = sc.broadcast(us)

        error = rmse(R, ms, us)
        print("Iteration %d:" % i)
        print("\nRMSE: %5.4f\n" % error)
        out+='%.4f\n'%error
    f=open(sys.argv[7],'w')
    f.write(out)
    f.close()
   
    spark.stop()
