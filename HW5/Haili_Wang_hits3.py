import re
import sys
import os
from operator import add
from pyspark.sql import SparkSession
def computeContribs(urls, rank):
    for url in urls:
        yield (url, rank)
def parseNeighbors(urls):
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]
def parseNeighborsT(urls):
    parts = re.split(r'\s+', urls)
    return parts[1], parts[0]

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    linksT = lines.map(lambda urls: parseNeighborsT(urls)).distinct().groupByKey().cache()
                                                                                                                                    
    h = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    k=int(sys.argv[2])
    for i in range(k):

        a = links.join(h).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        a = a.reduceByKey(add)
        maxRa = a.max(lambda x:x[1])
        a = a.mapValues(lambda a: a /maxRa[1])
        h=linksT.join(a).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        h=h.reduceByKey(add)
        maxRh = h.max(lambda x:x[1])
        h = h.mapValues(lambda h: h /maxRh[1])
    output=sys.argv[3]
    if not os.path.exists(output):
        os.makedirs(output)
    file_a=open(output+'/authority.txt','w')
    file_h=open(output+'/hub.txt','w')        # Collects all URL ranks and dump them to console.
    out=''
    a=a.map(lambda (k,v):(int(k),v)).sortByKey()
    h=h.map(lambda (k,v):(int(k),v)).sortByKey()
    for (link, rank) in a.collect():
    	out=str(link)+','+'%.5f\n'%rank+'\n'
        file_a.write(out)
    for (linkh, rankh) in h.collect():
    	out=str(linkh)+','+'%.5f\n'%rankh+'\n'
        file_h.write(out)

    file_a.close()
    file_h.close()

    spark.stop()
