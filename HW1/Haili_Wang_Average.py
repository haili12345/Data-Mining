from pyspark import SparkContext, SparkConf
import sys
import re



def main():

	sc = SparkContext(conf=SparkConf())
	tf = sc.textFile(sys.argv[1])

	tf = tf.map(lambda line: line.split(",")) \
			.map(lambda line: (line[3],line[18])) \
			.map(lambda (k,x): (k.lower(),x)) \
			.map(lambda (k,x): (re.sub("[-\']","",k),x)) \
			.map(lambda (k,x): (re.sub("[^\\w]"," ",k),x)) \
			.map(lambda (k,x): (re.sub("\\s+"," ",k),x)) \
			.map(lambda (k,x): (re.sub("^\\s","",k),x)) \
			.map(lambda (k,x): (re.sub("\\s$","",k),x)) \
			.filter(lambda (k,x): k!='' and k!='event') \
			.map(lambda (k,x): (k,float(x))) \
			.aggregateByKey((0,0), lambda U,v: (U[0] + v, U[1] + 1), lambda U1,U2: (U1[0]+ U2[0], U1[1] + U2[1])) \
			.map(lambda (k,(x,y)):(k,(x,(float(x/y)))) ) \
			.sortByKey() \
			.map(lambda (k,(x,y)): k+'    '+str(x)+'    '+str(y))

	tf.saveAsTextFile(sys.argv[2])

if __name__ == '__main__':
	main()