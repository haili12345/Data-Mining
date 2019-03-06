import sys
import itertools
from pyspark import SparkContext, SparkConf

def pairFilter(p,frequent,length):
	if length==1:
		return True
	sp = list(itertools.combinations(p,length-1))
	f = True
	for e in sp:
		f = f and (e in frequent)
	return f

def apriori(p, s):
	ans = []
	frequent = []
	text = list(p)
	for i in range(len(text)):
		text[i] = text[i].split(',')
		for j in range(len(text[i])):
			text[i][j] = int(text[i][j])
	threshold  = float(s) * len(text)
	length = 1
	while True:
		allPairs = []
		for line in text:
			tp = list(itertools.combinations(line,length))
			tp = filter(lambda p: pairFilter(p,frequent,length),tp)
			allPairs.extend(tp)
		removeList = []
		for p in allPairs:
			if allPairs.count(p) < threshold and (p not in removeList):
				removeList.append(p)
		for rl in removeList:
			allPairs = filter(lambda a: a!=rl,allPairs)
		allPairs.sort()
		ans.extend(allPairs)
		frequent = allPairs
		length += 1
		if len(frequent) < 1:
			break;
	return list(set(ans))

def combinations(line,maxLength):
	a = [list(itertools.combinations(line,n)) for n in range(1,maxLength+1)]
	return [item for line in a for item in line]

def toInt(line):
	for i in range(len(line)):
		line[i] = int(line[i])
	return line

def constructFile(result,maxLength):
	ans = [''] * maxLength
	tempList = []
	for r in result:
		if len(r)==1:
			tempList.append(r)
	tempList.sort()
	for r in tempList:
		ans[0] = ans[0] + str(r[0]) + '\n'
	for i in range(1,len(ans)):
		tempList = []
		for r in result:
			if len(r)==i+1:
				tempList.append(r)
		tempList.sort()
		for r in tempList:
			temp = ''
			for v in r:
				temp = temp + str(v) + ','
			temp = '(' + temp[:-1] + ')\n'
			ans[i] = ans[i] + temp
	ansStr = ''
	for s in ans:
		ansStr += s
	return ansStr

def main():
	sc = SparkContext(conf=SparkConf())
	tf = sc.textFile(sys.argv[1],2)
	support = float(sys.argv[2])
	candidates = tf.mapPartitions(lambda p: apriori(p,support)).collect()

	maxLength = 0
	for c in candidates:
		maxLength = maxLength if len(c) < maxLength else len(c)

	bucketNum = tf.count()
	result = tf.map(lambda line: line.split(',')) \
				.map(lambda line: toInt(line)) \
				.map(lambda line: filter(lambda p: p in candidates,combinations(line,maxLength))) \
				.flatMap(lambda pair: pair) \
				.map(lambda pair: (pair,1)) \
				.reduceByKey(lambda v1,v2: v1+v2) \
				.filter(lambda (pair,v): v>= support * bucketNum) \
				.map(lambda (k,v): k) \
				.collect()
	f = open(sys.argv[3],'w')
	f.write(constructFile(result,maxLength))
	f.close()

if __name__ == '__main__':
	main()
