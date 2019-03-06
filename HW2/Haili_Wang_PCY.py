import sys
import collections
import itertools
import os

def main():

    sum=0.0
    fr=0.0
    data = sys.argv[1]
    a = int(sys.argv[2])
    b = int(sys.argv[3])
    N = int(sys.argv[4])
    s = int(sys.argv[5])
    output_dir=sys.argv[6]
    CountTable={}
    CountTable_bit={}
    singletons=[]
    frequent_pair={}
    condidates={}

    data_open=open(data)
    #pass 1
    ##count table
    hash = {}
    while True:
        line = data_open.readline().rstrip().split(",")
        
        
        data_lines = []
        if len(line)>1:
            for key in line:
                data_lines.append(int(key))
            for pair in itertools.combinations(data_lines, 2):
                    ##if pair[0]<pair[1]:
                    hash_index = (a* pair[0] + b* pair[1])%N
                    hash.setdefault(hash_index, 0)
                    hash[hash_index] = hash[hash_index] + 1

            for key2 in data_lines:
                CountTable.setdefault(key2, 0)
                CountTable[key2] = CountTable[key2]+1
        else: 
             
              break

    for hashpair in hash:
        if hash[hashpair] >= s:
            hash[hashpair] = 1
        else:
           hash[hashpair] = 0
    for keyC in CountTable:
        if CountTable[keyC] >= s:
            singletons.append(keyC)
            CountTable_bit[keyC]=1
        else:
            CountTable_bit[keyC]=0
    for hashfre in hash:
        if hash[hashfre]==1:
            fr=fr+1
    False_positive_rate=round(float(fr)/float(N),3)
    print  "False Positive Rate:", False_positive_rate
    
#pass 2
    data_open.seek(0)
    while True:
        data_lines2 = []

        line2 = data_open.readline().rstrip().split(",")
        if len(line2)>1:

            for key3 in line2:
                if CountTable_bit[int(key3)]==1:
                    data_lines2.append(int(key3))
            for pair_pass2 in  itertools.combinations(data_lines2, 2):

                hash_index = (a * pair_pass2[0] + b * pair_pass2[1]) % N

                if hash[hash_index]==1:
                    frequent_pair.setdefault(pair_pass2, 0)
                    frequent_pair[pair_pass2] = frequent_pair[pair_pass2]+1

        else: 
            break
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    file_candidates=open(output_dir+'/candidates.txt','w')
    file_frequents=open(output_dir+'/frequentset.txt','w')

    for co in CountTable:
        if CountTable[co] >= s:
           file_frequents.write(str(list(CountTable.keys())[list(CountTable.values()).index(CountTable[co])])+'\n')
    for fpair in sorted(frequent_pair):
        if frequent_pair[fpair] >= s:
           file_frequents.write(str(list(frequent_pair.keys())[list(frequent_pair.values()).index(frequent_pair[fpair])])+'\n')

        else:
           file_candidates.write(str(list(frequent_pair.keys())[list(frequent_pair.values()).index(frequent_pair[fpair])])+'\n')
    
    
    file_candidates.close()
    file_frequents.close()
    data_open.close()
    ##print CountTable





if __name__ == '__main__':
    main()