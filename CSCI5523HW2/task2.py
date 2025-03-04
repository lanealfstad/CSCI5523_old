import pyspark
import argparse
import json
import itertools
import time


parser = argparse.ArgumentParser(description='A2T2')
parser.add_argument('--k', type=int, default=10,
                    help='integer that is used to filter out qualified users')
parser.add_argument('--s', type=int, default=20,
                    help='integer that defines the minimum count to qualify as a frequent itemset')
parser.add_argument('--input_file', type=str, default='./backup/data/nv.csv',
                    help='the input file that contains the data')
parser.add_argument('--output_file', type=str, default='./backup/data/a2t2.json',
                    help='the output file contains your answers')

args = parser.parse_args()
k = args.k
threshold = args.s
input_file_path = args.input_file
output_file_path = args.output_file

sc = pyspark.SparkContext('local[*]', 'task1')


def get_subsets2(baskets, i, thres):
    counts = {}
    for bas in baskets:
        for subset in itertools.combinations(bas, i):
            subset = tuple(sorted(set(subset)))
            if subset not in counts.keys():
                counts[subset] = 1
            else:
                counts[subset] += 1
    freq_itemsets = []
    for key in counts:
        if counts[key] >= thres:
            freq_itemsets.append(list(key))
    return freq_itemsets


def get_subsets(baskets, i, freqItemsets, thres):
    counts = {}
    for bas in baskets:
        for subset in itertools.combinations(bas, i):
            subset = tuple(sorted(set(subset)))
            if subset not in counts.keys():
                valid_subset = True
                for subset2 in itertools.combinations(list(subset), i - 1):
                    if list(sorted(set(subset2))) not in freqItemsets:
                        valid_subset = False
                        break
                if valid_subset:
                    counts[subset] = 1
            else:
                counts[subset] += 1
    freq_itemsets = []
    for key in counts:
        if counts[key] >= thres:
            freq_itemsets.append(list(key))
    return freq_itemsets


def check_subsets(subsets, baskets, thres):
    new_baskets = []
    valid = []
    for bas in baskets:
        new_baskets.append(set(bas))
    for subset in subsets:
        count = 0
        for bas in new_baskets:
            if set(subset).issubset(bas):
                count += 1
        if count >= thres:
            valid.append(subset)
    return valid

def countItems(baskets):
    counts = {}
    for bas in baskets:
        for i in bas:
            if i not in counts.keys():
                counts[i] = 0
            counts[i] += 1
    return counts

def checkFreq(counts, thres):
    freq_itemsets = []
    for key in counts:
        if counts[key] >= thres:
            freq_itemsets.append(key)
    return freq_itemsets


def numToList(listOfNums):
    list_of_lists = []
    for num in listOfNums:
        list = [num]
        list_of_lists.append(list)
    return list_of_lists


def cust_hash(sets, thres):
    buckets = {}
    valid_buckets = []
    for i in range(0,30000000):
        buckets[i] = []
    for set in sets:
        bucket = hash(sorted(set)[0]) % 30000000
        buckets[bucket].append(set)
    for i in range(0,30000000):
        if len(buckets[i]) >= thres:
            valid_buckets.append(i)
    return valid_buckets

def remove_non_singletons(baskets, singletons):
    new_baskets = []
    for bas in baskets:
        for i in bas:
            if i not in singletons:
                bas.remove(i)
        new_baskets.append(bas)
    return new_baskets


def remove_non_freq(baskets, itemsets):
    group = []
    for s in itemsets:
        for ele in s:
            if ele not in group:
                group.append(ele)
    new_baskets = []
    for bas in baskets:
        for i in bas:
            if i not in group:
                bas.remove(i)
        new_baskets.append(bas)
    return new_baskets


def user_get_freq_itemset_candidates(baskets):
    baskets = list(baskets)
    local_threshold = (len(baskets) / numUserBaskets) * threshold

    itemCounts = countItems(baskets) # itemset size 1 only
    freqItems = checkFreq(itemCounts, local_threshold) # itemset size 1 only
    baskets = remove_non_singletons(baskets, freqItems)
    freqItemsets = numToList(freqItems)
    allFreqItemsets = freqItemsets
    size = 2


    freqItemsets = get_subsets2(baskets, size, local_threshold)
    allFreqItemsets += freqItemsets

    while freqItemsets != []:
        size += 1
        baskets = remove_non_freq(baskets, freqItemsets)
        freqItemsets = get_subsets(baskets, size, freqItemsets, local_threshold)
        allFreqItemsets += freqItemsets

    return allFreqItemsets

start_time = time.time()

userBusinessRDD = sc.textFile(input_file_path).map(lambda line: line.split(',')).filter(lambda line: line[0] != 'user_id') #filter removes header

userBasketsRDD = userBusinessRDD.groupByKey().mapValues(list).filter(lambda user: len(set(user[1])) > k).map(lambda user: list(set(user[1])))
numUserBaskets = len(userBasketsRDD.collect())

candidateRDD = userBasketsRDD.mapPartitions(user_get_freq_itemset_candidates).map(lambda can: tuple(sorted(can))).distinct().map(lambda can: list(can)).sortBy(lambda can: can[0])
candidateGroupsRDD = candidateRDD.groupBy(lambda itemset: len(itemset)).mapValues(list)
sortCandidateItemsets = candidateGroupsRDD.sortBy(lambda len: len[0]).map(lambda set: set[1])
candidateAns = sortCandidateItemsets.collect()
candidates = candidateRDD.collect()

freqItemsets = check_subsets(list(candidates), list(userBasketsRDD.collect()), threshold)
freqItemsetsRDD = sc.parallelize(freqItemsets)
freqItemsetsGroupsRDD = freqItemsetsRDD.groupBy(lambda itemset: len(itemset)).mapValues(list)
sortFreqItemsets = freqItemsetsGroupsRDD.sortBy(lambda len: len[0]).map(lambda set: set[1])
frequentItemsetsAns = sortFreqItemsets.collect()


end_time = time.time()
runtime = round((end_time - start_time) * 100) / 100

Ans = {"Candidates": candidateAns, "Frequent Itemsets": frequentItemsetsAns, "Runtime": runtime}

f = open(output_file_path, "w")
f.write(json.dumps(Ans))
f.close()
