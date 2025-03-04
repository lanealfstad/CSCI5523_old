import pyspark
import argparse
import json
import itertools
import time


parser = argparse.ArgumentParser(description='A2T1')
parser.add_argument('--c', type=int, default=1,
                    help='integer that specifies the case: 1 for Case 1, and 2 for Case 2')
parser.add_argument('--s', type=int, default=8,
                    help='integer that defines the minimum count to qualify as a frequent itemset')
parser.add_argument('--input_file', type=str, default='./backup/data/small2.csv',
                    help='the input file that contains the data')
parser.add_argument('--output_file', type=str, default='./backup/data/a2t1.json',
                    help='the output file contains your answers')

args = parser.parse_args()
case = args.c
threshold = args.s
input_file_path = args.input_file
output_file_path = args.output_file


sc = pyspark.SparkContext('local[*]', 'task1')


def make_subsets(sets, i):
    group = []
    new_sets = []
    for s in sets:
        new_sets.append(set(s))
        for ele in s:
            if ele not in group:
                group.append(ele)
    subsets = []
    for subset in itertools.combinations(group, i):
        valid_subset = True
        for subset2 in itertools.combinations(list(subset), i - 1):
            if set(subset2) not in new_sets:
                valid_subset = False
                break
        if valid_subset:
            subsets.append(list(subset))
    return subsets

def make_subsets2(sets):
    group = []
    for s in sets:
        for ele in s:
            if ele not in group:
                group.append(ele)
    subsets = []
    for subset in itertools.combinations(group, 2):
        subsets.append(list(subset))
    return subsets
'''
def make_subsets(baskets, i, freqItemsets):
    new_sets = []
    for s in freqItemsets:
        new_sets.append(set(s))
    subsets = []
    for bas in baskets:
        for subset in itertools.combinations(bas, i):
            valid_subset = True
            for subset2 in itertools.combinations(list(subset), i - 1):
                if set(subset2) not in new_sets:
                    valid_subset = False
                    break
            if valid_subset and subset not in subsets:
                subsets.append(subset)
    return subsets
'''


def get_subsets(subsets, baskets, thres):
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


def get_subsets2(subsets, baskets, valid_buckets, thres):
    new_baskets = []
    valid = []
    for bas in baskets:
        new_baskets.append(set(bas))
    for subset in subsets:
        if (hash(sorted(subset)[0]) % 20) in valid_buckets:
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

def tupleToList(listofTuples):
    list_of_lists = []
    for tupe in listofTuples:
        list_of_lists.append(list(tupe))
    return list_of_lists

def listToTuple(listOfLists):
    list_of_tuples = []
    for list in listOfLists:
        list_of_tuples.append(tuple(list))
    return list_of_tuples


def numToList(listOfNums):
    list_of_lists = []
    for num in listOfNums:
        list = [num]
        list_of_lists.append(list)
    return list_of_lists


def cust_hash(sets, thres):
    buckets = {}
    valid_buckets = []
    for i in range(0,20):
        buckets[i] = []
    for set in sets:
        bucket = hash(sorted(set)[0]) % 20
        buckets[bucket].append(set)
    for i in range(0,20):
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


def user_get_freq_itemset_candidates(baskets):
    baskets = list(baskets)
    local_threshold = (len(baskets) / numUserBaskets) * threshold

    itemCounts = countItems(baskets) # itemset size 1 only
    freqItems = checkFreq(itemCounts, local_threshold) # itemset size 1 only
    baskets = remove_non_singletons(baskets, freqItems)
    freqItemsets = numToList(freqItems)
    allFreqItemsets = freqItemsets
    size = 2

    subsets = make_subsets2(freqItemsets)
    valid_buckets = cust_hash(subsets, local_threshold)
    freqItemsets = get_subsets2(subsets, baskets, valid_buckets, local_threshold)
    allFreqItemsets += freqItemsets

    while freqItemsets != []:
        size += 1
        subsets = make_subsets(freqItemsets, size)
        freqItemsets = get_subsets(subsets, baskets, local_threshold)
        allFreqItemsets += freqItemsets

    return allFreqItemsets


def bus_get_freq_itemset_candidates(baskets):
    baskets = list(baskets)
    local_threshold = (len(baskets) / numBusBaskets) * threshold

    itemCounts = countItems(baskets)  # itemset size 1 only
    freqItems = checkFreq(itemCounts, local_threshold)  # itemset size 1 only
    baskets = remove_non_singletons(baskets, freqItems)
    freqItemsets = numToList(freqItems)
    allFreqItemsets = freqItemsets
    size = 2

    subsets = make_subsets2(freqItemsets)
    valid_buckets = cust_hash(subsets, local_threshold)
    freqItemsets = get_subsets2(subsets, baskets, valid_buckets, local_threshold)
    allFreqItemsets += freqItemsets

    while freqItemsets != []:
        size += 1
        subsets = make_subsets(freqItemsets, size)
        freqItemsets = get_subsets(subsets, baskets, local_threshold)
        allFreqItemsets += tupleToList(freqItemsets)

    return allFreqItemsets

start_time = time.time()

userBusinessRDD = sc.textFile(input_file_path).map(lambda line: line.split(',')).filter(lambda line: line[0] != 'user_id') #filter removes header

if case == 1:
    userBasketsRDD = userBusinessRDD.groupByKey().mapValues(list).map(lambda user: user[1])
    numUserBaskets = len(userBasketsRDD.collect())

    candidateRDD = userBasketsRDD.mapPartitions(user_get_freq_itemset_candidates).map(lambda can: tuple(sorted(can))).distinct().map(lambda can: list(can))
    candidateGroupsRDD = candidateRDD.groupBy(lambda itemset: len(itemset)).mapValues(list)
    sortCandidateItemsets = candidateGroupsRDD.sortBy(lambda len: len[0]).map(lambda set: set[1])
    candidateAns = sortCandidateItemsets.collect()
    candidates = candidateRDD.collect()

    freqItemsets = get_subsets(list(candidates), list(userBasketsRDD.collect()), threshold)
    freqItemsetsRDD = sc.parallelize(freqItemsets)
    freqItemsetsGroupsRDD = freqItemsetsRDD.groupBy(lambda itemset: len(itemset)).mapValues(list)
    sortFreqItemsets = freqItemsetsGroupsRDD.sortBy(lambda len: len[0]).map(lambda set: set[1])
    frequentItemsetsAns = sortFreqItemsets.collect()

if case == 2:
    businessBasketsRDD = userBusinessRDD.map(lambda user: (user[1], user[0])).groupByKey().mapValues(list).map(lambda bus: bus[1])
    numBusBaskets = len(businessBasketsRDD.collect())

    candidateRDD = businessBasketsRDD.mapPartitions(bus_get_freq_itemset_candidates).map(lambda can: tuple(sorted(can))).distinct().map(lambda can: list(can))
    candidateGroupsRDD = candidateRDD.groupBy(lambda itemset: len(itemset)).mapValues(list)
    sortCandidateItemsets = candidateGroupsRDD.sortBy(lambda len: len[0]).map(lambda set: set[1])
    candidateAns = sortCandidateItemsets.collect()
    candidates = candidateRDD.collect()

    freqItemsets = get_subsets(list(candidates), list(businessBasketsRDD.collect()), threshold)
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

