import pyspark
import argparse
import json

parser = argparse.ArgumentParser(description='A1T2')
parser.add_argument('--input_file', type=str, default='./backup/data/hw1/review.json', help='the input file ')
parser.add_argument('--output_file', type=str, default='./backup/data/hw1/a1t3_d.json',
                    help='the output file contains your answers')
parser.add_argument('--n', type=int, default=10, help='the threshold of the number of reviews')

args = parser.parse_args()
review_file_path = args.input_file
n = args.n

sc = pyspark.SparkContext('local[*]', 'task3_default')

reviewRDD = sc.textFile(review_file_path).map(lambda x: json.loads(x))
businessReviewsRDD = reviewRDD.map(lambda review: (review['business_id'], 1)).reduceByKey(lambda r1, r2: r1 + r2)
moreThanNReviews = businessReviewsRDD.filter(lambda bus: bus[1] > n)
result = moreThanNReviews.collect()

n_partitions = moreThanNReviews.getNumPartitions()
partitions = moreThanNReviews.glom().collect() # groups rdd elements by partition

n_items = []
for part in partitions:
    n_items.append(len(part))

Ans = {"n_partitions": n_partitions, "n_items": n_items, "result": result}


output_file_path = args.output_file
f = open(output_file_path, "w")
f.write(json.dumps(Ans))
f.close()