import pyspark
import argparse
import json

parser = argparse.ArgumentParser(description='A1T2')
parser.add_argument('--review_file', type=str, default='./backup/data/hw1/review.json', help='the review file ')
parser.add_argument('--business_file', type=str, default='./backup/data/hw1/business.json', help='the business file ')
parser.add_argument('--output_file', type=str, default='./backup/data/hw1/a1t2.json',
                    help='the output file contains your answers')
parser.add_argument('--n', type=int, default=10, help='top n categories with highest average stars')

args = parser.parse_args()
review_file_path = args.review_file
business_file_path = args.business_file
n = args.n

sc = pyspark.SparkContext('local[*]', 'task2')

reviewRDD = sc.textFile(review_file_path).map(lambda x: json.loads(x))
businessRDD = sc.textFile(business_file_path).map(lambda x: json.loads(x))

businessStarsRDD = reviewRDD.map(lambda review: (review['business_id'],  review['stars']))
businessCategoriesRDD = businessRDD.map(lambda business: (business['business_id'], business['categories']))

# Joining
joinedRDD = businessCategoriesRDD.join(businessStarsRDD)
joinedNoNull = joinedRDD.filter(lambda bus: bus[1][0] is not None)

# Splitting Categories
CategoriesRatingsRDD = joinedNoNull.map(lambda business: (business[1][0].split(", "), business[1][1]))
print(CategoriesRatingsRDD.take(5))

# function for splitting categories:
def split_cats(categories):
    cats_and_ratings = []
    for cat in range(len(categories[0])):
        cats_and_ratings.append((categories[0][cat], categories[1]))
    return cats_and_ratings


SplitCategoriesRatingsRDD = CategoriesRatingsRDD.flatMap(lambda categories: split_cats(categories))
print(SplitCategoriesRatingsRDD.take(5))
GroupedCategoriesRDD = SplitCategoriesRatingsRDD.groupByKey().map(lambda cat: (cat[0], sum(cat[1]) / len(cat[1])))
print(GroupedCategoriesRDD.take(5))
SortedCategoriesRDD = GroupedCategoriesRDD.sortBy(lambda cat: cat[0]).sortBy(lambda cat: -cat[1])
result = SortedCategoriesRDD.take(n)
print(result)
'''
Ans = {"result": result}

output_file_path = args.output_file
f = open(output_file_path, "w")
f.write(json.dumps(Ans))
f.close()
'''



