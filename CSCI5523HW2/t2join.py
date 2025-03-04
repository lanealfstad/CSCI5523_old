import pyspark
import argparse
import json

parser = argparse.ArgumentParser(description='A2T2 join')
parser.add_argument('--review_file', type=str, default='./backup/data/review.json', help='the review file ')
parser.add_argument('--business_file', type=str, default='./backup/data/business.json', help='the business file ')
parser.add_argument('--output_file', type=str, default='./backup/data/nv.csv',
                    help='the output file contains your answers')

args = parser.parse_args()
review_file_path = args.review_file
business_file_path = args.business_file
output_file_path = args.output_file

sc = pyspark.SparkContext('local[*]', 'task2')

reviewRDD = sc.textFile(review_file_path).map(lambda x: json.loads(x))
businessRDD = sc.textFile(business_file_path).map(lambda x: json.loads(x))


nvBusinessRDD = businessRDD.filter(lambda bus: bus['state'] == 'NV')

busUserRDD = reviewRDD.map(lambda review: (review['business_id'], review['user_id']))
busBusRDD = nvBusinessRDD.map(lambda business: (business['business_id'], business['business_id']))

userBusinessRDD = busUserRDD.join(busBusRDD).map(lambda x: x[1])
userAndBusiness = userBusinessRDD.collect()

f = open(output_file_path, 'w')
f.write("user_id, business_id" + "\n")
for ele in userAndBusiness:
    f.write(ele[0] + "," + ele[1] + "\n")
f.close()
