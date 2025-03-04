import pyspark
import argparse
import json

parser = argparse.ArgumentParser(description='A1T1')
parser.add_argument('--input_file', type=str, default='./backup/data/hw1/review.json', help='the input file ')
parser.add_argument('--output_file', type=str, default='./backup/data/hw1/a1t1.json',
                    help='the output file contains your answers')
parser.add_argument('--stopwords', type=str, default='./backup/data/hw1/stopwords',
                   help='the file contains the stopwords')
parser.add_argument('--y', type=int, default=2018, help='year')
parser.add_argument('--m', type=int, default=10, help='top m users')
parser.add_argument('--n', type=int, default=10, help='top n frequent words')

args = parser.parse_args()
input_file_path = args.input_file

sc = pyspark.SparkContext('local[*]', 'task1')

jsonRDD = sc.textFile(input_file_path).map(lambda x: json.loads(x))


# Part A
A = jsonRDD.count()

# Part B
year = str(args.y)
jsonYearRDD = jsonRDD.filter(lambda x: year in x['date'])
B = jsonYearRDD.count()

# Part C
usersRDD = jsonRDD.map(lambda review: [review['user_id'], 1]).reduceByKey(lambda x, y: x + y)
C = usersRDD.count()

# Part D
m = args.m
topUsersRDD = usersRDD.sortBy(lambda user: user[0]).sortBy(lambda user: -user[1])
D = topUsersRDD.take(m)

# Part E
stopwords_file_path = args.stopwords
stopwordsList = sc.textFile(stopwords_file_path).flatMap(lambda line: line.split(' ')).collect()

punct = ["(", "[", ",", ".", "!", "?", ":", ";", "]", ")"]

def remove_punct(word):
    new_word = ''
    for letter in word:
        if letter not in punct:
            new_word += letter
    return new_word


n = args.n
wordsRDD = jsonRDD.map(lambda review: review['text']).flatMap(lambda line: line.split(" "))
wordsCountsRDD = wordsRDD.map(lambda word: [remove_punct(word.lower()), 1]).reduceByKey(lambda x, y: x + y)
noStopWords = wordsCountsRDD.filter(lambda word: word[0] not in stopwordsList)
topWordCountsRDD = noStopWords.sortBy(lambda word: word[0]).sortBy(lambda word: -word[1])
topWordsRDD = topWordCountsRDD.map(lambda word: word[0]).filter(lambda word: word != "")
E = topWordsRDD.take(n)



# Writing Answers
Ans = {"A": A, "B": B, "C": C, "D": D, "E": E}

output_file_path = args.output_file
f = open(output_file_path, "w")
f.write(json.dumps(Ans))
f.close()
