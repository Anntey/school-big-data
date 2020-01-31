from pyspark.sql import SparkSession
import re
import os

os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-12.0.2"

spark = SparkSession\
        .builder\
        .appName("sparktweets")\
        .getOrCreate()
sc = spark.sparkContext

tweets = sc.textFile("trump_tweets.csv")
words = tweets.flatMap(lambda line: line.split(','))

# Make pattern for date and time (for example 11-23-2019 17:18:00)
patt = re.compile(r'\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}')

# Delete parts which aren't date and time
datestimes = words.map(lambda word: "" if patt.search(word) == None else word)

# Modify date and time so that only year is left
years = datestimes.map(lambda x: x[6:10])

# Modify date and time so that only year and hour is left
yearshours = datestimes.map(lambda x: x[6:13])

# Make pairs so that key is the year and value is 1
mapped_yearly = years.map(lambda year: (year, 1))

# Make pairs so that key is the year and hour and value is 1
mapped_hourly = yearshours.map(lambda yearhour: (yearhour, 1))

# Calculate total count of tweets per year and divide by 365 to get the average
reduced_yearly = mapped_yearly.reduceByKey(lambda a, b: a + b)
reduced_yearly_final = reduced_yearly.map(lambda x: (x[0], x[1]/365))

reduced_yearly_final.take(25)

year_counts = {
        '': 1,
        '2019': 6560,
        '2017': 2602,
        '2016': 4225,
        '2015': 7536,
        '2014': 5784,
        '2013': 8144,
        '2012': 3531,
        '2011': 774,
        '2010': 142,
        '2018': 3568,
        '2009': 56
}

# Calculate total count of tweets per hour per year and divide by 24 to get hourly distribution
reduced_hourly = mapped_hourly.reduceByKey(lambda a, b: a + b)
reduced_hourly_final = reduced_hourly.map(lambda x: (x[0], (x[1] / year_counts[str(x[0][0:4])]) * 100))

reduced_hourly_final.take(1000)

