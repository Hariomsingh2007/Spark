''' This is Code to word count
Author : Hari om Singh'''


from pyspark import SparkConf, SparkContext

from operator import add
import sys
## Constants
APP_NAME = " HelloWorld of Big Data"
##OTHER FUNCTIONS/CLASSES

def main(sc ,filename):
    textRDD = sc.textFile(filename)
    print textRDD.collect()

    # textRDD.saveAsTextFile('/user/cloudera/spark/files/people_result')
    words = textRDD.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1))
    # words.saveAsTextFile('/user/cloudera/spark/files/people_result')
    wordcount = words.reduceByKey(lambda a ,b : a +b).collect()
    # wordcount.saveAsTextFile('/user/cloudera/spark/files/people_result')
    for wc in wordcount:
        print wc[0] ,wc[1]

if __name__ == "__main__":

    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    filename = sys.argv[1]
    print filename
    print "hai harry"
    # Execute Main functionality
    main(sc, filename)


