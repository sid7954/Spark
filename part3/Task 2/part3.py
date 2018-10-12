from pyspark.sql import SparkSession
import pyspark
import re
import sys

def calculate_pagerank(neighbors, current_rank):
    result=[]
    for n in neighbors:
        result.append((n, current_rank/len(neighbors)))
    return result

if __name__ == "__main__":
    sc = SparkSession.builder.appName("part3").getOrCreate()
    lines = sc.read.text(sys.argv[1]).rdd.map(lambda l: l[0])

    lines=lines.filter(lambda l: ("\t" in l) and ((":" not in l) or ("\tCategory:" in l)) and (not l.startswith("The files")) and (len(re.split(r"\t",l.strip()))>1))    

    edges = lines.map(lambda node: (re.split(r"\t",node.strip())[0].lower(), re.split(r"\t",node.strip())[1].lower() ) ).distinct().groupByKey().partitionBy(int(sys.argv[3])) 
    pagerank = edges.map(lambda edge: (edge[0], 1.0))
    for i in range(10):
        pagerank = edges.join(pagerank).flatMap(lambda item: calculate_pagerank(item[1][0], item[1][1]) ).reduceByKey(lambda a,b : a+b).mapValues(lambda r: 0.15 +  0.85*r)
    pagerank.coalesce(1).saveAsTextFile(sys.argv[2])
