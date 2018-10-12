from pyspark import SparkConf, SparkContext 
import sys

if __name__ == "__main__":
	my_conf = SparkConf().setAppName('part2')
	sc = SparkContext(conf=my_conf)
	csv_file_path = sys.argv[1]

	my_rdd = sc.textFile(csv_file_path)
	my_rdd2 = my_rdd.filter(lambda l: not l.startswith("battery_level"))

	sorted_rdd=my_rdd2.sortBy( lambda line: (line.split(",")[2],line.split(",")[-1]) )
	sorted_rdd.coalesce(1).saveAsTextFile(sys.argv[2])
