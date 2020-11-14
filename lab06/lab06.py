import sys
from pyspark import SparkConf
from collections import namedtuple
from pyspark.sql import SparkSession
import json

DataRecord = namedtuple("DataRecord", ["item_id", "rating"])
ITEM_ID = 288
PRINT_COUNT = 5
JSON_FILE = 'data_out\lab06.json'

def print_rdd(input_rdd, row_count):
    # Preview u.data
    print("Printing...")
    u_data_list = input_rdd.collect()
    for i, u_data_row in enumerate(u_data_list):
        print(u_data_row)
        if i == row_count: break


if __name__ == "__main__":
    conf = SparkConf() \
        .setMaster("local[3]") \
        .setAppName("HelloRDD")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext

    print("Start")

    if len(sys.argv) != 2:
        print(f"len(sys.argv): {len(sys.argv)}")
        sys.exit(-1)

    print(f"len(sys.argv): {len(sys.argv)}")

    # Read txt-files
    u_data_rdd = sc.textFile(sys.argv[1])

    # Preview src-dataset
    print_rdd(input_rdd=u_data_rdd, row_count=PRINT_COUNT)

    # 1
    # Process data for item_id == 288
    colsRDD = u_data_rdd.map(lambda line: line.split("\t"))
    selectRDD = colsRDD.map(lambda cols: DataRecord(int(cols[1]), int(cols[2])))
    filteredRDD = selectRDD.filter(lambda r: r.item_id == ITEM_ID)

    # Group and sort data
    kvRDD = filteredRDD.map(lambda r: (r.rating, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)
    sortRDD = countRDD.sortByKey()
    # Preview
    print_rdd(input_rdd=sortRDD, row_count=PRINT_COUNT)
    resultRDD = sortRDD.map(lambda r: (r[1]))
    # Preview
    print_rdd(input_rdd=resultRDD, row_count=PRINT_COUNT)

    hist_film = resultRDD.collect()

    # 2
    # Process data for all item_id
    colsRDD = u_data_rdd.map(lambda line: line.split("\t"))
    selectRDD = colsRDD.map(lambda cols: DataRecord(int(cols[1]), int(cols[2])))

    # Group and sort data
    kvRDD = selectRDD.map(lambda r: (r.rating, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)
    sortRDD = countRDD.sortByKey()
    # Preview
    print_rdd(input_rdd=sortRDD, row_count=PRINT_COUNT)
    resultRDD = sortRDD.map(lambda r: (r[1]))
    # Preview
    print_rdd(input_rdd=resultRDD, row_count=PRINT_COUNT)

    hist_all = resultRDD.collect()

    # 3
    result = {'hist_film': hist_film, 'hist_all': hist_all}

    with open(JSON_FILE, 'wt', encoding='utf-8') as f:
        print(json.dumps(result))
        # f.write(json.dumps(result))
        f.writelines(json.dumps(result))

    print("Finish")
