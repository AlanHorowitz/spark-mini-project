from pyspark import SparkContext
from operator import add


def extract_vin_key_value(line):
    cols = line.strip().split(",")
    key = cols[2]
    value = ([cols[1], cols[3], cols[5]])
    return key, value


def populate_make(values):
    """ Return all accidents for VIN, with make and year extracted from initial sale."""
    make, year = '', ''
    for value in values:
        if value[0] == 'I':
            make, year = value[1], value[2]
            break
    return [['A', make, year] for value in values if value[0] == 'A']


def extract_make_key_value(x):
    return x[1]+'-'+x[2], 1


def to_csv_format(x):
    return x[0] + ',' + str(x[1])


sc = SparkContext('local', 'spark-mini-project')
raw_rdd = sc.textFile('/user/admin/input/data.csv')

# key = VIN; value = Incident type, make, year
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))
# filter only accidents per VIN, including year and make
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))
# form output Key and add 1 as base to count them
make_kv = enhance_make.map(lambda x: extract_make_key_value(x))
# Count accidents by make, model and convert to csv format
accident_counts = make_kv.reduceByKey(add).map(to_csv_format)
accident_counts.saveAsTextFile("/user/admin/output/")


