from pyspark import SparkContext


def extract_vin_key_value(line):
    cols = line.strip().split(",")
    key = cols[2]
    value = ([cols[1], cols[3], cols[5]])
    return key, value


def populate_make(values):
    """expect iterable of values"""

    make, year = '', ''
    for value in values:
        print(value)
        if value[0] == 'I':
            make, year = value[1], value[2]
            break
    return [['A', make, year] for value in values if value[0] == 'A']


sc = SparkContext('local','spark-mini-project')
raw_rdd = sc.textFile('data.csv')

vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))
print(type(vin_kv))
o = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))
print(o.collect())