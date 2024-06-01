import os
import sys
import time
from pyspark import SparkContext, SparkConf
from itertools import combinations

def preprocess_line(line):
    # Remove double quotes from the line
    return line.replace('"', '')

def apriori(transactions, min_support, max_k):
    # Step 1: Find frequent 1-itemsets
    #clean_item = re.sub(r'[^a-zA-Z0-9]', '', transactions)
    single_itemsets = transactions.flatMap(lambda items: [(item, 1) for item in items])\
                        .reduceByKey(lambda x, y: x + y) \
                        .filter(lambda x: x[1] >= min_support) \
                        .map(lambda x: (x[0], x[1]))

    frequent_itemsets = single_itemsets.collect()

    # Step 2: Generate candidate itemsets and filter with min_support
    k = 2
    while k <= max_k:
        candidates = transactions.flatMap(lambda items: combinations(items, k)) \
                            .map(lambda itemset: (tuple(sorted(itemset)), 1)) \
                            .reduceByKey(lambda x, y: x + y) \
                            .filter(lambda x: x[1] >= min_support) \
                            .map(lambda x: (x[0], x[1]))

        candidates_list = candidates.collect()
        if not candidates_list:
            break

        frequent_itemsets.extend(candidates_list)
        k += 1

    return frequent_itemsets, k

if __name__ == "__main__":
    input_file = sys.argv[1]
    # Create folders for frequent itemsets and time outputs if they don't exist
    folder_frequent_items = "frequentItems_APRIORI"
    folder_computation_time = "computationTime_APRIORI"
    os.makedirs(folder_frequent_items, exist_ok=True)
    os.makedirs(folder_computation_time, exist_ok=True)

    # Create a SparkContext and generate input data
    print("Reading input data....")
    conf = SparkConf().setAppName("APRIORI ALGORITHM")
    sc = SparkContext(conf=conf)
    input_data = sc.textFile(input_file).map(preprocess_line)
    transactions = input_data.map(lambda line: line.strip().split(','))
    print("Completed reading input data....")

    # Store frequent itemsets and computation times for different support values
    frequent_itemsets_all = {}
    computation_time = {}

    # Determine support values from user input
    maximum_k = 4
    support_abs_value = [0.01,0.02,0.4,0.5,0.025]
    print("Support values are: ")
    for support in support_abs_value:
        print(support)


    # APRIORI Algorithm
    print("Running APRIORI algorithm....")

    for support in support_abs_value:
        # Measure computation time for each support value
        start_time = time.time()

        # Call APRIORI function
        min_support = support * transactions.count()
        frequent_itemset_iter, k = apriori(transactions, min_support, maximum_k)
        
        # Store frequent itemsets and computation time
        frequent_itemsets_all[support]  = frequent_itemset_iter
        computation_time[support] = time.time() - start_time

    print("Writing frequent itemsets and computation times to files....")
    # Write frequent itemsets and computation times to separate files
    for support, frequent_itemsets in frequent_itemsets_all.items():
        with open(os.path.join(folder_frequent_items, f"APRIORI_frequent_itemsets_support_{support}.txt"), "w") as f:
            for itemset, frequency in frequent_itemsets:
                f.write(f"{itemset}: {frequency}\n")
        with open(os.path.join(folder_computation_time, f"APRIORI_computation_times_support_{support}.txt"), "a") as f:
            f.write(f"Computation Time : {computation_time[support]} seconds , Support : {support} , Passes : {k} \n")

    # Finish running APRIORI Algorithm
    print("Completed running APRIORI Algorithm. Output written to folder.")

    # Stop SparkContext
    sc.stop()

