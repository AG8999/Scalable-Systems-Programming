import sys
import time
import os
from pyspark import SparkContext, SparkConf
from itertools import combinations

def generate_candidates(row, k):
    candidates = {}
    items = row.split(',')
    for item in items:
        clean_item = item.strip().replace('"', '')  # Remove leading/trailing whitespace and quotation marks
        candidates[(clean_item,)] = candidates.get((clean_item,), 0) + 1  # Single-item sets
    for subset in combinations(items, k):
        candidate = tuple(sorted(subset))  # Convert to tuple and sort for consistent representation
        candidates[candidate] = candidates.get(candidate, 0) + 1
    return candidates.items()


def filter_frequent(candidate_counts, min_support):
    frequent_itemsets = {}
    for itemset, count in candidate_counts:
        if count >= min_support:
            frequent_itemsets[itemset] = count
    return frequent_itemsets.items()

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: python SON_ALGO.py <input_file> <max_k>")
        sys.exit(1)

    input_file = sys.argv[1]
    #min_confidence = float(sys.argv[2])
    max_k = int(sys.argv[2])

    print(f"Input file: {input_file}")
    print(f"Maximum size of itemsets (K): {max_k}")

    support_values = [0.1, 0.2, 0.3]
    num_chunks = 5 
    
    frequent_items_folder = "Frequent_items"
    computation_time_folder = "Time_results"
    os.makedirs(frequent_items_folder, exist_ok=True)
    os.makedirs(computation_time_folder, exist_ok=True)

    conf = SparkConf().setAppName("Frequrent Rules Mining")
    sc = SparkContext(conf=conf)

    # Read input data from the input file
    input_data = sc.textFile(input_file)
    Total_transactions = input_data.count()

    print("Reading input data...")

    frequent_itemsets_all = {}
    computation_times_all = {}

    total_start_time = time.time()

    chunks = input_data.randomSplit([1.0 / num_chunks] * num_chunks)

    print("Running SON algorithm...")

    for support in support_values:
        start_time = time.time()

        frequent_itemsets = []
        for chunk in chunks:
            current_support = support * chunk.count()  # Calculate support from percentage
            for k in range(1, max_k + 1):  # Generate itemsets of sizes 1 to max_k
                candidate_counts = chunk.flatMap(lambda row: generate_candidates(row, k)) \
                                        .reduceByKey(lambda x, y: x + y) \
                                        .collect()
                frequent_itemsets.extend(filter_frequent(candidate_counts, current_support))

        combined_candidate_counts = input_data.flatMap(lambda row: generate_candidates(row, max_k)) \
                                              .reduceByKey(lambda x, y: x + y) \
                                              .collect()

        frequent_itemsets_combined = filter_frequent(combined_candidate_counts, current_support)

        frequent_itemsets_all[support] = frequent_itemsets_combined
        computation_times_all[support] = time.time() - start_time

    print("Writing frequent itemsets and computation times to files...")

    for support, frequent_itemsets in frequent_itemsets_all.items():
        with open(os.path.join(frequent_items_folder, f"frequent_itemsets_support_{support}.txt"), "w") as f:
            for itemset, support_count in frequent_itemsets:
                f.write(f"{itemset}: {support_count}\n")
        with open(os.path.join(computation_time_folder, f"computation_times_support_{support}.txt"), "a") as f:
            f.write(f"Computation Time: {computation_times_all[support]} seconds , With Support as {support} and Passes {max_k} where no of transaction are {Total_transactions} \n")

    print("Successfully Executed SON Algorithms")

    # Stop SparkContext
    sc.stop()
