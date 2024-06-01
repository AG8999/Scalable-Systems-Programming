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

# Function to filter frequent itemsets based on support threshold
def filter_frequent(candidate_counts, min_support):
    frequent_itemsets = {}
    for itemset, count in candidate_counts:
        if count >= min_support:
            frequent_itemsets[itemset] = count
    return frequent_itemsets.items()

def generate_association_rules(frequent_itemsets, min_confidence):
    association_rules = []
    for itemset in frequent_itemsets:
        support = frequent_itemsets[itemset]
        if len(itemset) > 1:
            for i in range(1, len(itemset)):
                for antecedent in combinations(itemset, i):
                    antecedent = tuple(sorted(antecedent))
                    consequent = tuple(sorted(set(itemset) - set(antecedent)))
                    antecedent_support = frequent_itemsets.get(antecedent, 0)
                    if antecedent_support != 0:
                        confidence = support / antecedent_support
                        lift = confidence / ((frequent_itemsets[consequent] / len(frequent_itemsets)))
                        if confidence >= min_confidence:
                            association_rules.append((antecedent, consequent, confidence, lift))
    return association_rules

if __name__ == "__main__":
    # Create a SparkContext
    conf = SparkConf().setAppName("Association Rules Mining")
    sc = SparkContext(conf=conf)
    input_file = sys.argv[1]
    # Input data
    input_data = sc.textFile(input_file)

    # Set initial parameters
    min_confidence = 0.1
    max_k = 3  # Maximum size of itemsets to consider
    num_chunks = 4 # Number of chunks to divide the data

    # Support values to test
    support_values = [0.01]  # Example support values as percentages

    # Store association rules and computation times for different support values
    association_rules_all = {}
    #computation_times_all = {}

    # Measure total computation time
    # total_start_time = time.time()
    
    frequent_items_folder = "Frequent_items"
    computation_time_folder = "Time_results"
    association_rules_folder = "Association_rules"
    os.makedirs(frequent_items_folder, exist_ok=True)
    os.makedirs(computation_time_folder, exist_ok=True)
    os.makedirs(association_rules_folder, exist_ok=True)
    
    # Divide data into chunks
    chunks = input_data.randomSplit([1.0 / num_chunks] * num_chunks)

    for support in support_values:
        # Measure computation time for each support value
        start_time = time.time()

        # Step 1: Run SON algorithm for each chunk
        frequent_itemsets = []
        for chunk in chunks:
            current_support = support * chunk.count()  # Calculate support from percentage
            for k in range(1, max_k + 1):  # Generate itemsets of sizes 1 to max_k
                candidate_counts = chunk.flatMap(lambda row: generate_candidates(row, k)) \
                                        .reduceByKey(lambda x, y: x + y) \
                                        .collect()
                frequent_itemsets.extend(filter_frequent(candidate_counts, current_support))

        # Step 2: Combine and filter frequent itemsets from all chunks
        frequent_itemsets_combined = filter_frequent(frequent_itemsets, current_support)

        # Step 3: Generate association rules from frequent itemsets
        association_rules = generate_association_rules(dict(frequent_itemsets_combined), min_confidence)

        # Store association rules and computation time
        association_rules_all[support] = association_rules
        #computation_times_all[support] = time.time() - start_time

    # Output association rules and computation times for different support values
    for support, association_rules in association_rules_all.items():
        print(f"<<<<<<<<<<- Support Value: {support} ->>>>>>>>>>>>>>")
        for antecedent, consequent, confidence, lift in association_rules:
            print(f"Rule: {antecedent} => {consequent}, Confidence: {confidence}, Lift: {lift}")
        with open(os.path.join(association_rules_folder, f"association_rules_support_{support}.txt"), "w") as f:
            for antecedent, consequent, confidence , lift in association_rules_all[support]:
                f.write(f"Antecedent: {antecedent}, Consequent: {consequent}, Confidence: {confidence}\n, Lift: {lift}\n")

       # print(f"Computation Time: {computation_times_all[support]} seconds")
        print()

    # Stop SparkContext
    sc.stop()
