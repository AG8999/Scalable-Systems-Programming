# Comparison Study Between A-priori and SON Algorithm on Lottery Winner Numbers Using MapReduce Algorithm

## Table of Contents
- [Project Overview](#project-overview)
- [Objectives](#objectives)
- [Methodology](#methodology)
  - [Data Collection](#data-collection)
  - [Data Cleaning](#data-cleaning)
  - [MapReduce Architecture](#mapreduce-architecture)
  - [Algorithms](#algorithms)
    - [A-priori Algorithm](#a-priori-algorithm)
    - [SON Algorithm](#son-algorithm)
- [Results](#results)
  - [Association Rules](#association-rules)
- [Conclusion and Future Work](#conclusion-and-future-work)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Usage](#usage)
- [Adding Results Images](#adding-results-images)

## Project Overview
This project aims to compare two frequent data mining algorithms, A-priori and SON, using a dataset of lottery winning draws. By implementing parallel processing with the MapReduce algorithm on the PySpark environment, the project focuses on identifying interesting association rules between lottery winning numbers. The performance of both algorithms is evaluated based on computation time and memory usage.

## Objectives
- Perform a comparative analysis of A-priori and SON algorithms.
- Implement parallel processing using MapReduce in a PySpark environment.
- Evaluate the algorithms based on computation time and memory usage.
- Identify interesting association rules from the lottery winning numbers.

## Methodology
### Data Collection
The dataset of lottery winning draws is collected from a publicly available repository [data.gov](https://data.gov). The data includes:
- Draw date
- Draw number
- Draw time
- Winning number
- Extra numbers

### Data Cleaning
The data cleaning process involves handling missing values, duplicate values, and transforming data types to ensure data quality.

### MapReduce Architecture
MapReduce is used to handle the large-scale data by splitting it into smaller chunks and processing it in parallel. The process involves:
- **Mapper Phase**: Tokenizes each distinct winning number into key-value pairs.
- **Reducer Phase**: Calculates the total frequency of each winning number by grouping them.

### Algorithms
#### A-priori Algorithm
A-priori is used to find frequent item-sets and generate association rules. It uses a support threshold to filter frequent item-sets. However, it has limitations in terms of computation time and space due to multiple passes over the dataset.

#### SON Algorithm
SON algorithm overcomes the limitations of A-priori by breaking the data into smaller chunks and processing them locally. It reduces computational time and increases efficiency by minimizing the need to read the entire dataset repeatedly.

## Results
The comparison between A-priori and SON algorithms is based on computation time and memory usage:
- A-priori is faster for small data samples.
- SON is more efficient in terms of memory usage.

### Association Rules
Top 5 association rules identified:
| Antecedent   | Consequent | Confidence | Lift   |
|--------------|------------|------------|--------|
| ('38', '54') | ('57')     | 0.2675     | 1.8298 |
| ('07', '29') | ('71')     | 0.2659     | 1.8011 |
| ('49', '58') | ('69')     | 0.2653     | 1.8084 |
| ('18', '23') | ('51')     | 0.2642     | 1.8016 |
| ('24', '29') | ('71')     | 0.2625     | 1.7777 |

## Conclusion and Future Work
The study concludes that A-priori is suitable for small data samples due to its faster computation time, while SON is more efficient in terms of memory usage. Future work includes executing the algorithms on larger datasets with more computing power to achieve more accurate results.

## Getting Started
### Prerequisites
- Hadoop
- PySpark
- Python

### Usage
1. Run A-priori algorithm:
python <script file> <input data path>

```sh
#python <script file> <input data path>
python3 APRIORI_CODE.py ../dataset/200Klottey_winning_draws.txt
```

2. Run SON algorithm:
   
```sh
#python <script file> <input data path> <max size of itemset>
python SON_ALGO.py ../dataset/200Klottey_winning_draws.txt 3
```
OR 
```sh
#python <script file> <input data path> <max size of itemset>
python3 SON_ALGO.py ../dataset/200Klottey_winning_draws.txt 3
```
3. Run Association Rules:
```sh
python3 AssociationRules.py ../dataset/200Klottey_winning_draws.txt
```
Thank you!
 
