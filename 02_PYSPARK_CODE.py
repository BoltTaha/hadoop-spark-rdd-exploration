#!/usr/bin/env python3
"""
Apache Spark RDD Exploration — PySpark pipeline
Demonstrates RDD transformations, partition analysis, and Spark UI profiling
on a Hadoop/YARN-integrated cluster.

Run:
  spark-submit --master local[2] 02_PYSPARK_CODE.py
  spark-submit --master yarn --deploy-mode client 02_PYSPARK_CODE.py

Spark UI: http://localhost:4040 (kept alive for 3 minutes after execution)
"""

# ============================================================================
# PART 1: Initialize Spark Context
# ============================================================================

from pyspark import SparkConf, SparkContext
import time

# Create SparkContext
conf = SparkConf().setAppName("RDD Exploration")
sc = SparkContext(conf=conf)

# View Spark Context
print("\n=== Spark Context ===")
print(sc)
# Note: URL will be shown here - copy for Question 1

# ============================================================================
# PART 2: Create Basic RDD
# ============================================================================

print("\n=== Creating RDD ===")
x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
xRDD = sc.parallelize(x)

print("Original array x:", x)
print("Type of x:", type(x))
print("Type of xRDD:", type(xRDD))

# ============================================================================
# PART 3: Understanding RDD Type (Question 2 & 3)
# ============================================================================

print("\n=== Question 2: RDD Class Type ===")
print("Class type:", type(xRDD))
print("Full class path:", xRDD.__class__.__module__ + "." + xRDD.__class__.__name__)
# Answer for Question 2: pyspark.rdd.RDD
# API Documentation: https://spark.apache.org/docs/latest/api/python/reference/pyspark.rdd.RDD.html

# ============================================================================
# PART 4: Understanding Partitions
# ============================================================================

print("\n=== Understanding Partitions ===")
num_partitions = xRDD.getNumPartitions()
print("Number of partitions:", num_partitions)
print("\nPartition contents (using glom()):")
print(xRDD.glom().collect())

# ============================================================================
# PART 5: Question 3 - What does glom() do?
# ============================================================================

print("\n=== Question 3: What glom() does ===")
print("glom() converts each partition into an array")
print("Returns: ", type(xRDD.glom()))
print("Content: ", xRDD.glom().collect())
# Answer: glom() converts the RDD into an array of arrays, where each sub-array
# represents a partition. It's useful for debugging partition contents.

# ============================================================================
# PART 6: Question 4 - Difference between print outputs
# ============================================================================

print("\n=== Question 4: Output Differences ===")

print("\n1. print(xRDD):")
print(xRDD)
# Output: pyspark.rdd.RDD object at 0x...
# Explanation: Lazy evaluation - doesn't compute, just shows object reference

print("\n2. print(xRDD.collect()):")
print(xRDD.collect())
# Output: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
# Explanation: collect() actually executes and brings all data to driver

print("\n3. print(xRDD.glom().collect()):")
print(xRDD.glom().collect())
# Output: [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]] or [[...], [...]] depending on partitions
# Explanation: Shows partition structure - array of arrays

# ============================================================================
# PART 7: Question 5 - Default Partitions for Array of 50
# ============================================================================

print("\n=== Question 5: Default Partitions ===")
x50 = list(range(1, 51))  # Array of size 50
xRDD50 = sc.parallelize(x50)
default_partitions = xRDD50.getNumPartitions()

print("Array size: 50")
print("Default number of partitions:", default_partitions)
print("Partition structure:")
print(xRDD50.glom().collect())

# Explanation for Question 5:
# The default number of partitions depends on:
# - For local mode: typically = number of CPU cores
# - For cluster mode: typically = total CPU cores across cluster or SPARK_DEFAULT_PARALLELISM

# ============================================================================
# PART 6: Question 6 - Repartition to 7 and Check Workload
# ============================================================================

print("\n=== Question 6: Repartition to 7 ===")
xRDD50_7part = xRDD50.repartition(7)
print("Number of partitions after repartition(7):", xRDD50_7part.getNumPartitions())
print("\nPartition composition:")
partitions_content = xRDD50_7part.glom().collect()
for i, partition in enumerate(partitions_content):
    print(f"Partition {i}: {len(partition)} elements -> {partition}")

# Explanation for Question 6:
# Repartition distributes 50 elements across 7 partitions
# Approx distribution: 50/7 ≈ 7 elements per partition (some may have 8)

# ============================================================================
# PART 7: Question 7 - Execution Order Using foreachPartition
# ============================================================================

print("\n=== Question 7: Execution Order with foreachPartition ===")

def print_partition_info(iterator):
    """Process each partition"""
    partition_data = list(iterator)
    print(f"Processing partition with elements: {partition_data}")
    for x in partition_data:
        yield x

print("\nExecuting foreachPartition:")
xRDD50_7part.foreachPartition(lambda iterator: 
    print(f"Partition: {list(iterator)}")
)

# Explanation for Question 7:
# Partitions are NOT executed in order (they may be parallelized)
# Execution order depends on available cores and scheduler

# ============================================================================
# PART 8: Filter for Even and Odd
# ============================================================================

print("\n=== Filtering for Even and Odd ===")

xRDDEven = xRDD.filter(lambda y: y % 2 == 0)
xRDDOdd = xRDD.filter(lambda y: y % 2 == 1)

print("\nEven numbers (1-12):", xRDDEven.collect())
print("Odd numbers (1-12):", xRDDOdd.collect())

# ============================================================================
# PART 9: Union Operation
# ============================================================================

print("\n=== Union Operation ===")

xRDDUnion = xRDDEven.union(xRDDOdd)
print("Union (Even + Odd):", xRDDUnion.collect())
print("Number of partitions after union:", xRDDUnion.getNumPartitions())

# ============================================================================
# PART 10: Job Information (Question 8)
# ============================================================================

print("\n=== Question 8: Job Information ===")
print("Open Spark UI at: http://localhost:4040")
print("Steps to find job info:")
print("1. Go to http://localhost:4040/jobs/")
print("2. Find the most recent job")
print("3. Record: Job ID, Description, Duration (in seconds)")
print("\nNote: Run the code above first, then check UI before it closes!")

# Record format:
# Job ID: [check UI]
# Job Description: [check UI]
# Duration: [check UI] seconds

# ============================================================================
# PART 11: Summary for DAG Analysis
# ============================================================================

print("\n=== Questions 9, 10, 11: DAG and Stages ===")
print("Steps:")
print("1. Keep pyspark shell open after running code")
print("2. Open Spark UI at http://localhost:4040")
print("3. Click on 'Jobs' tab")
print("4. Click on most recent job")
print("5. Scroll down to 'DAG Visualization'")
print("6. Screenshot the DAG")
print("\nDAG shows:")
print("- Stages: logical phases of computation")
print("- Stage 0: initial RDD operations")
print("- Stage 1+: shuffles/grouping operations")
print("\nTake screenshots for:")
print("- foreachPartition() job DAG")
print("- union() job DAG")

print("\n" + "="*70)
print("Setup complete! Now follow the instructions for Questions 8-11")
print("="*70)

ui_wait_seconds = 180
print(f"\nKeeping Spark UI alive for {ui_wait_seconds} seconds for screenshots...")
time.sleep(ui_wait_seconds)
sc.stop()
print("SparkContext stopped.")
