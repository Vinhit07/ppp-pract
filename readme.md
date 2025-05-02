# Task 1: Squares using multiprocessing.Process
```from multiprocessing import Process

def compute_square(n):
    print(f"Square of {n} is {n * n}")

if _name_ == "_main_":
    processes = []
    for i in range(1, 11):
        p = Process(target=compute_square, args=(i,))
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
```

# Task 2: Squares using multiprocessing.Pool
```from multiprocessing import Pool

def square(n):
    return n * n

if _name_ == "_main_":
    with Pool(processes=4) as pool:
        numbers = list(range(1, 11))
        results = pool.map(square, numbers)
        print("Squares:", results)
```

# Task 3: Factorials using multiprocessing
```import math

def factorial(n):
    return math.factorial(n)

if _name_ == "_main_":
    with Pool(processes=4) as pool:
        numbers = list(range(1, 11))
        results = pool.map(factorial, numbers)
        print("Factorials:", results)
```

# Task 4: Compare serial vs multiprocessing for squares
```import time

# Serial
start = time.time()
squares = [x * x for x in range(1, 1001)]
end = time.time()
print("Serial Time:", end - start)

# Parallel
start = time.time()
with Pool() as pool:
    pool.map(square, range(1, 1001))
end = time.time()
print("Multiprocessing Time:", end - start)
```

# Task 5: Compute squares of 1000 numbers
# Already done above; analyze speed via output timing.

# Task 6: Download images using threading
```import threading
import requests

urls = [
    'https://via.placeholder.com/150',
    'https://via.placeholder.com/200'
]

def download_image(url):
    img = requests.get(url).content
    filename = url.split('/')[-1] + ".png"
    with open(filename, 'wb') as f:
        f.write(img)

threads = []
for url in urls:
    t = threading.Thread(target=download_image, args=(url,))
    t.start()
    threads.append(t)

for t in threads:
    t.join()
```

# Task 7: Use ThreadPoolExecutor for image downloading
```from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor() as executor:
    executor.map(download_image, urls)
```

# Task 8: Web scraper using multithreading
```from bs4 import BeautifulSoup

web_urls = [
    'https://example.com',
    'https://example.org'
]

def fetch_title(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    print(soup.title.string)

threads = []
for url in web_urls:
    t = threading.Thread(target=fetch_title, args=(url,))
    t.start()
    threads.append(t)

for t in threads:
    t.join()
```

# Task 9: Time comparison for file downloads
```import urllib.request

file_urls = urls * 5

def download_file(url):
    urllib.request.urlretrieve(url, url.split('/')[-1] + '.jpg')

# Sequential
start = time.time()
for url in file_urls:
    download_file(url)
end = time.time()
print("Sequential Download Time:", end - start)

# Parallel
start = time.time()
with ThreadPoolExecutor() as executor:
    executor.map(download_file, file_urls)
end = time.time()
print("Multithreaded Download Time:", end - start)
```

# Task 10: Multithreaded file read/write
```files = ['file1.txt', 'file2.txt']

def write_file(fname):
    with open(fname, 'w') as f:
        f.write("Hello world\n" * 1000)

def read_file(fname):
    with open(fname, 'r') as f:
        print(fname, "read", len(f.readlines()), "lines")

threads = []
for fname in files:
    t1 = threading.Thread(target=write_file, args=(fname,))
    t2 = threading.Thread(target=read_file, args=(fname,))
    t1.start()
    t2.start()
    threads.extend([t1, t2])

for t in threads:
    t.join()
```

# Task 11: CPU-intensive work with multithreading (note: GIL limits performance)
```def cpu_task(n):
    count = 0
    for _ in range(10**6):
        count += n * n

threads = [threading.Thread(target=cpu_task, args=(i,)) for i in range(4)]
start = time.time()
for t in threads: t.start()
for t in threads: t.join()
print("CPU-bound task with threads completed in", time.time() - start)

# Task 12: Matrix multiplication (serial)
A = [[i for i in range(100)] for _ in range(100)]
B = [[j for j in range(100)] for _ in range(100)]
C = [[0]*100 for _ in range(100)]

for i in range(100):
    for j in range(100):
        for k in range(100):
            C[i][j] += A[i][k] * B[k][j]
```

# Task 13: Matrix mult using multiprocessing
```from multiprocessing import Pool

def matmul_row(i):
    return [sum(A[i][k] * B[k][j] for k in range(100)) for j in range(100)]

if _name_ == "_main_":
    with Pool() as pool:
        C = pool.map(matmul_row, range(100))
```

# Task 14: Matrix multiplication using NumPy
```import numpy as np

A_np = np.array(A)
B_np = np.array(B)
start = time.time()
C_np = np.dot(A_np, B_np)
print("NumPy dot time:", time.time() - start)
```

# Task 15: Test with 500x500 and 1000x1000 to compare
# Change matrix sizes and rerun similar to above

# Task 16: Parallel matmul using shared_memory
from multiprocessing import shared_memory
# Requires advanced setup, left as implementation idea due to complexity

# Task 17: Word count using Dask
```import dask.bag as db

# Save a large text file (or simulate)
with open("sample.txt", 'w') as f:
    f.write("word1 word2 word3\n" * 100000)

bag = db.read_text("sample.txt").flat_map(str.split)
word_counts = bag.frequencies().compute()
print(word_counts)
```
# 18. Distributed Matrix Operation using Dask
```
import dask.array as da
import numpy as np
import time

x = da.random.random((10000, 10000), chunks=(1000, 1000))
y = da.random.random((10000, 10000), chunks=(1000, 1000))

start = time.time()
result = (x @ y).compute()
print("Dask time:", time.time() - start)

x_np = np.random.random((1000, 1000))
y_np = np.random.random((1000, 1000))

start = time.time()
result_np = x_np @ y_np
print("NumPy time:", time.time() - start)
```

# 19. PySpark Word Count
```
from pyspark import SparkContext
sc = SparkContext("local", "WordCount")
text_files = sc.textFile("/path/to/files/*.txt")
counts = text_files.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("output")
```

# 20. Download images with threading
```
import threading, requests
urls = ["https://example.com/image1.jpg", "https://example.com/image2.jpg"]
def download_image(url):
    response = requests.get(url)
    with open(url.split("/")[-1], "wb") as f:
        f.write(response.content)
threads = [threading.Thread(target=download_image, args=(url,)) for url in urls]
[t.start() for t in threads]
[t.join() for t in threads]
```

# 21. Fetch multiple APIs concurrently
```
from concurrent.futures import ThreadPoolExecutor
urls = ["https://api.github.com", "https://httpbin.org/get"]
def fetch(url):
    return requests.get(url).json()
with ThreadPoolExecutor() as executor:
    results = list(executor.map(fetch, urls))
print(results)
```

# 22. Web scraper with multithreading
```
from bs4 import BeautifulSoup
webpages = ["https://example.com/page1", "https://example.com/page2"]
def scrape(url):
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    print(soup.title.string)
threads = [threading.Thread(target=scrape, args=(url,)) for url in webpages]
[t.start() for t in threads]
[t.join() for t in threads]
```

# 23. Process large logs with multithreading
```
import os
log_files = ["log1.txt", "log2.txt"]
def process_log(file):
    with open(file) as f:
        for line in f:
            if "ERROR" in line:
                print(line.strip())
threads = [threading.Thread(target=process_log, args=(log,)) for log in log_files]
[t.start() for t in threads]
[t.join() for t in threads]
```

# 24. Prime numbers with multithreading
```
import math
nums = list(range(2, 100000))
def is_prime(n):
    if n < 2: return False
    for i in range(2, int(math.sqrt(n)) + 1):
        if n % i == 0:
            return False
    return True

def compute_primes():
    for n in nums:
        is_prime(n)
threads = [threading.Thread(target=compute_primes) for _ in range(4)]
[t.start() for t in threads]
[t.join() for t in threads]
# Note: CPU-bound task; GIL restricts threading scalability
```

# 25. Banking system simulation
```
import threading
class Account:
    def _init_(self, balance):
        self.balance = balance
        self.lock = threading.Lock()

    def deposit(self, amount):
        with self.lock:
            self.balance += amount

    def withdraw(self, amount):
        with self.lock:
            if self.balance >= amount:
                self.balance -= amount

account = Account(1000)
def task():
    for _ in range(100):
        account.deposit(5)
        account.withdraw(5)
threads = [threading.Thread(target=task) for _ in range(10)]
[t.start() for t in threads]
[t.join() for t in threads]
print("Final balance:", account.balance)
```

# 26. I/O-bound file reading
```
import os
files = ["file1.txt", "file2.txt"]
def read_file(file):
    with open(file, 'r') as f:
        print(f.read())
threads = [threading.Thread(target=read_file, args=(file,)) for file in files]
[t.start() for t in threads]
[t.join() for t in threads]
```

# 27. Matrix multiplication manual vs NumPy
```
import numpy as np
size = 100
A = np.random.rand(size, size)
B = np.random.rand(size, size)

# NumPy
start = time.time()
C = A @ B
print("NumPy time:", time.time() - start)

# Manual
C_manual = np.zeros((size, size))
start = time.time()
for i in range(size):
    for j in range(size):
        for k in range(size):
            C_manual[i][j] += A[i][k] * B[k][j]
print("Manual time:", time.time() - start)
```

# 28. Compare serial vs parallel
```
import multiprocessing as mp
def matmul_worker(A, B, start_row, end_row):
    C = np.zeros((end_row - start_row, B.shape[1]))
    for i in range(start_row, end_row):
        for j in range(B.shape[1]):
            C[i - start_row][j] = sum(A[i, k] * B[k, j] for k in range(B.shape[0]))
    return C

def parallel_matmul(A, B):
    num_procs = mp.cpu_count()
    rows = A.shape[0]
    step = rows // num_procs
    pool = mp.Pool()
    results = []
    for i in range(num_procs):
        start_row = i * step
        end_row = (i + 1) * step if i != num_procs - 1 else rows
        results.append(pool.apply_async(matmul_worker, (A, B, start_row, end_row)))
    C_parts = [res.get() for res in results]
    return np.vstack(C_parts)

for n in [10, 100, 1000]:
    A = np.random.rand(n, n)
    B = np.random.rand(n, n)
    start = time.time()
    A @ B
    print(f"{n}x{n} NumPy time:", time.time() - start)

    start = time.time()
    parallel_matmul(A, B)
    print(f"{n}x{n} Parallel time:", time.time() - start)
```

# 29. GPU Parallelization using Numba
```
from numba import cuda
@cuda.jit
def add_kernel(a, b, out):
    i = cuda.grid(1)
    if i < a.size:
        out[i] = a[i] + b[i]

a = np.arange(1000000, dtype=np.float32)
b = np.arange(1000000, dtype=np.float32)
out = np.zeros_like(a)

start = time.time()
d_a = cuda.to_device(a)
d_b = cuda.to_device(b)
d_out = cuda.device_array_like(a)
threadsperblock = 256
blockspergrid = (a.size + (threadsperblock - 1)) // threadsperblock
add_kernel[blockspergrid, threadsperblock](d_a, d_b, d_out)
d_out.copy_to_host(out)
print("GPU time:", time.time() - start)
```

# 30. Parallel Merge Sort using multiprocessing
```
def merge_sort(arr):
    if len(arr) <= 1:
        return arr
    mid = len(arr) // 2
    left = merge_sort(arr[:mid])
    right = merge_sort(arr[mid:])
    return merge(left, right)

def merge(left, right):
    result = []
    i = j = 0
    while i < len(left) and j < len(right):
        if left[i] < right[j]:
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1
    result.extend(left[i:])
    result.extend(right[j:])
    return result

def parallel_merge_sort(arr):
    if len(arr) <= 100000:
        return merge_sort(arr)
    else:
        mid = len(arr) // 2
        pool = mp.Pool(2)
        left, right = pool.map(parallel_merge_sort, [arr[:mid], arr[mid:]])
        return merge(left, right)

arr = np.random.randint(0, 1000000, 500000)
start = time.time()
sorted_arr = parallel_merge_sort(arr.tolist())
print("Parallel merge sort time:", time.time() - start)
```