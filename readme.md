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
        p.join()```

# Task 2: Squares using multiprocessing.Pool
```from multiprocessing import Pool

def square(n):
    return n * n

if _name_ == "_main_":
    with Pool(processes=4) as pool:
        numbers = list(range(1, 11))
        results = pool.map(square, numbers)
        print("Squares:", results)```

# Task 3: Factorials using multiprocessing
```import math

def factorial(n):
    return math.factorial(n)

if _name_ == "_main_":
    with Pool(processes=4) as pool:
        numbers = list(range(1, 11))
        results = pool.map(factorial, numbers)
        print("Factorials:", results)```

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
print("Multiprocessing Time:", end - start)```

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
    t.join()```

# Task 7: Use ThreadPoolExecutor for image downloading
```from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor() as executor:
    executor.map(download_image, urls)```

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
    t.join()```

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
print("Multithreaded Download Time:", end - start)```

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
    t.join()```

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
            C[i][j] += A[i][k] * B[k][j]```

# Task 13: Matrix mult using multiprocessing
```from multiprocessing import Pool

def matmul_row(i):
    return [sum(A[i][k] * B[k][j] for k in range(100)) for j in range(100)]

if _name_ == "_main_":
    with Pool() as pool:
        C = pool.map(matmul_row, range(100))```

# Task 14: Matrix multiplication using NumPy
```import numpy as np

A_np = np.array(A)
B_np = np.array(B)
start = time.time()
C_np = np.dot(A_np, B_np)
print("NumPy dot time:", time.time() - start)```

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
print(word_counts)```