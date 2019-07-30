from multiprocessing import Pool
import multiprocessing as mp
import time
import pandas as pd
import csv
import pprint
'''
with open('ave_temp.csv') as f:
    #data = list(csv.reader(f))
    set=[list(map(str,line.rstrip().split(","))) for line in open('ave_temp.csv').readlines()]
'''

with open('data.csv',  newline='') as f:
    next(f)
    dataReader = csv.reader(f)
    set = []
    for row in dataReader:
        row[1]=float(row[1])
        set.append(row)

#2つに分割したので並列プロセス数proc=2
proc = 2
n = len(set)
part = n/proc
size = []
for i in range(proc):
    size.append(int(part*i))

#print(entire)

def merge_sort(arr):
    if len(arr) <= 1:
        return arr
    
    mid = len(arr) // 2
    left = arr[:mid]
    right = arr[mid:]
    #再帰的にmerge_sortを行う。
    left = merge_sort(left)
    right = merge_sort(right)
    
    return merge(left, right)


def merge(left, right):
    merged = []
    l_i, r_i = 0, 0
    
    while l_i < len(left) and r_i < len(right):
        
        if left[l_i][1] <= right[r_i][1]:
            merged.append(left[l_i])
            l_i += 1
        else:
            merged.append(right[r_i])
            r_i += 1

    if l_i < len(left):
        merged.extend(left[l_i:])
    if r_i < len(right):
        merged.extend(right[r_i:])
    return merged
    
#実行部分
start = time.time()#計測    

syu=merge_sort(set)
pprint.pprint(syu)

elapsed_time = time.time() - start
print ("elapsed_time:{0}".format(elapsed_time))