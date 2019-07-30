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

set1 = set[size[1]:]
set = set[size[0]:size[1]]
entire=[set,set1]
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

#ここから並列処理のためのプログラム
start = time.time()

pool = mp.Pool(proc) #mpのPool関数に引数として与えられた数字がプロセス数が指定される

#2つをそれぞれmerge_sort関数でソート
callback = pool.map(merge_sort,entire)#map関数は各リストを指定された関数に引数として渡し実行する。

#merge関数でペアを作りソート　2→1 完了
finish = merge(callback[0],callback[1])
pprint.pprint(finish)

elapsed_time = time.time() - start
print ("elapsed_time:{0}".format(elapsed_time))