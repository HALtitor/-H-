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
    sub = []
    for row in dataReader:
        row[1]=float(row[1])
        sub.append(row)

#2つに分割したので並列プロセス数proc=2
proc = 3
n = len(sub)
part = n/proc
size = []
for i in range(proc):
    size.append(int(part*i))

sub3 = sub[size[2]:]
sub2 = sub[size[1]:size[2]]
sub = sub[size[0]:size[1]]

#entire=[sub,sub2] →thereadでは使わない
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
    return merge(left,right)

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




#以下全て追加コード

#threadを使う準備
import threading
from queue import Queue

#実行を円滑に行うためのメソッド→これを実行してソートを行う。
def aaa(arr,save):
    a=merge_sort(arr)
    save.put(a)

#ここからthreadプログラム
start = time.time()

#返り値を保存するための設定
save=Queue()
save2=Queue()
save3=Queue()

#threadを割り振る(プロセスの時と同様にデータを分割した数だけ用意する→今回は2分割)
thread1 = threading.Thread(target=aaa, args=(sub,save))
thread2 = threading.Thread(target=aaa, args=(sub2,save2))
thread3 = threading.Thread(target=aaa, args=(sub3,save3))

#実行
thread1.start()
thread2.start()
thread3.start()
#実行終了まで待機の意
thread1.join()
thread2.join()
thread3.join()

#返り値獲得
get=save.get()
get2=save2.get()
get3=save3.get()

#merge(実行終わり)
m = merge(get,get2)
pprint.pprint(merge(m,get3))

elapsed_time = time.time() - start

print ("elapsed_time:{0}".format(elapsed_time))
