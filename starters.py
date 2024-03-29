import psycopg2
import psycopg2.extensions
import psycopg2.extras
import numpy as np
from psycopg2 import OperationalError
from psycopg2 import sql
from psycopg2 import pool
#from psycopg2 import wait_select
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
import concurrent.futures
import queries as qu
import connections as co
import bd
import select
import csv
import threading
from collections import deque
import time

def exe_query_Ntimes_pool(query, N):
    #START CONNECTION POOL
    pool = co.create_connection_pool(1,5,"postgis_test","postgres","postgres","localhost","5432")
    times = []
    #for i in range(N):
    #   t = get_image(connection,'altifr_75m_0150_6825',154938.251,6821208.497, 500, 500)
    #   times.append(t)
    for i in range(N):
        results = qu.query_with_pool(pool,query)
        times.append(results[1])
    pool.closeall
    bd.plot_perf(times,'chart_pool')

def my_wait(conn):
    while True:
        state = conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            #print("my_wait : poll ok")
            break
        elif state == psycopg2.extensions.POLL_WRITE:
            select.select([], [conn.fileno()], [])
        elif state == psycopg2.extensions.POLL_READ:
            select.select([conn.fileno()], [], [])
        else:
            raise psycopg2.OperationalError("poll() returned %s" % state)

def exe_query_async_Ntimes(query, N):
    aconn = psycopg2.connect(database="postgis_test", user="postgres", host="127.0.0.1", port="5432", password="postgres", _async=1)
    my_wait(aconn)

    acurs = aconn.cursor()      

    times_exe = []
    times_fetch = []
    times_wait = []
    times_total = []

    for i in range(N):
        print("Running query {} in async mode".format(i))
        
        start = time.perf_counter()
        acurs.execute(query)
        end = time.perf_counter()
        my_wait(acurs.connection)
        end_wait = time.perf_counter()
        runtime_exe = end - start

        runtime_wait = end_wait - end

        result = acurs.fetchall()
        end_fetch = time.perf_counter()
        runtime_fetchall = end_fetch - end_wait

        total = end_fetch-start

        times_total.append(total)
        times_exe.append(runtime_exe)
        times_fetch.append(runtime_fetchall)
        times_wait.append(runtime_wait)

    acurs.close()

    bd.plot_fig(times_exe,times_wait,times_fetch,times_total, 'async_perf')
    #plot_perf(times_exe,'async_execution')
    #plot_perf(times_fetch,'async_fetch')
    #plot_perf(times_wait, 'async_wait')
    
def query_async_pool_Ntimes(query,N,nbpool):
   
    pool = co.create_connection_pool(1,nbpool,"postgis_test","postgres","postgres","localhost","5432",1)
    #time.sleep(3)
    times_exe = []
    times_fetch = []
    times_wait = []
    times_total = []
    

    cursors = []
    connections = []
    counter = 0
    dispo = nbpool

    while counter < N :

        print("Starting connections and queries")
        while dispo != 0:
            aconn  = pool.getconn()
            dispo = dispo -1
            counter +=1
            connections.append(aconn)

            if (aconn):
                print("get conn ok")
                #time.sleep(3)
                my_wait(aconn)
                #psycopg2.extras.wait_select(aconn)
                #print("wait aconn ok")
                acurs = aconn.cursor()

                start = time.perf_counter()
                acurs.execute(query)
                end = time.perf_counter()
                runtime_exe = end - start
                times_exe.append(runtime_exe)

                cursors.append(acurs)
                #pool.putconn(aconn)

        print("Gettting results")
        for cur in cursors:
            swait = time.perf_counter()
            my_wait(cur.connection)
            ewait = time.perf_counter()
            runtime_wait = ewait - swait
            times_wait.append(runtime_wait)

            result = cur.fetchall()
            qu.test_raster_results(result)
            end_fetch = time.perf_counter()
            runtime_fetchall = end_fetch - ewait
            times_fetch.append(runtime_fetchall)

            total = end_fetch - start
            times_total.append(total)

            print("query done")
            #acurs.close()
            pool.putconn(connections[cursors.index(cur)])
            dispo +=1

        cursors = []
        connections = []
                        
    #results = execute_read_query(ps_connection, query)
    #bd.plot_perf(times_exe,'execution')
    #bd.plot_perf(times_wait,'wait')
    #bd.plot_perf(times_fetch,'fetch')
    bd.plot_fig(times_exe,times_wait,times_fetch,times_total, 'test_async_pool_perf')
    #Use this method to release the connection object and send back ti connection pool

def task_getconn(conn_pool):
    aconn  = conn_pool.getconn()
    my_wait(aconn)
    #print("get conn ok")
    return aconn

def task_test():
    return 10

def task_execute(aconn,query):
    #psycopg2.extras.wait_select(aconn)
    #print("wait aconn ok")
    q1 = "SELECT ST_AsGDALRaster(ST_Union(altifr_75m_0150_6825.rast), 'GTiff') FROM altifr_75m_0150_6825"
    acurs = aconn.cursor()

    start = time.perf_counter()
    acurs.execute(query)
    end = time.perf_counter()
    runtime_exe = end - start
       
    #print("execute query ok")
    return acurs, runtime_exe

def task_wait_fetch(curs):
    swait = time.perf_counter()
    my_wait(curs.connection)
    ewait = time.perf_counter()

    runtime_wait = ewait - swait

    result = curs.fetchall()
    end_fetch = time.perf_counter()
    #if type(result[0][0]) == 'memoryview':
     #   qu.test_raster_results(result)
    #else :
     #   print(result)
    runtime_fetchall = end_fetch - ewait
   
    #print("query done")
    return runtime_wait, runtime_fetchall

class QueryExecution:
        def __init__(self, query, pool):
            self.query = query
            self.conn_pool = pool
            
            self.create_time = 0
            self.query_time_start = 0
            self.query_time_submit = 0
            self.query_time_end = 0
            self.wait_time_start = 0
            self.wait_time_end = 0
            self.fetch_time_start = 0
            self.fetch_time_end = 0

        def submitQuery(self):
            self.query_time_start = time.perf_counter()
            self.connection = task_getconn(self.conn_pool)
            self.query_cursor = self.connection.cursor()
            self.query_time_submit = time.perf_counter()
            self.query_cursor.execute(self.query)
            self.query_time_end = time.perf_counter()

        def waitForQueryResult(self):
            self.wait_time_start = time.perf_counter()
            my_wait(self.query_cursor.connection)
            self.wait_time_end = time.perf_counter()

        def fetchQueryResult(self):
            self.fetch_time_start = time.perf_counter()
            self.result = self.query_cursor.fetchall()
            self.fetch_time_end = time.perf_counter()
            self.query_cursor.close()
            self.conn_pool.putconn(self.connection)

        def startSeqQuery(self):
            self.submitQuery()
            self.waitForQueryResult()
            self.fetchQueryResult()



class TasksList:
        def __init__(self):
            self.tasks = []
            self.execQueries = []
            self.tasksMutex = threading.Lock()

        def initQueries(self, queries, pool):
            for i in range(len(queries)):
                self.execQueries.append(QueryExecution(queries.pop(), pool))

            self.tasks = deque()
            def fetchLambda(tasksList, execQuery):
                execQuery.fetchQueryResult()
            def waitLambda(tasksList, execQuery):
                execQuery.waitForQueryResult()
                tasksList.addTask(lambda: fetchLambda(tasksList, execQuery))
            def submitLambda(tasksList, execQuery):
                execQuery.submitQuery()
                tasksList.addTask(lambda: waitLambda(tasksList, execQuery))
            for execQuery in self.execQueries:
                self.tasks.append(lambda execQuery=execQuery: submitLambda(self, execQuery))

        def addTask(self, task):
            self.tasksMutex.acquire()
            self.tasks.append(task)
            self.tasksMutex.release()

        def executeNext(self):
            self.tasksMutex.acquire()
            try:
                nextTask = self.tasks.popleft()
            except:
                return False
            finally:
                self.tasksMutex.release()
            nextTask()
            return True

        def execute_next_task(self):
            hasTask = self.executeNext()
            return hasTask


def start_multith_tasks(nbthreads,nbpool,queries):

    pool = co.create_connection_pool(nbpool,nbpool,"postgis_test","postgres","postgres","localhost","5432",1)
    N = len(queries)
    time.sleep(1)
    start = time.perf_counter()
#    class ThreadSafePool:
#        def __init__(self, pool):
#            self.pool = pool
#            self.mutex = threading.Lock()
#        def getconn(self):
#            self.mutex.acquire()
#            connection = self.pool.getconn()
#            self.mutex.release()
#            return connection    

    allTasks = TasksList()
    allTasks.initQueries(queries, pool)
#    for task, param in allTasks.tasks:
#        task(param)

    def workOnTask(tasksList):
        while tasksList.executeNext():
            pass

    futures = []

    with ThreadPoolExecutor(max_workers= nbthreads) as executor:
        for i in range(nbthreads):
            futures.append(executor.submit(workOnTask, allTasks))

    wait(futures, return_when='ALL_COMPLETED')

    end = time.perf_counter()
    total_prog = end - start 

    starts = []
    ends = []
    fends = []
    wends = []

    for future in futures:
        hasThrown = future.exception()
        if hasThrown:
            raise hasThrown
        print(future.result)

    f = open("async_overviews.txt", "w")
    for execQuery in allTasks.execQueries:
        print (execQuery.result)
        f.write("{}, {}, {}, {}, {}, {}, {}, {}\n".format(execQuery.query,execQuery.query_time_start,execQuery.query_time_submit,execQuery.query_time_end , execQuery.wait_time_start, execQuery.wait_time_end ,execQuery.fetch_time_start, execQuery.fetch_time_end))
        starts.append(execQuery.query_time_start)
        ends.append(execQuery.query_time_submit)
        wends.append(execQuery.wait_time_end)
        fends.append(execQuery.fetch_time_end)
    f.close()
    bd.plot_start_end_phases(starts,ends,wends,fends,'async_overviews_phases')

    print("total time for N = {} executions : {} s".format(N,total_prog))
#    print("mean execution time : {} s".format(np.mean(times_total)))

#    bd.plot_fig(times_exe,times_wait,times_fetch,times_total, 'multithreading_perf')

    
def exe_wait_fetch(pool,query,times_exe,times_wait,times_fetch,times_total):
    print("exe wait fetch")
    print("query :")
    print(query)

    start_time = time.perf_counter()
     
    aconn = task_getconn(pool)
    #time.sleep(1)
    res_exe = task_execute(aconn,query)

    acurs = res_exe[0]
    times_exe.append(res_exe[1])

    times = task_wait_fetch(acurs)
    times_wait.append(times[0])
    times_fetch.append(times[1])

    pool.putconn(aconn)
    
    end_time = time.perf_counter()

    total = end_time - start_time 
    times_total.append(total)   

def exe_wait_fetch_dict(pool,query,times_exe,times_wait,times_fetch,times_total,qdict,perf,starts,ends):
    print("exe wait fetch")
    print("query :")
    print(query)

    start_time = time.perf_counter()
    starts.append(start_time)
    aconn = task_getconn(pool)
    #time.sleep(1)
    res_exe = task_execute(aconn, qdict[query])

    acurs = res_exe[0]
    times_exe.append(res_exe[1])

    times = task_wait_fetch(acurs)
    times_wait.append(times[0])
    times_fetch.append(times[1])

    pool.putconn(aconn)
    
    end_time = time.perf_counter()
    ends.append(end_time)
    total = end_time - start_time 
    times_total.append(total)   

    perf.append("query : {} execution : {}s , wait : {}s, fetch : {}s, total : {}s \n".format(query,res_exe[1],times[0],times[1],total))

def start_multith(N,nbthreads,nbpool,query):
#https://www.tutorialspoint.com/concurrency_in_python/concurrency_in_python_pool_of_threads.htm
    pool = co.create_connection_pool(1,nbpool,"postgis_test","postgres","postgres","localhost","5432",1)

    count = 0
    results = []

    connections = []
    cursors = []
    dispo = nbpool 

    times_exe = []
    times_wait = []
    times_fetch = []
    times_total = []

    start = time.perf_counter()

    #while count < N:
    futures = []

    with ThreadPoolExecutor(max_workers= nbthreads) as executor:
        for i in range(N):
            futures.append(executor.submit(exe_wait_fetch, pool, query,times_exe,times_wait,times_fetch,times_total))
            #count +=1

    end = time.perf_counter()
    total_prog = end - start 
    print("total time for N = {} executions : {} s".format(N,total_prog))
    print("mean total time : {} s".format(np.mean(times_total)))

    bd.plot_fig(times_exe,times_wait,times_fetch,times_total, 'callback_threads_test')

def start_multith_file(nbthreads,nbpool,file,chartname):
#https://www.tutorialspoint.com/concurrency_in_python/concurrency_in_python_pool_of_threads.htm
    pool = co.create_connection_pool(1,nbpool,"postgis_test","postgres","postgres","localhost","5432",1)


    times_exe = []
    times_wait = []
    times_fetch = []
    times_total = []
    starts = []
    ends = []

    dict_queries = get_queries(file)
    queries = list(dict_queries.keys())
    print(queries)

    #while count < N:
    futures = []
    perf = []

    with ThreadPoolExecutor(max_workers= nbthreads) as executor:
        for i in range(len(queries)):
            futures.append(executor.submit(exe_wait_fetch_dict, pool, queries.pop(),times_exe,times_wait,times_fetch,times_total, dict_queries,perf,starts,ends))
            #count +=1

    wait(futures, return_when='ALL_COMPLETED')

    
    f = open("results_multithreading.txt", "w")
    for l in perf :
        #print(l)
        f.write(l)

    f.close()

    bd.plot_start_end(starts,ends,chartname)
   # bd.plot_start_end(starts,ends,'asynchronous_execution')

    #bd.plot_fig(times_exe,times_wait,times_fetch,times_total, 'callback_threads_test')

def get_queries(file):
    with open(file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='#')
        qdict = {}
        #liste = []
        #line_count = 0
        for line in csv_reader:
            qdict[line[0]] = line[1]

    return qdict

def start_sync_file_queries(mode,file,chartname):
    queries_dict = get_queries(file)

    keys = list(queries_dict.keys())
    print(keys)
    keys_list = []
    #mode "normal" pas de changement on fait les requetes a la suite comme elles sont dans le dict
    if mode == 0:
        keys_list = keys
    #mode double : on fait chaque requete 2 fois a la suite
    elif mode == 1:
        for e in keys:
            keys_list.append(e)
            keys_list.append(e)
            
    #mode aleatoire : on melange les requetes aleatoirement
    elif mode == 2:
        keys_list = random.sample(keys,len(keys))

    elif mode == 3:
        keys.reverse()
        keys_list = keys

    starts = []
    ends = []

    #START CONNECTION
    connection = co.create_connection("postgis_test","postgres","postgres","localhost","5432")
        
    f = open("results_seq_{}.txt".format(mode), "w")
    for key in keys_list:
        print(key)
        starts.append(time.perf_counter())
        results = qu.execute_read_query(connection,queries_dict[key])
        ends.append(time.perf_counter())
        values = results[0]
        runtime_exe = results[1]
        runtime_fetchall = results[2]
        ratio = (runtime_fetchall / runtime_exe + runtime_fetchall)*100 
        f.write("{} executed in {} seconds and fetched in {} seconds \n ratio : {} % \n".format(key,runtime_exe, runtime_fetchall,ratio))
    f.close()

    bd.plot_start_end(starts,ends,chartname)
    #bd.plot_start_end(starts,ends,'synchronous_execution')
    
def query_table_overviews(max_o, table, nbthreads,nbpool):
    
    names_list = qu.table_overviews_list(max_o,table)
    qlist = []
    for name in names_list:
        #val = names[name]
        query = "SELECT ST_AsGDALRaster(ST_Union({}.rast), 'GTiff') FROM {}".format(name,name)
        #print(name)
        qlist.append(query)


    if nbthreads == 0:  
        pool = co.create_connection_pool(1,nbpool,"postgis_test","postgres","postgres","localhost","5432",0)
        #connection = co.create_connection("postgis_test","postgres","postgres","localhost","5432")
        starts = []
        ends = []
        wends = []
        fends = []

        f = open("sync_overviews.txt", "w")
        qlist.reverse()
        for q in qlist:
            print("sync query")
            todo = QueryExecution(q,pool)
            todo.startSeqQuery()
            f.write("{}, {}, {}, {}, {}, {}, {}, {}\n".format(todo.query,todo.query_time_start,todo.query_time_submit,todo.query_time_end , todo.wait_time_start, todo.wait_time_end ,todo.fetch_time_start, todo.fetch_time_end))
            starts.append(todo.query_time_start)
            ends.append(todo.query_time_submit)
            wends.append(todo.wait_time_end)
            fends.append(todo.fetch_time_end)

        
        f.close()
        bd.plot_start_end_phases(starts,ends,wends,fends,'sync_overviews_phases')
     
    else:
        #print(names_list)
        #qlist.reverse()
        start_multith_tasks(nbthreads,nbpool,qlist)
