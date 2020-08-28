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
import time
import threading
from collections import deque

def exe_query_Ntimes_pool(query, N):
    #START CONNECTION POOL
    pool = co.create_connection_pool(1,5,"postgis_test","postgres","admin","localhost","5432")
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
#            print("my_wait : poll ok")
            break
        elif state == psycopg2.extensions.POLL_WRITE:
            select.select([], [conn.fileno()], [])
        elif state == psycopg2.extensions.POLL_READ:
            select.select([conn.fileno()], [], [])
        else:
            raise psycopg2.OperationalError("poll() returned %s" % state)

def exe_query_async_Ntimes(query, N):
    aconn = psycopg2.connect(database="postgis_test", user="postgres", host="127.0.0.1", port="5432", password="admin", async_=1)
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
   
    pool = co.create_connection_pool(1,nbpool,"postgis_test","postgres","admin","localhost","5432",1)
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
#    print("get conn ok")
    return aconn

def task_test():
    return 10

def task_execute(aconn,query):
    #psycopg2.extras.wait_select(aconn)
    #print("wait aconn ok")
    acurs = aconn.cursor()

    start = time.perf_counter()
    acurs.execute(query)
    end = time.perf_counter()
    runtime_exe = end - start
       
#    print("execute query ok")
    return acurs, runtime_exe

def task_wait_fetch(curs):
    swait = time.perf_counter()
    my_wait(curs.connection)
    ewait = time.perf_counter()

    runtime_wait = ewait - swait

    result = curs.fetchall()
    
    end_fetch = time.perf_counter()
    qu.test_raster_results(result)
    runtime_fetchall = end_fetch - ewait
   
   
    qu.test_raster_results(result)
    print("query done")

    return runtime_wait, runtime_fetchall
    

def start_multithreading(N,nbthreads,nbpool,query):
#https://www.tutorialspoint.com/concurrency_in_python/concurrency_in_python_pool_of_threads.htm
    pool = co.create_connection_pool(1,nbpool,"postgis_test","postgres","admin","localhost","5432",1)

    count = 0
    results = []

    connections = []
    cursors = []
    dispo = nbpool 

    while count < N:
 
        with ThreadPoolExecutor(max_workers = nbpool) as executor:
            futures = []
            while len(connections) < N and dispo != 0:
                futures.append(executor.submit(task_getconn,pool))
                dispo = dispo -1

            for f in as_completed(futures):
                connections.append(f.result())


        with ThreadPoolExecutor(max_workers = 3) as executor:
            #executor.submit(task_execute, (connections,query)).result
            #cursors.append(executor.map(task_execute, (connections,query)).result)
            cursors_list = executor.map(task_execute, connections)

            for cur in cursors_list:
                cursors.append(cur)

            print(len(cursors))
         

        with ThreadPoolExecutor(max_workers = 3) as executor:
            results = executor.map(task_wait_fetch, cursors)
            print("fetch")
            for curs in cursors:
                pool.putconn(connections[cursors.index(curs)])
            count += 1

def start_multith_tasks(N,nbthreads,nbpool,query):
#https://www.tutorialspoint.com/concurrency_in_python/concurrency_in_python_pool_of_threads.htm
    pool = co.create_connection_pool(1,nbpool,"postgis_test","postgres","admin","localhost","5432",1)

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
#    class ThreadSafePool:
#        def __init__(self, pool):
#            self.pool = pool
#            self.mutex = threading.Lock()
#        def getconn(self):
#            self.mutex.acquire()
#            connection = self.pool.getconn()
#            self.mutex.release()
#            return connection
    class QueryExecution:
        def __init__(self, query, pool):
            self.query = query
            self.pool = pool

            self.create_time = 0
            self.query_time_start = 0
            self.query_time_submit = 0
            self.query_time_end = 0
            self.wait_time_start = 0
            self.wait_time_end = 0
            self.fetch_time_start = 0
            self.fetch_time_end = 0

        def submitQueryConnection(self):
            self.query_time_start = time.perf_counter()
            self.connection = pool.getconn()

        def submitQueryCreateCursor(self):
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
            self.pool.putconn(self.connection)

    class TasksList:
        def __init__(self):
            self.tasks = []
            self.execQueries = []
            self.tasksMutex = threading.Lock()

        # execute in order: submitQueryConnection, [wait], submitQueryCreateCursor, [wait], fetchQueryResult
        def initQueries(self, numQueries, query, pool):
            for i in range(numQueries):
                self.execQueries.append(QueryExecution(query, pool))
            self.tasks = deque()
            def fetchLambda(tasksList, execQuery):
                execQuery.fetchQueryResult()
            def waitQueryLambda(tasksList, execQuery):
                state = execQuery.query_cursor.connection.poll()
                if state == psycopg2.extensions.POLL_OK:
                    # wait is finished, go to next stage: fetchQueryResult
                    self.tasks.append(lambda: fetchLambda(tasksList, execQuery))
                elif state == psycopg2.extensions.POLL_WRITE or state == psycopg2.extensions.POLL_READ:
                    # wait is not finished, resubmit this same task at end of queue
                    self.tasks.append(lambda: waitQueryLambda(tasksList, execQuery))
                else:
                    raise psycopg2.OperationalError("poll() returned %s" % state)
            def submitQueryCreateCursorLambda(tasksList, execQuery):
                execQuery.submitQueryCreateCursor()
                tasksList.addTask(lambda: waitQueryLambda(tasksList, execQuery))
            def waitConnectionLambda(tasksList, execQuery):
                state = execQuery.connection.poll()
                if state == psycopg2.extensions.POLL_OK:
                    # wait is finished, go to next stage: submitQueryCreateCursorLambda
                    self.tasks.append(lambda: submitQueryCreateCursorLambda(tasksList, execQuery))
                elif state == psycopg2.extensions.POLL_WRITE or state == psycopg2.extensions.POLL_READ:
                    # wait is not finished, resubmit this same task at end of queue
                    self.tasks.append(lambda: waitConnectionLambda(tasksList, execQuery))
                else:
                    raise psycopg2.OperationalError("poll() returned %s" % state)
            def submitQueryConnectionLambda(tasksList, execQuery):
                execQuery.submitQueryConnection()
                my_wait(execQuery.connection)
                submitQueryCreateCursorLambda(tasksList, execQuery)
#                tasksList.addTask(lambda: waitConnectionLambda(tasksList, execQuery))
            for execQuery in self.execQueries:
                self.tasks.append(lambda execQuery=execQuery: submitQueryConnectionLambda(self, execQuery))

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


    allTasks = TasksList()
    allTasks.initQueries(N, query, pool)
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

    for future in futures:
        hasThrown = future.exception()
        if hasThrown:
            raise hasThrown
        print(future.result)
    for execQuery in allTasks.execQueries:
        print (execQuery.result)

    print("total time for N = {} executions : {} s".format(N,total_prog))
#    print("mean execution time : {} s".format(np.mean(times_total)))

#    bd.plot_fig(times_exe,times_wait,times_fetch,times_total, 'multithreading_perf')



def get_queries(file):
    with open(file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='#')
        qdict = {}
        #liste = []
        #line_count = 0
        for line in csv_reader:
            qdict[line[0]] = line[1]

    return qdict



def start_queries(mode,connection):
    queries_dict = qu.get_queries('queries_not_union.txt')

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
        
    f = open("results_sans_union_{}.txt".format(mode), "w")
    for key in keys_list:
        print(key)
        results = qu.execute_read_query(connection,queries_dict[key])
        values = results[0]
        runtime_exe = results[1]
        runtime_fetchall = results[2]
        ratio = (runtime_fetchall / runtime_exe + runtime_fetchall)*100 
        f.write("{} executed in {} seconds and fetched in {} seconds \n ratio : {} % \n".format(key,runtime_exe, runtime_fetchall,ratio))
    f.close()
