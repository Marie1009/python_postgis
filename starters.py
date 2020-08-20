import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2 import OperationalError
from psycopg2 import sql
from psycopg2 import pool
#from psycopg2 import wait_select
import queries as qu
import connections as co
import bd
import select
import time

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




def wait(conn):
    while True:
        state = conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            print("wait : poll ok")
            break
        elif state == psycopg2.extensions.POLL_WRITE:
            select.select([], [conn.fileno()], [])
        elif state == psycopg2.extensions.POLL_READ:
            select.select([conn.fileno()], [], [])
        else:
            raise psycopg2.OperationalError("poll() returned %s" % state)

def exe_query_async_Ntimes(query, N):
    aconn = psycopg2.connect(database="postgis_test", user="postgres", host="127.0.0.1", port="5432", password="admin", async=1)
    wait(aconn)

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
        wait(acurs.connection)
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
    
def test_pool():
    pool = co.create_connection_pool(1,nbpool,"postgis_test","postgres","admin","localhost","5432",1)

    aconn = pool.getconn()
    wait(aconn)
    psycopg2.extras.wait_select(aconn)
    #print("wait aconn ok")
    acurs = aconn.cursor()

    start = time.perf_counter()
    acurs.execute(query)


def query_async_pool_Ntimes(query,N,nbpool):
   
    pool = co.create_connection_pool(1,nbpool,"postgis_test","postgres","admin","localhost","5432",1)
    #time.sleep(3)
    times_exe = []
    times_fetch = []
    times_wait = []
    times_total = []
    

    cursors = []
    connections = []

    print("Starting queries")
    for i in range(N):
        aconn  = pool.getconn()
        connections.append(aconn)

        if (aconn):
            print("get conn ok")
            #time.sleep(3)
            wait(aconn)
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
        wait(cur.connection)
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
                        
    #results = execute_read_query(ps_connection, query)
    #bd.plot_perf(times_exe,'execution')
    #bd.plot_perf(times_wait,'wait')
    #bd.plot_perf(times_fetch,'fetch')
    bd.plot_fig(times_exe,times_wait,times_fetch,times_total, 'test_async_pool_perf')
    #Use this method to release the connection object and send back ti connection pool


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
