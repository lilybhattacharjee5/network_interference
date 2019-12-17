import pandas as pd
import sqlalchemy as sql

load_failed = True

x = 1000000 # the number of rows to load from the table at once
year = 2017 # specify the year of measurements that should be considered

sql_engine = sql.create_engine('postgresql:///metadb') # connect to the database...

start = 276 # the number of the row to start at (in millions)
count = start
end = 325 # number of the row to end at (in millions) -- note: if end - start = too big, process will be killed
file_num = int(end / 25) # measurements_(year)_(file_num).csv name specification

failed = []

# read in the first x rows to set a dataframe schema
meas = pd.read_sql_query("SELECT * from (select * from measurement limit " + str(x) + " offset " + str(count * x) + ") small_meas where to_char(small_meas.measurement_start_time, 'YYYY') = '" + str(year) + "'", sql_engine)
meas_full = meas
curr_meas = meas
print(len(meas_full)) # print the number of relevant rows added to the dataframe

while count < end:
    try:
        if count != start:
            meas_full = meas_full.append(curr_meas) # only append the result of the query if it isn't already in the df
        count += 1
        next_query = "SELECT * from (select * from measurement limit " + str(x) + " offset " + str(count * x) + ") small_meas where to_char(small_meas.measurement_start_time, 'YYYY') = '" + str(year) + "'"
        print(next_query)
        curr_meas = pd.read_sql_query(next_query, sql_engine)
        print(len(curr_meas)) # print the number of rows that will be added to the df in the next iteration
    except:
        failed.append(count)
        count += 1 # if accessing a certain group of x rows fails, move on to the next group of x rows without retrying
        continue

meas_full.to_csv("measurements_" + str(year) + "_" + str(file_num) + ".csv") # write the final df to a csv file
with open('failed_' + str(year) + '.txt', 'a') as f:
    for i in failed:
        f.write("%s\n" % i)
