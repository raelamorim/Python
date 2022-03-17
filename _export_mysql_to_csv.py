import pymysql
import pandas

conn = pymysql.connect(host='localhost', user='root', password='MySql2019!', database='testepythondb')
cursor = conn.cursor()
query = 'select id, height, weight from height_weight'

fmt = {
    'id':'{:06d}'.format,
    'height': '{:09.5f}'.format, 
    'weight': '{:08.4f}'.format
}

results = pandas.read_sql_query(query, conn)
# results.to_csv('./test_files/hw.csv', sep = ';', index=False)
results.to_string('./test_files/hw.txt', formatters = fmt, index=False)