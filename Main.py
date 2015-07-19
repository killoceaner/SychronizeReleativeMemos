#!/usr/bin/env python
# -* - coding: UTF-8 -* -
__author__ = 'houxiang'

import  MySQLdb
import  logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('log.log')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)


targetDB={'host':"192.168.80.130","user":"trustie","passwd":"1234","port":3306,"database":"ossean_production"}

TargetConn = MySQLdb.connect(host=targetDB["host"],user=targetDB["user"],passwd=targetDB["passwd"],port=targetDB["port"])

handleSqlCountPart1 = 'select DISTINCT relative_memo_id from '

handleSqlCountPart2 = 'where "has_synchronized" = 0 '

handleSqlSource = 'SELECT created_time, replies_num, view_num_crawled, memo_type, view_num_trustie, COUNT(*)  FROM relative_memos WHERE id = %s;'

handleSqlUpdate = ' SET created_time = %s, replies_num = %s, view_num_crawled = %s, memo_type = %s, view_num_trustie = %s, has_synchronized = 1 WHERE relative_memo_id = %s;'

dicTable = {'tmp_created_time':'','tmp_replies_num':'','tmp_view_num_crawled':'','tmp_memo_type':'','tmp_view_num_trustie':''}

def main():
    cur = TargetConn.cursor()
    TargetConn.select_db(targetDB["database"])
    for num in range(1,71):
        sqlOfCount = handleSqlCountPart1 +"`relative_memo_to_open_source_projects_"+ str(num)+"`" + handleSqlCountPart2
        print sqlOfCount
        sqlOfUpdate = "update"+" `relative_memo_to_open_source_projects_"+str(num)+"`"+handleSqlUpdate
        count = cur.execute(sqlOfCount)
        result = cur.fetchall()
        TargetConn.commit()
        for ans in result:
            countMemos = cur.execute(handleSqlSource,ans)
            logger.info(countMemos)
            resultOfMenos = cur.fetchone()
            TargetConn.commit()
            tmp_created_time = resultOfMenos[0]
            tmp_replies_num = resultOfMenos[1]
            tmp_view_num_crawled = resultOfMenos[2]
            tmp_memo_type = resultOfMenos[3]
            tmp_view_num_trustie = resultOfMenos[4]
            args = (tmp_created_time , tmp_replies_num , tmp_view_num_crawled ,tmp_memo_type , tmp_view_num_trustie , ans)
            countRelativeMemos = cur.execute(sqlOfUpdate,args)
            if countRelativeMemos > 0:
                logger.info("one record insert success !")
            TargetConn.commit()
    TargetConn.close()

if  __name__ == '__main__':
    main()


