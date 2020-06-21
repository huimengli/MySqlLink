'''
数据库连接模块
'''

import MySQLdb;

#连接地址
LINKPATH = "localhost";

#连接mysql的用户名
USERNAME = "root";

#连接mysql的用户密码
USERPASSWORD = "root";

#连接的数据库名称
DATANAME = "flask1";

#数据编码
CHARSET = "utf8";

# 打开数据库连接
link = MySQLdb.connect(LINKPATH,USERNAME,USERPASSWORD,DATANAME,charset=CHARSET);

def getValue(sql):
    '''
    直接运行sql语句
    '''
    print(sql);

    # 使用cursor()方法获取操作游标 
    cursor = link.cursor()

    try:
        # 使用execute方法执行SQL语句
        ret = cursor.execute(sql);

        #data1 = cursor.fet

        # 使用 fetchone() 方法获取一条数据
        data = cursor.fetchall();

        if len(data)==0:
            if ret==0:
                return False;
            elif ret==1:
                return True;
        else:
            return data;
    except Exception as e:
        print(e);
        return False;

def getTable(tablename,listname=None,where=None):
    '''
    获取表的信息
    '''
    sql = "SELECT ";
    if listname==None:
        sql += "* FROM ";
    elif isinstance(listname,list):
        for x in listname:
            if listname.index(x)==len(listname)-1:
                sql+=("`"+x+"` FROM ");
            else:
                sql+=("`"+x+"`,");
    else:
        sql+=listname+" FROM ";

    sql+=tablename;

    if not where==None:
        sql+=" WHERE "+where;

    return getValue(sql);

def addValue(tablename,listnames,values):
    '''
    向表中添加数据
    '''
    #sql = "INSERT INTO `users` (`id`, `name`, `password`) VALUES ('0', '楼听', 'ba436ed15d0b0da7518772e3b23acd94')";
    sql = "INSERT INTO `"+tablename+"` (`";
    for x in listnames:
        if listnames.index(x)==len(listnames)-1:
            sql+=x+"`) ";
        else:
            sql+=x+"`, `";

    sql+="VALUES ('";
    for x in values:
        if values.index(x)==len(values)-1:
            sql+=x+"')";
        else:
            sql+=x+"','";

    ret = getValue(sql);
    link.commit();
    return ret;

def addValues(tablename,listnames,valuess):
    '''
    向表中添加多条数据
    '''
    ret = False;
    for x in valuess:
        ret = addValue(tablename,listnames,x);

    link.commit();
    return ret;

def changeValue(tablename,listname,newvalue,keyname,keyvalue):
    '''
    修改数据
    '''
    sql = "UPDATE `users` SET `name`='lt' WHERE (`id`='1') LIMIT 1";
    sql = "UPDATE `"+ tablename+"` SET `"+listname+"`='"+newvalue+"' WHERE (`"+keyname+"`='"+keyvalue+"')";
    ret = getValue(sql);
    link.commit();
    return ret;

def getColumns(tablename):
    '''
    获取表的所有列名
    '''
    sql = "show columns from " + tablename + " ;";
    rets = getValue(sql);
    ret = [];
    for x in rets:
        ret.append(x[0]);

    return ret;

def deleteValue(tablename,keyname,key):
    '''
    删除一条信息
    '''
    sql = "DELETE FROM `"+tablename+"` WHERE (`"+ keyname + "`='"+ key + "')";
    ret = getValue(sql);
    link.commit();
    return ret;

def deleteTable(tablename):
    '''
    删除数据表(数据和结构)
    '''
    sql = "DROP TABLE `" + tablename + "`";
    ret = getValue(sql);
    link.commit();
    return ret;

def deleteTableValues(tablename):
    '''
    删除数据表(全部数据)
    '''
    sql = "TRUNCATE TABLE `" + tablename + "`";
    ret = getValue(sql);
    link.commit();
    return ret;

def close():
    # 关闭数据库连接
    link.close();
