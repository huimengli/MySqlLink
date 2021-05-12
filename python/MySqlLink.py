'''
数据库连接模块
'''

import MySQLdb;
import os;
import time;

#连接地址
LINKPATH = "localhost";

#连接mysql的用户名
USERNAME = "root";

#连接mysql的用户密码
USERPASSWORD = "password";

#连接的数据库名称
DATANAME = "flask1";

#数据编码
CHARSET = "utf8";

#报错显示位置
LOGPATH = "";

# 当前目录
NOWDIR = os.getcwd().replace('\\','/');

#------------------#
#以下内容不建议修改#
#------------------#
LOGPATH = LOGPATH and LOGPATH or NOWDIR+"/log.txt";

# 打开数据库连接
link = MySQLdb.connect(LINKPATH,USERNAME,USERPASSWORD,DATANAME,charset=CHARSET);

def getValue(sql):
    '''
    直接运行sql语句
    '''
    print(sql);

    try:
        # 使用cursor()方法获取操作游标 
        cursor = link.cursor()

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
        with open(LOGPATH,"a") as f:
            f.write("报告时间: "+str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())))
            f.write("错误内容: "+str(e));
            f.write("错误文件: "+str(e.__traceback__.tb_frame.f_globals["__file__"]));
            f.write("错误行数: "+str(e.__traceback__.tb_lineno));
            f.write("\n");
            if str(e)=="(2006, 'MySQL server has gone away')":
                f.write("系统尝试重新连接数据库...\n");
                try:
                    link.ping(True);

                    # 使用cursor()方法获取操作游标 
                    cursor = link.cursor()

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

                except Exception as e2:
                    f.write("系统重新连接数据库失败,错误内容:"+str(e2));
                    f.write(",尝试重启数据库连接...\n");
                    try:
                        link2 = MySQLdb.connect(LINKPATH,USERNAME,USERPASSWORD,DATANAME,charset=CHARSET);

                        # 使用cursor()方法获取操作游标 
                        cursor = link2.cursor()

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
                    except Exception as e3:
                        f.write("重启数据库连接失败,错误内容:"+str(e3));
                        return False;

    #finally:
    #    try:
    #        # 使用cursor()方法获取操作游标 
    #        cursor = link.cursor()

    #        # 使用execute方法执行SQL语句
    #        ret = cursor.execute(sql);

    #        # 使用 fetchone() 方法获取一条数据
    #        data = cursor.fetchall();

    #        if len(data)==0:
    #            if ret==0:
    #                return False;
    #            elif ret==1:
    #                return True;
    #        else:
    #            return data;
    #    except Exception as e:
    #        with open(LOGPATH,"a") as f:
    #            f.write("最后尝试操作数据库失败,错误内容:"+str(e));
    #            return False;

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
    #sql = "INSERT INTO `users` (`id`, `name`, `password`) VALUES ('0', 'name', 'ba436ed15d0b0da7518772e3b23acd94')";
    sql = "INSERT INTO `"+tablename+"` (`";
    for i in range(len(listnames)):
        x = listnames[i];
        if i==len(listnames)-1:
            sql+=x+"`) ";
        else:
            sql+=x+"`, `";

    sql+="VALUES ('";
    for i in range(len(values)):
        x = values[i];
        if i==len(values)-1:
            sql+=str(x)+"')";
        else:
            sql+=str(x)+"','";

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
    sql = "UPDATE `"+ tablename+"` SET `"+listname+"`='"+str(newvalue)+"' WHERE (`"+keyname+"`='"+str(keyvalue)+"')";
    ret = getValue(sql);
    link.commit();
    return ret;

def changeValues(tableName,listNames:list,newValues:list,keyName,keyValue)->bool:
    '''
    修改多项数据

    Error:
        传入数据错误
        传入数据量少于2
    '''
    sql = "UPDATE `files` SET `name`=' 1', `size`='1', `eachSize`='1' WHERE (`id`='6') LIMIT 1";
    sql = "UPDATE `"+tableName+"` SET `";
    if isinstance(listNames,list) and isinstance(newValues,list):
        for x in range(len(listNames)-1):
            sql+=str(listNames[x])+"`='"+str(newValues[x])+"', `";
        sql+= str(listNames[len(listNames)-1]) +"`='"+ newValues[len(listNames)-1] +"' WHERE (`"+str(keyName)+"`='"+ str(keyValue) +"')";
    else:
        return "Error";
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

def getCount(tablename):
    '''
    获取表中的数据条数
    '''
    columns = getColumns(tablename);
    sql = "SELECT COUNT("+columns[0]+") FROM "+tablename;
    ret = getValue(sql);
    if isinstance(ret,tuple):
        return ret[0][0];
    else:
        return 0;

def close():
    ''' 
    关闭数据库连接
    '''
    link.close();
