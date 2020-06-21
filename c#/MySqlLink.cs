using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Text.RegularExpressions;

namespace LtMySqlLink
{
    /// <summary>
    /// Mysql数据库连接方法
    /// </summary>
    public class MysqlLink
    {
        #region 数据内容
        /// <summary>
        /// 连接的数据库
        /// </summary>
        public MySqlConnection connection;

        /// <summary>
        /// 临时使用表
        /// </summary>
        public MySqlCommand temporary;
        #endregion

        #region 创建数据库连接

        /// <summary>
        /// 创建数据库连接
        /// </summary>
        /// <param name="connstr">连接信息</param>
        /// <example>connstr = "data source=localhost;database=forhtml;user id=root;password=root;pooling=false;charset=utf8";</example>
        public MysqlLink(string connstr)
        {
            connection = new MySqlConnection(connstr);
        }

        /// <summary>
        /// 创建数据库连接
        /// </summary>
        /// <param name="sqlName">数据库名称</param>
        /// <param name="sqlUserName">登录数据库账户</param>
        /// <param name="sqlPassWord">登录数据库密码</param>
        public MysqlLink(string sqlName, string sqlUserName, string sqlPassWord)
        {
            var connstr = "data source = localhost;database=" + sqlName + ";user id=" + sqlUserName + ";password=" + sqlPassWord + ";pooling=false;charset=utf8";
            connection = new MySqlConnection(connstr);
        }

        /// <summary>
        /// 创建数据库连接
        /// </summary>
        /// <param name="sqlName">数据库名称</param>
        /// <param name="sqlUserName">登录数据库账户</param>
        /// <param name="sqlPassWord">登录数据库密码</param>
        /// <param name="usePooling">是否使用连接池</param>
        public MysqlLink(string sqlName, string sqlUserName, string sqlPassWord, bool usePooling)
        {
            var pooling = usePooling ? "true" : "false";
            var connstr = "data source = localhost;database=" + sqlName + ";user id=" + sqlUserName + ";password=" + sqlPassWord + ";pooling=" + pooling + ";charset=utf8";
            connection = new MySqlConnection(connstr);
        }

        /// <summary>
        /// 创建数据库连接
        /// </summary>
        /// <param name="sqlName">数据库名称</param>
        /// <param name="sqlUserName">登录数据库账户</param>
        /// <param name="sqlPassWord">登录数据库密码</param>
        /// <param name="usePooling">是否使用连接池</param>
        /// <param name="encoded">编码方式</param>
        public MysqlLink(string sqlName, string sqlUserName, string sqlPassWord, bool usePooling, string encoded)
        {
            var pooling = usePooling ? "true" : "false";
            var connstr = "data source = localhost;database=" + sqlName + ";user id=" + sqlUserName + ";password=" + sqlPassWord + ";pooling=" + pooling + ";charset=" + encoded;
            connection = new MySqlConnection(connstr);
        }

        #endregion

        #region 表连接
        /// <summary>
        /// 获取数据库表内容
        /// </summary>
        /// <param name="sql">数据库语句</param>
        /// <returns></returns>
        /// <example>string sql = "select * from (数据库表名)";</example>
        public MySqlCommand GetCommand(string sql)
        {
            temporary = new MySqlCommand(sql, connection);
            return temporary;
        }

        /// <summary>
        /// 获取数据库表内容
        /// </summary>
        /// <param name="columnNames">列名</param>
        /// <param name="tableName">表名</param>
        /// <returns></returns>
        public MySqlCommand GetCommand(string columnNames, string tableName)
        {
            var sql = "select " + columnNames + " from " + tableName;
            temporary = new MySqlCommand(sql, connection);
            return temporary;
        }

        /// <summary>
        /// 获取数据库表内容
        /// </summary>
        /// <param name="columnNames">列名</param>
        /// <param name="tableName">表名</param>
        /// <param name="condition">条件语句</param>
        /// <returns></returns>
        public MySqlCommand GetCommand(string columnNames, string tableName, string condition)
        {
            var sql = "select " + columnNames + " from " + tableName + " ";
            sql += "Where " + condition;
            temporary = new MySqlCommand(sql, connection);
            return temporary;
        }
        #endregion

        #region 开关数据连接
        /// <summary>
        /// 打开连接
        /// </summary>
        public void Open()
        {
            connection.Open();
        }

        /// <summary>
        /// 关闭连接
        /// </summary>
        public void Close()
        {
            connection.Close();
        }
        #endregion

        #region 获取表信息

        /// <summary>
        /// 获取表的所有列名
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <returns></returns>
        public List<string> GetColumns(string tableName)
        {
            var sql = "show columns from " + tableName + " ;";
            var cmd = new MySqlCommand(sql, connection);
            var ret = new List<string>();
            Open();
            var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                ret.Add(reader.GetString("field"));
            }
            Close();
            return ret;
        }

        /// <summary>
        /// 获取临时表所有列名
        /// </summary>
        /// <returns></returns>
        public List<string> GetColumns()
        {
            var getTableName = new Regex(@"from ([^ ]+) ?[wher]{0,5}");
            var tableName = getTableName.Match(temporary.CommandText).Groups[1].ToString();
            return GetColumns(tableName);
        }

        /// <summary>
        /// 获取表的所有内容
        /// </summary>
        /// <returns></returns>
        public List<List<object>> GetAll(string tableName)
        {
            var ret = new List<List<object>>();
            var columns = GetColumns(tableName);
            Open();
            var cmd = GetCommand("*", tableName);
            var reader = cmd.ExecuteReader();
            ret.Add(new List<object>());
            foreach (var item in columns)
            {
                ret[0].Add(item);
            }
            var j = 0;
            while (reader.Read())
            {
                ret.Add(new List<object>());
                j++;
                for (int i = 0; i < columns.Count; i++)
                {
                    if (reader.IsDBNull(i))
                    {
                        ret[j].Add(null);
                    }
                    else
                    {
                        ret[j].Add(reader.GetString(columns[i]));
                    }
                }
            }
            Close();
            return ret;
        }

        /// <summary>
        /// 获取临时表的所有内容
        /// </summary>
        /// <returns></returns>
        public List<List<object>> GetAll()
        {
            var getTableName = new Regex(@"from ([^ ]+) ?[wher]{0,5}");
            var tableName = getTableName.Match(temporary.CommandText).Groups[1].ToString();
            return GetAll(tableName);
        }

        /// <summary>
        /// 获取表的某几项内容
        /// </summary>
        /// <param name="columns">某一列数据</param>
        /// <param name="tablename">表名</param>
        /// <returns></returns>
        public List<List<object>> GetAll(List<string> columns, string tableName)
        {
            var ret = new List<List<object>>();
            Open();
            var columnNames = "`";
            for (int i = 0; i < columns.Count; i++)
            {
                if (i > 0)
                {
                    columnNames += "`,`";
                }
                columnNames += columns[i];
            }
            columnNames += "`";
            var cmd = GetCommand(columnNames, tableName);
            var reader = cmd.ExecuteReader();
            ret.Add(new List<object>());
            foreach (var item in columns)
            {
                ret[0].Add(item);
            }
            var j = 0;
            while (reader.Read())
            {
                ret.Add(new List<object>());
                j++;
                for (int i = 0; i < columns.Count; i++)
                {
                    if (reader.IsDBNull(i))
                    {
                        ret[j].Add(null);
                    }
                    else
                    {
                        ret[j].Add(reader.GetString(columns[i]));
                    }
                }
            }
            Close();
            return ret;
        }

        #endregion

        #region 修改表信息

        /// <summary>
        /// 修改表信息
        /// </summary>
        /// <param name="sql">sql语句</param>
        /// <returns></returns>
        public bool ChangeValue(string sql)
        {
            temporary = new MySqlCommand(sql, connection);
            Open();
            var ret = temporary.ExecuteNonQuery();
            Close();
            return ret==0 ? false : true;
        }

        /// <summary>
        /// 修改表信息
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public bool ChangeValueWithOutOpen(string sql)
        {
            temporary = new MySqlCommand(sql, connection);
            var ret = temporary.ExecuteNonQuery();
            Close();
            return ret == 0 ? false : true;
        }

        /// <summary>
        /// 修改表信息(更新数据)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ColumnName">列名</param>
        /// <param name="ColumnValue">新值</param>
        /// <param name="keyName">主键名</param>
        /// <param name="key">主键值</param>
        /// <returns></returns>
        public bool ChangeValue(string tableName,string ColumnName,string ColumnValue,string keyName,string key)
        {
            var sql = "UPDATE `"+ tableName + "` SET `"+ ColumnName + "`='"+ ColumnValue + "' WHERE (`"+ keyName + "`='"+ key + "')";
            return ChangeValue(sql);
        }

        /// <summary>
        /// 修改表信息(更新数据)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="ColumnName">列名</param>
        /// <param name="ColumnValue">新值</param>
        /// <param name="keyName">主键名</param>
        /// <param name="key">主键值</param>
        /// <returns></returns>
        public bool ChangeValueWithOutOpen(string tableName,string ColumnName,string ColumnValue,string keyName,string key)
        {
            var sql = "UPDATE `"+ tableName + "` SET `"+ ColumnName + "`='"+ ColumnValue + "' WHERE (`"+ keyName + "`='"+ key + "')";
            return ChangeValueWithOutOpen(sql);
        }

        /// <summary>
        /// 修改表信息(删除数据)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="keyName">主键名</param>
        /// <param name="key">主键值</param>
        /// <returns></returns>
        public bool ChangeValue(string tableName,string keyName,string key)
        {
            var sql = "DELETE FROM `"+tableName+"` WHERE (`"+ keyName + "`='"+ key + "')";
            return ChangeValue(sql);
        }

        /// <summary>
        /// 修改表信息(删除数据)
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="keyName">主键名</param>
        /// <param name="key">主键值</param>
        /// <returns></returns>
        public bool ChangeValueWithOutOpen(string tableName,string keyName,string key)
        {
            var sql = "DELETE FROM `"+tableName+"` WHERE (`"+ keyName + "`='"+ key + "')";
            return ChangeValueWithOutOpen(sql);
        }

        /// <summary>
        /// 修改表信息(添加数据)
        /// </summary>
        /// <param name="tableName">数据表名</param>
        /// <param name="columns">所有列名</param>
        /// <param name="columnValues">所有列名数据</param>
        /// <returns></returns>
        public bool ChangeValue(string tableName, List<string> columns, List<object> columnValues)
        {
            var sql = "INSERT INTO `" + tableName + "` (`";
            for (int i = 0; i < columns.Count - 1; i++)
            {
                sql += columns[i] + "`, `";
            }
            sql += columns[columns.Count-1] + "`) VALUES ('";
            for (int i = 0; i < columnValues.Count-1; i++)
            {
                sql += columnValues[i] + "', '";
            }
            sql += columnValues[columnValues.Count-1] +"')";
            return ChangeValue(sql);
        }

        /// <summary>
        /// 修改表信息(添加数据)
        /// </summary>
        /// <param name="tableName">数据表名</param>
        /// <param name="columns">所有列名</param>
        /// <param name="columnValues">所有列名数据</param>
        /// <returns></returns>
        public bool ChangeValueWithOutOpen(string tableName, List<string> columns, List<object> columnValues)
        {
            var sql = "INSERT INTO `" + tableName + "` (`";
            for (int i = 0; i < columns.Count - 1; i++)
            {
                sql += columns[i] + "`, `";
            }
            sql += columns[columns.Count-1] + "`) VALUES ('";
            for (int i = 0; i < columnValues.Count-1; i++)
            {
                sql += columnValues[i] + "', '";
            }
            sql += columnValues[columnValues.Count-1] +"')";
            return ChangeValueWithOutOpen(sql);
        }

        /// <summary>
        /// 修改表信息(录入表的所有数据)
        /// </summary>
        /// <param name="tableName">数据表名</param>
        /// <param name="columns">所有列名</param>
        /// <param name="lists">表的所有数据</param>
        /// <returns></returns>
        public bool ChangeValue(string tableName,List<string> columns,List<List<object>> lists)
        {
            var ret = true;
            foreach (var i in lists)
            {
                if (ret)
                {
                    ret = ChangeValue(tableName, columns, i);
                }
                else
                {
                    throw new Exception("录入所有数据时出错");
                }
            }
            return ret;
        }

        /// <summary>
        /// 修改表信息(录入表的所有数据)
        /// </summary>
        /// <param name="tableName">数据表名</param>
        /// <param name="columns">所有列名</param>
        /// <param name="lists">表的所有数据</param>
        /// <returns></returns>
        public bool ChangeValueWithOutOpen(string tableName,List<string> columns,List<List<object>> lists)
        {
            var ret = true;
            foreach (var i in lists)
            {
                if (ret)
                {
                    ret = ChangeValueWithOutOpen(tableName, columns, i);
                }
                else
                {
                    throw new Exception("录入所有数据时出错");
                }
            }
            return ret;
        }

        #endregion

        #region 删除表

        /// <summary>
        /// 删除表(数据和结构)
        /// 立刻释放磁盘空间，不管是innodb和myisam
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public bool DeleteTable(string tableName)
        {
            var sql = "DROP TABLE `" + tableName + "`";
            return ChangeValue(sql);
        }

        /// <summary>
        /// 删除表(是否仅删除数据,保留表结构)
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="onlyData">
        /// 0:删除数据和结构
        /// 1:仅删除数据
        /// 2:仅删除数据(innodb不会释放空间)
        /// </param>
        /// <returns></returns>
        public bool DeleteTable(string tableName,int onlyData)
        {
            var sql = "";
            if (onlyData==1)
            {
                sql = "TRUNCATE TABLE `" + tableName + "`";
                return ChangeValue(sql);
            }
            else if (onlyData==0)
            {
                return DeleteTable(tableName);
            }
            else if (onlyData==2)
            {
                sql = "DELETE FROM `" + tableName + "`";
                return ChangeValue(sql);
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// 删除表(带条件删除)
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="where">删除条件</param>
        /// <returns></returns>
        public bool DeleteTalbe(string tableName,string where)
        {
            var sql = "DELETE FROM `" + tableName + "` WHERE "+where;
            return ChangeValue(sql);
        }


        #endregion
    }
}
