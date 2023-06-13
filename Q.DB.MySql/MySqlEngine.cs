/*************************************************************************************
 *
 * 文 件 名:   MySqlEngine.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/11/25 14:53
 * 
 * ======================================
 * 历史更新记录
 * 版本：V          修改时间：         修改人：
 * 修改内容：
 * ======================================
 * 
*************************************************************************************/

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.PortableExecutable;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using MySqlConnector;
using Q.DB.Interface;
using Q.DB.Models;
namespace Q.DB.MySql
{
    public class MySqlEngine : IDBEngine
    {

        DBConnPool ConnPool;

        public string TypeName => "MySql";
        public string ConnStr { get; set; }
        public string DefaultDBName { get; set; }
        public string LastIdentitySql => "SELECT LAST_INSERT_ID();";

        public string Sl => "`";

        public string Sr => "`";

        public MySqlEngine(string connstr)
        {
            if (string.IsNullOrEmpty(connstr))
            {
                throw new Exception("链接无效");
            }
            else
            {
                this.DefaultDBName = connstr.Split(';').FirstOrDefault(x => x.ToLower().Contains("database"))?.Split('=')?[1]?.Trim();
                this.ConnStr = connstr;
                ConnPool = new DBConnPool(this);
            }
        }

        #region 其它方法
        /// <summary>
        /// 检查链接是否有效
        /// </summary>
        /// <param name="conn"></param>
        /// <returns></returns>
        public bool CheckConn(DbConnection conn)
        {
            if (conn == null) return false;
            var res = ((MySqlConnection)conn).Ping();
            return res;
        }
        /// <summary>
        /// 创建连接
        /// </summary>
        /// <returns></returns>
        public DbConnection CreateConn()
        {
            return new MySqlConnection(ConnStr);
        }

        public DBConnectionItem GetConnection(string dbName)
        {
            return ConnPool.GetConnection(dbName ?? DefaultDBName);
        }

        public string ConvertSelectLimit(string sql, int count)
        {
            return sql += " limit " + count;
        }

        public long BulkInsert<T>(DBConnectionItem connectionItem, List<T> ts, string tableSuffix, int splitCount)
        {

            var tableName = EntityCache.TryGetRealTableName<T>() + QDBTools.ConvertSuffixTableName(tableSuffix);
            string tempFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"{tableName}_{QDBTools.RandomCode(8, 0)}.sqlLocal");
            var felids = QDBTools.ConvertToInsertCvsWriteToFile(ts, tempFile);
            long st = System.Environment.TickCount;
            var mySqlBulkLoader = new MySqlBulkLoader((MySqlConnection)connectionItem.SqlConnection)
            {
                FileName = tempFile,
                TableName = tableName,
                FieldTerminator = ",",
                FieldQuotationCharacter = '"',
                FieldQuotationOptional = true,
                Local = true,
                LineTerminator = "\r\n",
            };
            mySqlBulkLoader.Columns.AddRange(felids);
            int result = mySqlBulkLoader.Load();
            SqlLogUtil.SendLog(LogType.SQL, "MySqlBulkLoader :"+ ts.Count, System.Environment.TickCount - st);
            File.Delete(tempFile);
            return result;
        }

        public async Task<long> BulkInsertAsync<T>(DBConnectionItem connectionItem, List<T> ts, string tableSuffix, int splitCount)
        {
            var tableName = EntityCache.TryGetRealTableName<T>() + QDBTools.ConvertSuffixTableName(tableSuffix);

            string tempFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"{tableName}_{QDBTools.RandomCode(8, 0)}.sqlLocal");
            var felids = QDBTools.ConvertToInsertCvsWriteToFile(ts, tempFile);
            long st = System.Environment.TickCount;

            var mySqlBulkLoader = new MySqlBulkLoader((MySqlConnection)connectionItem.SqlConnection)
            {
                FileName = tempFile,
                TableName = tableName,
                FieldTerminator = ",",
                FieldQuotationCharacter = '"',
                FieldQuotationOptional = true,
                Local = true,
                LineTerminator = "\r\n",
            };
            mySqlBulkLoader.Columns.AddRange(felids);
            int result = await mySqlBulkLoader.LoadAsync();
            SqlLogUtil.SendLog(LogType.SQL, "MySqlBulkLoader :" + ts.Count, System.Environment.TickCount - st);
            File.Delete(tempFile);
            return result;
        }
        #endregion





        public bool CheckTableExisted(DBConnectionItem connectionItem, string tableName)
        {
            string sql = $"SELECT Count(*) FROM INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = '{connectionItem.SqlConnection.Database}' AND TABLE_NAME = '{tableName}';";
            return ((IDBEngine)this).ExecuteScalar<int>(connectionItem, sql, null, false) > 0;
        }

        public bool CreateTable<T>(DBConnectionItem connectionItem, string tableName, bool force = false, string tableEngine = "Default", string[] additionals = null)
        {
            var Engine = ((IDBEngine)this);
            if (force)
            {
                string sql = $"DROP TABLE {tableName} ;";
                Engine.ExecuteNonQuery(connectionItem, sql, null);
            }
            var createSql = T2CreateSql<T>(tableName, tableEngine, additionals);
            return Engine.ExecuteNonQuery(connectionItem, createSql, null) > 0;
        }

        public bool CreateTableIfNotExists<T>(DBConnectionItem connectionItem, string tableName, string tableEngine = "Default", string[] additionals = null)
        {
            var createSql = $"DROP TABLE IF EXISTS {connectionItem.SqlConnection.Database}.{tableName};" + T2CreateSql<T>(tableName, tableEngine, additionals);
            return ((IDBEngine)this).ExecuteNonQuery(connectionItem, createSql, null) > 0;
        }

        private string T2CreateSql<T>(string tableName, string tableEngine = "Default", string[] additionals = null)
        {
            ValueStringBuilder sb = new ValueStringBuilder(1024);
            sb.Append($"CREATE TABLE {tableName}(");
            var nea = EntityCache.TryGetInfo<T>();

            foreach (var item in nea.PropertyInfos)
            {
                string sqlType = string.Empty;
                if (!string.IsNullOrEmpty(item.Value.DBType))
                {
                    if (item.Value.DBType == "JSONSTR")
                    {
                        sqlType = "text";
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(item.Value.DBLength))
                        {
                            sqlType = $"{item.Value.DBType}({item.Value.DBLength})";
                        }
                        else
                        {
                            sqlType = $"{item.Value.DBType}";
                        }
                    }
                }
                else
                {
                    if (item.Value.PropertyType == typeof(string))
                    {
                        sqlType = $"varchar({item.Value.DBLength ?? "255"})";
                    }
                    else if (item.Value.PropertyType == typeof(DateTime) || item.Value.PropertyType == typeof(DateTime?))
                    {
                        sqlType = "datetime";
                    }
                    else if (item.Value.PropertyType == typeof(decimal) || item.Value.PropertyType == typeof(decimal?))
                    {
                        sqlType = $"decimal({item.Value.DBLength ?? "15, 3"})";
                    }
                    else if (item.Value.PropertyType == typeof(int) || item.Value.PropertyType == typeof(int?))
                    {
                        sqlType = $"int({item.Value.DBLength ?? "11"})";
                    }
                    else if (item.Value.PropertyType == typeof(long) || item.Value.PropertyType == typeof(long?))
                    {
                        sqlType = "bigint";
                    }
                    else
                    {
                        sqlType = $"varchar({item.Value.DBLength ?? "255"})";
                    }
                }
                sb.Append($"{Sl}{item.Key}{Sr} {sqlType} {(nea.PrimaryKeys.Contains(item.Key) ? "NOT NULL" : "")} {(nea.AutoIncrements.Contains(item.Key) ? "AUTO_INCREMENT" : "")} COMMENT '{item.Value.Remark ?? ""}',");
            }

            if (nea.PrimaryKeys.Count > 0)
            {
                sb.Append($"PRIMARY KEY ({Sl}{string.Join($"{Sr},{Sl}", nea.PrimaryKeys)}{Sr})");
            }
            var indexs = EntityCache.Index<T>();

            if (indexs.Count > 0)
            {
                foreach (var item in indexs)
                {
                    string indexFileds = string.Join($"{Sr},{Sl}", item.IndexFields.Split(','));
                    sb.Append($",KEY {Sl}{item.IndexName}{Sr} ({Sl}{indexFileds}{Sr})");
                }
            }
            var sql = sb.ToString().TrimEnd(',') + ")";
            if (tableEngine != "Default")
            {
                sql += " engine=" + tableEngine;
            }
            if (additionals != null)
            {
                foreach (var additional in additionals)
                {
                    sql += $" {additional}";
                }
            }
            return sql;
        }
    }
}
