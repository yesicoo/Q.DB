/*************************************************************************************
 *
 * 文 件 名:   SqlServerEngine.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/11/25 14:45
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
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Q.DB.Interface;
using Q.DB.Models;

namespace Q.DB.SqlServer
{
    public class SqlServerEngine : IDBEngine
    {
        DBConnPool ConnPool;
        public string TypeName => "SqlServer";

        public string Sl => "[";

        public string Sr => "]";

        public string ConnStr { get; set; }
        public string DefaultDBName { get; set; }

        public string LastIdentitySql => "SELECT SCOPE_IDENTITY();";

        public SqlServerEngine(string connstr)
        {
            if (string.IsNullOrEmpty(connstr))
            {
                throw new Exception("链接无效");
            }
            else
            {
                this.DefaultDBName = connstr.Split(';').FirstOrDefault(x => x.ToLower().Contains("initial catalog"))?.Split('=')?[1]?.Trim();
                this.ConnStr = connstr;
                ConnPool = new DBConnPool(this);
            }
        }

        public bool CheckConn(DbConnection conn)
        {
            if (conn == null) return false;
            return ((SqlConnection)conn).State == System.Data.ConnectionState.Open;
        }

        public DbConnection CreateConn()
        {
            return new SqlConnection(ConnStr);
        }

        public DBConnectionItem GetConnection(string dbName)
        {
            return ConnPool.GetConnection(dbName ?? DefaultDBName);
        }

        public string ConvertSelectLimit(string sql, int count)
        {
            return $"top {count} " + sql;
        }

        public string CreatePageSql(string tableName, string fieldsStr, string where, string groupBy, string orderBy, int pageNum, int pageSize, string dbVersion)
        {
            if (string.IsNullOrEmpty(groupBy))
            {
                orderBy = " Order By  GetDate()";
            }
            int dbVer = 0;
            try
            {
                dbVer = int.Parse(dbVersion.Split('.')[0]);
            }
            catch (Exception ex)
            {
                SqlLogUtil.SendLog(LogType.Error, $"数据库服务器版本获取错误:{dbVersion}-->" + ex.Message);
            }
            if (dbVer <= 10)
            {
                return $"WITH t AS ( SELECT {fieldsStr}, ROW_NUMBER() OVER(  {orderBy}) AS __rownum__ FROM {tableName} {where} {groupBy}  ) SELECT t.* FROM t where __rownum__ between {(pageNum - 1) * pageSize + 1} and {pageNum * pageSize};";

            }
            else
            {
                return $"SELECT {fieldsStr} FROM {tableName} {where} {groupBy} {orderBy} OFFSET {(pageNum - 1) * pageSize} ROW FETCH NEXT {pageSize} ROW ONLY;";

            }
        }

        public string CreatePageSql(string sql, int pageNum, int pageSize, string dbVersion)
        {

            int dbVer = 0;
            try
            {
                dbVer = int.Parse(dbVersion.Split('.')[0]);
            }
            catch (Exception ex)
            {
                SqlLogUtil.SendLog(LogType.Error, $"数据库服务器版本获取错误:{dbVersion}-->" + ex.Message);
            }
            if (dbVer > 10)
            {
                return $"SELECT {sql} OFFSET {(pageNum - 1) * pageSize} ROW FETCH NEXT {pageSize} ROW ONLY;";

            }
            else
            {
                throw new Exception($"CreatePageSql:{sql}分页 SqlServer 不支持");
            }
        }

        public long BulkInsert<T>(DBConnectionItem connectionItem, List<T> ts, string tableSuffix, int splitCount)
        {
            var tableName = EntityCache.TryGetTableName<T>() + QDBTools.ConvertSuffixTableName(tableSuffix);

            long result = 0;
            if (ts.Count > splitCount)
            {
                int batchCount = ts.Count / splitCount + 1;
                for (int i = 0; i < batchCount; i++)
                {
                    var tempdatas = ts.Skip(i * splitCount).Take(splitCount).ToList();
                    using (SqlBulkCopy sqlRevdBulkCopy = new SqlBulkCopy(connectionItem.SqlConnection.ConnectionString))
                    {
                        try
                        {
                            var dataTable = this.ConvertToDataTable<T>(tempdatas);
                            sqlRevdBulkCopy.DestinationTableName = tableName;
                            sqlRevdBulkCopy.NotifyAfter = dataTable.Rows.Count;
                            sqlRevdBulkCopy.BulkCopyTimeout = int.MaxValue;
                            sqlRevdBulkCopy.WriteToServer(dataTable);
                            sqlRevdBulkCopy.Close();
                            result += dataTable.Rows.Count;
                        }
                        catch (Exception ex)
                        {
                            SqlLogUtil.SendLog(LogType.Error, "入库失败:\r\n" + ex.ToString());
                            throw;
                        }
                    }
                }
            }
            else
            {
                using (SqlBulkCopy sqlRevdBulkCopy = new SqlBulkCopy(connectionItem.SqlConnection.ConnectionString))
                {
                    try
                    {
                        var dataTable = this.ConvertToDataTable<T>(ts);
                        sqlRevdBulkCopy.DestinationTableName = tableName;
                        sqlRevdBulkCopy.NotifyAfter = dataTable.Rows.Count;
                        sqlRevdBulkCopy.BulkCopyTimeout = int.MaxValue;
                        sqlRevdBulkCopy.WriteToServer(dataTable);
                        sqlRevdBulkCopy.Close();
                        result += dataTable.Rows.Count;
                    }
                    catch (Exception ex)
                    {
                        SqlLogUtil.SendLog(LogType.Error, "入库失败:\r\n" + ex.ToString());
                        throw;
                    }
                }
            }
            return result;
        }


        public async Task<long> BulkInsertAsync<T>(DBConnectionItem connectionItem, List<T> ts, string tableSuffix, int splitCount)
        {

            var tableName = EntityCache.TryGetTableName<T>() + QDBTools.ConvertSuffixTableName(tableSuffix);

            long result = 0;
            if (ts.Count > splitCount)
            {
                int batchCount = ts.Count / splitCount + 1;
                for (int i = 0; i < batchCount; i++)
                {
                    var tempdatas = ts.Skip(i * splitCount).Take(splitCount).ToList();
                    using (SqlBulkCopy sqlRevdBulkCopy = new SqlBulkCopy(connectionItem.SqlConnection.ConnectionString))
                    {
                        try
                        {
                            var dataTable = this.ConvertToDataTable<T>(tempdatas);
                            sqlRevdBulkCopy.DestinationTableName = tableName;
                            sqlRevdBulkCopy.NotifyAfter = dataTable.Rows.Count;
                            sqlRevdBulkCopy.BulkCopyTimeout = int.MaxValue;
                            await sqlRevdBulkCopy.WriteToServerAsync(dataTable);
                            sqlRevdBulkCopy.Close();
                            result += dataTable.Rows.Count;
                        }
                        catch (Exception ex)
                        {
                            SqlLogUtil.SendLog(LogType.Error, "入库失败:\r\n" + ex.ToString());
                            throw;
                        }
                    }
                }
            }
            else
            {
                using (SqlBulkCopy sqlRevdBulkCopy = new SqlBulkCopy(connectionItem.SqlConnection.ConnectionString))
                {
                    try
                    {
                        var dataTable = this.ConvertToDataTable<T>(ts);
                        sqlRevdBulkCopy.DestinationTableName = tableName;
                        sqlRevdBulkCopy.NotifyAfter = dataTable.Rows.Count;
                        sqlRevdBulkCopy.BulkCopyTimeout = int.MaxValue;
                        await sqlRevdBulkCopy.WriteToServerAsync(dataTable);
                        sqlRevdBulkCopy.Close();
                        result += dataTable.Rows.Count;
                    }
                    catch (Exception ex)
                    {
                        SqlLogUtil.SendLog(LogType.Error, "入库失败:\r\n" + ex.ToString());
                        throw;
                    }
                }
            }
            return result;
        }

        

        public bool CheckTableExisted(DBConnectionItem connectionItem, string tableName)
        {
            string sql = $"select count(*) from sysobjects where id = object_id('{tableName}') and OBJECTPROPERTY(id, 'IsUserTable') = 1)";
            return ((IDBEngine)this).ExecuteScalar<int>(connectionItem, sql, null, false) > 0;
        }

        public bool CreateTable<T>(DBConnectionItem connectionItem, string tableName, bool force = false, string tableEngine = "Default", string[] additionals = null)
        {
            var Engine = ((IDBEngine)this);
            if (force)
            {
                string sql = $"DROP TABLE dbo.{tableName};";
                Engine.ExecuteNonQuery(connectionItem, sql, null);
            }
            var createSql = T2CreateSql<T>(tableName, additionals);
            return Engine.ExecuteNonQuery(connectionItem, createSql, null) > 0;
        }

        public bool CreateTableIfNotExists<T>(DBConnectionItem connectionItem, string tableName, string tableEngine = "Default", string[] additionals = null)
        {
            int dbVer = 0;
            try
            {
                dbVer = int.Parse(connectionItem.SqlConnection.ServerVersion.Split('.')[0]);
            }
            catch (Exception ex)
            {
                SqlLogUtil.SendLog(LogType.Error, $"数据库服务器版本获取错误:{connectionItem.SqlConnection.ServerVersion}-->" + ex.Message);
            }
            if (dbVer >= 13)
            {
                //2016及以上版本支持DROP TABLE IF EXISTS
                var createSql = $"DROP TABLE IF EXISTS  dbo.{tableName}\r\n;" + T2CreateSql<T>(tableName, additionals);
                return ((IDBEngine)this).ExecuteNonQuery(connectionItem, createSql, null) > 0;
            }
            else
            {
                var createSql = $"IF OBJECT_ID('dbo.{tableName}','U') IS NOT NULL\r\nDROP TABLE PERSON\r\n" + T2CreateSql<T>(tableName, additionals);
                return ((IDBEngine)this).ExecuteNonQuery(connectionItem, createSql, null) > 0;
            }


        }

        public bool RenameTable(DBConnectionItem connectionItem, string oldTableName,string newTableName)
        {
            var Engine = ((IDBEngine)this);
            return Engine.ExecuteNonQuery(connectionItem, $"EXEC sp_rename '{oldTableName}', '{newTableName}';") > -1;
        }

        private string T2CreateSql<T>(string tableName, string[] additionals = null)
        {
            ValueStringBuilder sb = new ValueStringBuilder(1024);
            sb.Append($"CREATE TABLE {tableName}(");
            var nea = EntityCache.TryGetInfo<T>();
            bool isFirst = true;
            foreach (var item in nea.PropertyInfos)
            {
                if (isFirst)
                {
                    isFirst = false;
                    sb.Append($"{Sl}{item.Key}{Sr} ");
                }
                else
                {
                    sb.Append($",{Sl}{item.Key}{Sr} ");
                }
                if (!string.IsNullOrEmpty(item.Value.DBType))
                {
                    if (item.Value.DBType == "JSONSTR")
                    {
                        sb.Append("varchar(max)");
                    }
                    else
                    {
                        sb.Append(item.Value.DBType);
                        if (!string.IsNullOrEmpty(item.Value.DBLength))
                        {
                            sb.Append($"({item.Value.DBLength})");
                        }
                    }
                }
                else
                {

                    if (item.Value.PropertyType == typeof(string))
                    {
                        sb.Append($"varchar({item.Value.DBLength ?? "255"})");
                    }
                    else if (item.Value.PropertyType == typeof(DateTime) || item.Value.PropertyType == typeof(DateTime?))
                    {
                        sb.Append("datetime");
                    }
                    else if (item.Value.PropertyType == typeof(decimal) || item.Value.PropertyType == typeof(decimal?))
                    {
                        sb.Append($"decimal({item.Value.DBLength ?? "15, 3"})");
                    }
                    else if (item.Value.PropertyType == typeof(int) || item.Value.PropertyType == typeof(int?))
                    {
                        sb.Append($"int");
                    }
                    else if (item.Value.PropertyType == typeof(long) || item.Value.PropertyType == typeof(long?))
                    {
                        sb.Append($"bigint");
                    }
                    else
                    {
                        sb.Append($"varchar({item.Value.DBLength ?? "255"})");
                    }
                }
                if (nea.AutoIncrements.Contains(item.Key))
                {
                    sb.Append($" identity(1,1)");
                }
                if (nea.PrimaryKeys.Contains(item.Key))
                {
                    sb.Append($" primary key NOT NULL");
                }
            }
            sb.Append(");");
            var indexs = EntityCache.Index<T>();
            if (indexs.Count > 0)
            {
                foreach (var item in indexs)
                {
                    string indexFileds = string.Join($"{Sr},{Sl}", item.IndexFields.Split(','));
                    sb.Append($"CREATE NONCLUSTERED INDEX {item.IndexName} ON {tableName} ({Sl}{indexFileds}{Sr});");
                }
            }
            if (additionals != null)
            {
                foreach (var item in additionals)
                {
                    sb.Append($" {item}");
                }
            }
            return sb.ToString();
        }


        private  DataTable ConvertToDataTable<T>(IEnumerable<T> data)
        {
            var minDate = SqlDateTime.MinValue.Value;// DateTime.Parse("1753-01-01 12:00:00");
            PropertyDescriptorCollection properties = TypeDescriptor.GetProperties(typeof(T));
            DataTable table = new DataTable();
            foreach (PropertyDescriptor prop in properties)
                table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
            foreach (T item in data)
            {
                DataRow row = table.NewRow();
                foreach (PropertyDescriptor prop in properties)
                {
                    if (prop.PropertyType == typeof(DateTime))
                    {

                        var value = (DateTime?)prop.GetValue(item);
                        if (value == null || value <= minDate)
                        {
                            value = minDate;
                        }
                        row[prop.Name] = value;
                    }
                    else
                    {
                        row[prop.Name] = prop.GetValue(item) ?? DBNull.Value;
                    }
                }
                table.Rows.Add(row);
            }
            return table;
        }
    }
}
