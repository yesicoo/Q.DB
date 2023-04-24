/*************************************************************************************
 *
 * 文 件 名:   ClickHouseEngine.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/8 17:54
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
using System.Data.Common;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Q.DB.Interface;
using Q.DB.Models;

namespace Q.DB.ClickHouse
{
    //https://github.com/DarkWanderer/ClickHouse.Client/wiki/Quick-start
    public class ClickHouseEngine : IDBEngine
    {
        DBConnPool ConnPool;
        public string TypeName => "ClickHouse";

        public string Sl => "`";

        public string Sr => "`";

        public string ConnStr { set; get; }

        public string LastIdentitySql => "";
        public string DefaultDBName { set; get; }


        public (string, string) EscapeParamKey(string key = null, string typeName = "String")
        {
            if (key == null)
            {
                key = QDBTools.RandomCode(6, 0, true);
            }
            switch (typeName)
            {
                case "String":
                case "string":
                    return (key, $"{{{key}:String}}");
                case "Int":
                case "Int[]":
                case "Int32":
                case "Int32[]":
                case "int":
                case "int32":
                case "int[]":
                case "int32[]":
                    return (key, $"{{{key}:Int32}}");
                case "Long":
                case "Int64[]":
                case "Int64":
                case "long":
                case "int64":
                case "int64[]":
                    return (key, $"{{{key}:Int64}}");
                case "DateTime":
                    return (key, $"{{{key}:DateTime}}");
                case "Decimal":
                    return (key, $"{{{key}:Decimal64}}");
                default:
                    return (key, $"{{{key}:String}}");
            }

        }


        public ClickHouseEngine(string connstr)
        {
            if (string.IsNullOrEmpty(connstr))
            {
                throw new Exception("链接无效");
            }
            else
            {
                this.DefaultDBName = connstr.Split(';').FirstOrDefault(x => x.ToLower().Contains("database"))?.Split('=')?[1]?.Trim();
                SqlLogUtil.SendLog(LogType.Msg, "Default DBName:" + this.DefaultDBName);
                this.ConnStr = connstr;
                ConnPool = new DBConnPool(this);
            }
        }

        public bool CheckConn(DbConnection conn)
        {
            return ((ClickHouseConnection)conn).State == System.Data.ConnectionState.Open;
        }

        public DbConnection CreateConn()
        {
            return new ClickHouseConnection(ConnStr);
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
            var nea = EntityCache.TryGetInfo<T>();
            var tableName = nea.TableName + QDBTools.ConvertSuffixTableName(tableSuffix);
            using var bulkCopyInterface = new ClickHouseBulkCopy((ClickHouseConnection)connectionItem.SqlConnection)
            {

                DestinationTableName = $"{connectionItem.SqlConnection.Database}.{tableName}",
                BatchSize = splitCount,
            };
            List<object[]> datas = new List<object[]>(ts.Count);
            foreach (T t in ts)
            {
                List<object> list = new List<object>();
                foreach (var item in nea.PropertyInfos)
                {
                    list.Add(item.Value.GetValue(t));
                }
                datas.Add(list.ToArray());
            }
            bulkCopyInterface.WriteToServerAsync(datas, nea.PropertyInfos.Keys.ToList().AsReadOnly()).Wait();
            return bulkCopyInterface.RowsWritten;
        }

        public async Task<long> BulkInsertAsync<T>(DBConnectionItem connectionItem, List<T> ts, string tableSuffix, int splitCount)
        {
            var nea = EntityCache.TryGetInfo<T>();
            var tableName = nea.TableName + QDBTools.ConvertSuffixTableName(tableSuffix);
            using var bulkCopyInterface = new ClickHouseBulkCopy((ClickHouseConnection)connectionItem.SqlConnection)
            {

                DestinationTableName = $"{connectionItem.SqlConnection.Database}.{tableName}",
                BatchSize = splitCount,
            };

            List<object[]> datas = new List<object[]>(ts.Count);
            foreach (T t in ts)
            {
                List<object> list = new List<object>();
                foreach (var item in nea.PropertyInfos)
                {
                    list.Add(item.Value.GetValue(t));
                }
                datas.Add(list.ToArray());
            }

            await bulkCopyInterface.WriteToServerAsync(datas, nea.PropertyInfos.Keys.ToList().AsReadOnly());
            return bulkCopyInterface.RowsWritten;
        }

        public void BulkInsertFile(DBConnectionItem connectionItem, string sql, string filePath, bool isCompressed = false)
        {
            long ts = System.Environment.TickCount64;

            try
            {

                using (StreamReader sr = new StreamReader(filePath))
                {
                    ((ClickHouseConnection)connectionItem.SqlConnection).PostStreamAsync(sql, sr.BaseStream, isCompressed, default).Wait();
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.SQL, sql + "\r\n" + filePath, tick);
                }
            }
            catch (Exception ex)
            {
                long tick = System.Environment.TickCount64 - ts;
                SqlLogUtil.SendLog(LogType.SQL, sql + "\r\n" + filePath + "\r\n" + ex.ToString(), tick);
                throw;
            }


        }
        public async Task BulkInsertFileAsync(DBConnectionItem connectionItem, string sql, string filePath, bool isCompressed = false)
        {
            long ts = System.Environment.TickCount64;
            try
            {
                using (StreamReader sr = new StreamReader(filePath))
                {
                    await ((ClickHouseConnection)connectionItem.SqlConnection).PostStreamAsync(sql, sr.BaseStream, isCompressed, default);
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.SQL, sql + "\r\n" + filePath, tick);
                }

            }
            catch (Exception ex)
            {
                long tick = System.Environment.TickCount64 - ts;
                SqlLogUtil.SendLog(LogType.SQL, sql + "\r\n" + filePath + "\r\n" + ex.ToString(), tick);
                throw;
            }
        }


        public async Task ExportToCsvFileAsync(DBConnectionItem connectionItem, string commandText, string filePath, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            long ts = System.Environment.TickCount64;
            try
            {
                using var command = ((ClickHouseConnection)connectionItem.SqlConnection).CreateCommand();
                command.CommandText = commandText + " FORMAT CSVWithNames";
                if (dbParameters != null)
                {
                    foreach (var item in dbParameters)
                    {
                        var parameter = command.CreateParameter();
                        parameter.DbType = item.DbType;
                        parameter.ParameterName = item.Name;
                        parameter.Value = item.Value;
                        command.Parameters.Add(parameter);
                    }
                }
                using var result = await command.ExecuteRawResultAsync(CancellationToken.None);
                string tempFilePath = filePath + ".tmp";
                using (FileStream fileStream = File.Create(tempFilePath))
                {
                    await result.CopyToAsync(fileStream);
                }
                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                }
                File.Move(tempFilePath, filePath);
                long tick = System.Environment.TickCount64 - ts;
                FileInfo fi = new FileInfo(filePath);
                SqlLogUtil.SendLog(LogType.Msg, $"{fi.Name}下载完成 大小:{Math.Round(fi.Length / 1000m, 2)} kb 耗时:{tick} ms 速度:{fi.Length / tick} kb/s ");
            }
            catch (Exception ex)
            {
                long tick = System.Environment.TickCount64 - ts;
                SqlLogUtil.SendLog(LogType.SQL, commandText + ex.ToString(), tick);
                throw;
            }
        }



        public async Task<Stream> QueryStreamAsync(DBConnectionItem connectionItem, string commandText, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            long ts = System.Environment.TickCount64;
            try
            {
                using var command = ((ClickHouseConnection)connectionItem.SqlConnection).CreateCommand();
                command.CommandType = IsProcedure ? System.Data.CommandType.StoredProcedure : System.Data.CommandType.Text;
                command.CommandText = commandText;
                if (dbParameters != null)
                {
                    foreach (var item in dbParameters)
                    {
                        var parameter = command.CreateParameter();
                        parameter.DbType = item.DbType;
                        parameter.ParameterName = item.Name;
                        parameter.Value = item.Value;
                        command.Parameters.Add(parameter);
                    }
                }
                using var result = await command.ExecuteRawResultAsync(CancellationToken.None);
                MemoryStream stream = new MemoryStream();
                await result.CopyToAsync(stream);
                long tick = System.Environment.TickCount64 - ts;
                SqlLogUtil.SendLog(LogType.SQL, commandText, tick);
                return stream;
            }
            catch (Exception ex)
            {
                long tick = System.Environment.TickCount64 - ts;
                SqlLogUtil.SendLog(LogType.SQL, commandText + ex.ToString(), tick);
                throw;
            }
        }
        public Stream QueryStream(DBConnectionItem connectionItem, string commandText, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            long ts = System.Environment.TickCount64;
            try
            {
                using var command = ((ClickHouseConnection)connectionItem.SqlConnection).CreateCommand();
                command.CommandType = IsProcedure ? System.Data.CommandType.StoredProcedure : System.Data.CommandType.Text;
                command.CommandText = commandText;
                if (dbParameters != null)
                {
                    foreach (var item in dbParameters)
                    {
                        var parameter = command.CreateParameter();
                        parameter.DbType = item.DbType;
                        parameter.ParameterName = item.Name;
                        parameter.Value = item.Value;
                        command.Parameters.Add(parameter);
                    }
                }
                using var result = command.ExecuteRawResultAsync(CancellationToken.None).Result;
                MemoryStream stream = new MemoryStream();
                result.CopyToAsync(stream).Wait();
                long tick = System.Environment.TickCount64 - ts;
                SqlLogUtil.SendLog(LogType.SQL, commandText, tick);
                return stream;
            }
            catch (Exception ex)
            {
                long tick = System.Environment.TickCount64 - ts;
                SqlLogUtil.SendLog(LogType.SQL, commandText + ex.ToString(), tick);
                throw;
            }
        }


        public bool CheckTableExisted(DBConnectionItem connectionItem, string tableName)
        {
            string sql = $"SELECT count(*) FROM system.tables WHERE (name = '{tableName}') AND (database = '{connectionItem.SqlConnection.Database}');";
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
            var createSql = T2CreateSql<T>("IF NOT EXISTS " + tableName, tableEngine, additionals);
            return ((IDBEngine)this).ExecuteNonQuery(connectionItem, createSql, null) > 0;
        }

        private string T2CreateSql<T>(string tableName, string tableEngine = "Default", string[] additionals = null)
        {
            ValueStringBuilder sb = new ValueStringBuilder(1024);
            sb.Append($"CREATE TABLE {tableName}(\r\n");
            var nea = EntityCache.TryGetInfo<T>();
            bool isFirst = true;
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
                        sqlType = $"String";
                    }
                    else if (item.Value.PropertyType == typeof(DateTime) || item.Value.PropertyType == typeof(DateTime?))
                    {
                        sqlType = "DateTime";
                    }
                    else if (item.Value.PropertyType == typeof(decimal) || item.Value.PropertyType == typeof(decimal?))
                    {
                        sqlType = $"Decimal64({item.Value.DBLength ?? "3"})";
                    }
                    else if (item.Value.PropertyType == typeof(int) || item.Value.PropertyType == typeof(int?))
                    {
                        sqlType = $"Int32";
                    }
                    else if (item.Value.PropertyType == typeof(long) || item.Value.PropertyType == typeof(long?))
                    {
                        sqlType = "Int64";
                    }
                    else
                    {
                        sqlType = $"String";
                    }
                }
                if (isFirst)
                {
                    isFirst = false;
                    sb.Append($"{Sl}{item.Key}{Sr} {(item.Value.Nullable ? $" Nullable({sqlType})" : sqlType)}");
                }
                else
                {
                    sb.Append($",\r\n{Sl}{item.Key}{Sr} {(item.Value.Nullable ? $" Nullable({sqlType})" : sqlType)}");

                }
                if (!string.IsNullOrEmpty(item.Value.Remark))
                {
                    sb.Append($" COMMENT '{item.Value.Remark}'");
                }
            }



            var indexs = EntityCache.Index<T>();
            if (indexs.Count > 0)
            {
                foreach (var item in indexs)
                {
                    string indexFileds = string.Join($"{Sr},{Sl}", item.IndexFields.Split(','));
                    sb.Append($",\r\nINDEX {item.IndexName}({Sl}{string.Join($"{Sr},{Sl}", indexFileds)}{Sr}) Type {(string.IsNullOrEmpty(item.IndexType) ? "bloom_filter" : item.IndexType)}");
                }
            }


            sb.Append("\r\n)");
            if (tableEngine != "Default")
            {
                sb.Append($"ENGINE={tableEngine}\r\n");
            }
            else
            {
                sb.Append("ENGINE=MergeTree()\r\n");
            }

            if (nea.PrimaryKeys.Count > 0)
            {
                sb.Append($"ORDER BY ({Sl}{string.Join($"{Sr},{Sl}", nea.PrimaryKeys)}{Sr})\r\n");
            }
            if (additionals != null)
            {
                foreach (var item in additionals)
                {
                    sb.Append($"{item})\r\n");
                }
            }

            sb.Append(";");
            var sql = sb.ToString();

            return sql;
        }

        public string CreateDelSql(string dbTable, string whereStr, bool isSync = false)
        {
            return $"set mutations_sync={(isSync ? 2 : 0)}; ALTER TABLE {dbTable} DELETE where {whereStr};";
        }

        public string CreateUpdateSql(string dbTable, string setStr, string whereStr, bool isSync = false)
        {
            return $"set mutations_sync={(isSync ? 2 : 0)}; ALTER TABLE {dbTable} UPDATE {setStr} where {whereStr};";
        }
    }
}
