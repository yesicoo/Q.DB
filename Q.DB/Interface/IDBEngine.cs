/*************************************************************************************
 *
 * 文 件 名:   IDBEngine.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/11/25 14:50
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
using System.Dynamic;
using System.Linq;
using System.Net.Http.Json;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Unicode;
using System.Threading.Tasks;
using System.Xml.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Q.DB.Models;

namespace Q.DB.Interface
{
    public interface IDBEngine
    {
        string TypeName { get; }
        string Sl { get; }
        string Sr { get; }
        string ConnStr { get; }
        string DefaultDBName { get; }
        string LastIdentitySql { get; }

        public string EscapeStr(params string[] strs) => $"{Sl}{string.Join($"{Sr}.{Sl}", strs)}{Sr}";
        (string, string) EscapeParamKey(string key = null, string typeName = "String")
        {
            if (key == null)
            {
                key = QDBTools.RandomCode(6, 0, true);
            }
            return (key, "@" + key);
        }
        DateTime? ConvertToDBMinTime(DateTime? dateTime)
        {
            return dateTime;
        }
        ParamSql CreateParamItemByPropertiy(PropertyInfo propertiy, object value)
        {

            ParamItem item = null;
            (string key, string skey) = EscapeParamKey(typeName: propertiy.PropertyType.Name);
            string SqlStr = $"{propertiy.Name} = {skey}";
            switch (propertiy.PropertyType.Name)
            {

                case "String":
                    {
                        item = new ParamItem(key, value, System.Data.DbType.String);
                    }
                    break;
                case "Int32":
                    {
                        item = new ParamItem(key, value, System.Data.DbType.Int32);

                    }
                    break;
                case "Decimal":
                    {
                        item = new ParamItem(key, value, System.Data.DbType.Decimal);

                    }
                    break;
                case "DateTime":
                    {
                        item = new ParamItem(key, value, System.Data.DbType.DateTime);
                    }
                    break;
                case "Int64":
                    {
                        item = new ParamItem(key, value, System.Data.DbType.Int64);
                    }
                    break;
                default:
                    {
                        item = new ParamItem(key, value, System.Data.DbType.String);
                    }
                    break;
            }
            return new ParamSql(SqlStr, item);
        }

        bool CheckConn(DbConnection conn);
        DbConnection CreateConn();
        DBConnectionItem GetConnection(string dbName = null);

        #region 检查表以及创建表

        /// <summary>
        /// 检查表是否存在
        /// </summary>
        /// <param name="dbname"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        bool CheckTableExisted(string dbname, string tableName)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.CheckTableExisted(conn, tableName);
            }
            finally
            {
                conn.GiveBack();
            }

        }
        bool CheckTableExisted(DBConnectionItem connectionItem, string tableName);
        /// <summary>
        /// 创建表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dbname"></param>
        /// <param name="tableName"></param>
        /// <param name="force">强制新建（先执行删除表操作）</param>
        /// <returns></returns>
        bool CreateTable<T>(string dbname, string tableName, bool force = false, string tableEngine = "Default", string[] additionals = null)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.CreateTable<T>(conn, tableName, force, tableEngine, additionals);
            }
            finally
            {
                conn.GiveBack();
            }

        }
        bool CreateTable<T>(DBConnectionItem connectionItem, string tableName, bool force = false, string tableEngine = "Default", string[] additionals = null);
        /// <summary>
        /// 表不存在就新建
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dbname"></param>
        /// <param name="tableName"></param>
        /// <param name="Create"></param>
        /// <returns></returns>

        bool CreateTableIfNotExists<T>(string dbname, string tableName, string tableEngine = "Default", string[] additionals = null)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.CreateTableIfNotExists<T>(conn, tableName, tableEngine, additionals);
            }
            finally
            {
                conn.GiveBack();
            }

        }
        bool CreateTableIfNotExists<T>(DBConnectionItem connectionItem, string tableName, string tableEngine = "Default", string[] additionals = null);

        bool RenameTable(string dbname, string oldTableName, string newTableName)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.RenameTable(conn, oldTableName, newTableName);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        bool RenameTable(DBConnectionItem connectionItem, string oldTableName, string newTableName)
        {
            return this.ExecuteNonQuery(connectionItem, $"RENAME TABLE {oldTableName} TO {newTableName};") > -1;
        }
        #endregion


        #region 增删改查

        #region 基本操作

        int ExecuteNonQuery(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false, bool lastIdentity = false)
        {
            var conn = this.GetConnection();
            try
            {
                return this.ExecuteNonQuery(conn, text, dbParameters, IsProcedure, lastIdentity);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        int ExecuteNonQuery(DBConnectionItem connectionItem, string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false, bool lastIdentity = false)
        {
            long ts = System.Environment.TickCount64;
            int result = 0;
            using (var command = connectionItem.SqlConnection.CreateCommand())
            {
                command.CommandText = text;
                command.CommandType = IsProcedure ? System.Data.CommandType.StoredProcedure : System.Data.CommandType.Text;
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

                try
                {
                    result = command.ExecuteNonQuery();
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.SQL, text, tick);
                }
                catch (Exception ex)
                {
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.Error, text + "\r\n ErrorMsg:\r\n" + ex.Message, tick);
                    throw;
                }
            }

            if (!lastIdentity || string.IsNullOrEmpty(LastIdentitySql))
            {
                return result;
            }
            else
            {
                return ExecuteScalar<int>(connectionItem, LastIdentitySql, null);
            }

        }

        async Task<int> ExecuteNonQueryAsync(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false, bool lastIdentity = false)
        {
            var conn = this.GetConnection();
            try
            {
                return await this.ExecuteNonQueryAsync(conn, text, dbParameters, IsProcedure, lastIdentity);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        async Task<int> ExecuteNonQueryAsync(DBConnectionItem connectionItem, string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false, bool lastIdentity = false)
        {
            long ts = System.Environment.TickCount64;
            int result = 0;
            using (var command = connectionItem.SqlConnection.CreateCommand())
            {
                command.CommandText = text;
                command.CommandType = IsProcedure ? System.Data.CommandType.StoredProcedure : System.Data.CommandType.Text;
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
                try
                {
                    result = await command.ExecuteNonQueryAsync();
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.SQL, text, tick);
                }
                catch (Exception ex)
                {
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.Error, text + "\r\n ErrorMsg:\r\n" + ex.Message, tick);
                    throw;
                }
            }

            if (!lastIdentity || string.IsNullOrEmpty(LastIdentitySql))
            {
                return result;
            }
            else
            {
                return await ExecuteScalarAsync<int>(connectionItem, LastIdentitySql, null);
            }
        }

        long ExecuteNonQuery64(DBConnectionItem connectionItem, string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false, bool lastIdentity = false)
        {
            long ts = System.Environment.TickCount64;
            long result = 0;
            using (var command = connectionItem.SqlConnection.CreateCommand())
            {
                command.CommandText = text;
                command.CommandType = IsProcedure ? System.Data.CommandType.StoredProcedure : System.Data.CommandType.Text;
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

                try
                {
                    result = command.ExecuteNonQuery();
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.SQL, text, tick);
                }
                catch (Exception ex)
                {
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.Error, text + "\r\n ErrorMsg:\r\n" + ex.Message, tick);
                    throw;
                }
            }

            if (!lastIdentity || string.IsNullOrEmpty(LastIdentitySql))
            {
                return result;
            }
            else
            {
                return ExecuteScalar<long>(connectionItem, LastIdentitySql, null);
            }

        }

        long ExecuteNonQuery64(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false, bool lastIdentity = false)
        {
            var conn = this.GetConnection();
            try
            {
                return this.ExecuteNonQuery64(conn, text, dbParameters, IsProcedure, lastIdentity);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        async Task<long> ExecuteNonQuery64Async(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false, bool lastIdentity = false)
        {
            var conn = this.GetConnection();
            try
            {
                return await this.ExecuteNonQuery64Async(conn, text, dbParameters, IsProcedure, lastIdentity);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        async Task<long> ExecuteNonQuery64Async(DBConnectionItem connectionItem, string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false, bool lastIdentity = false)
        {
            long ts = System.Environment.TickCount64;
            long result = 0;
            using (var command = connectionItem.SqlConnection.CreateCommand())
            {
                command.CommandText = text;
                command.CommandType = IsProcedure ? System.Data.CommandType.StoredProcedure : System.Data.CommandType.Text;
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
                try
                {
                    result = await command.ExecuteNonQueryAsync();
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.SQL, text, tick);
                }
                catch (Exception ex)
                {
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.Error, text + "\r\n ErrorMsg:\r\n" + ex.Message, tick);
                    throw;
                }
            }

            if (!lastIdentity || string.IsNullOrEmpty(LastIdentitySql))
            {
                return result;
            }
            else
            {
                return await ExecuteScalarAsync<long>(connectionItem, LastIdentitySql, null);
            }
        }

        T ExecuteScalar<T>(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            var conn = this.GetConnection();
            try
            {
                return this.ExecuteScalar<T>(conn, text, dbParameters, IsProcedure);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        T ExecuteScalar<T>(DBConnectionItem connectionItem, string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            using (var command = connectionItem.SqlConnection.CreateCommand())
            {
                command.CommandText = text;

                command.CommandType = IsProcedure ? System.Data.CommandType.StoredProcedure : System.Data.CommandType.Text;
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
                try
                {
                    var res = command.ExecuteScalar();
                    if (res != null)
                    {
                        return (T)Convert.ChangeType(res, typeof(T));
                    }
                    else
                    {
                        return default;
                    }
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }

        async Task<T> ExecuteScalarAsync<T>(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            var conn = this.GetConnection();
            try
            {
                return await this.ExecuteScalarAsync<T>(conn, text, dbParameters, IsProcedure);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        async Task<T> ExecuteScalarAsync<T>(DBConnectionItem connectionItem, string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            long ts = System.Environment.TickCount64;

            using (var command = connectionItem.SqlConnection.CreateCommand())
            {
                command.CommandText = text;

                command.CommandType = IsProcedure ? System.Data.CommandType.StoredProcedure : System.Data.CommandType.Text;
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
                try
                {
                    var res = await command.ExecuteScalarAsync();
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.SQL, text, tick);
                    if (res != null)
                    {
                        return (T)Convert.ChangeType(res, typeof(T));
                    }
                    else
                    {
                        return default;
                    }
                }
                catch (Exception ex)
                {
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.Error, text + "\r\n ErrorMsg:\r\n" + ex.Message, tick);
                    throw;
                }
            }
        }
        T ExecuteQueryFirst<T>(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            var conn = this.GetConnection();
            try
            {
                return this.ExecuteQueryFirst<T>(conn, text, dbParameters, IsProcedure);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        T ExecuteQueryFirst<T>(DBConnectionItem connectionItem, string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            long ts = System.Environment.TickCount64;
            T result = default;
            using (var command = connectionItem.SqlConnection.CreateCommand())
            {
                command.CommandText = text;
                command.CommandType = IsProcedure ? System.Data.CommandType.StoredProcedure : System.Data.CommandType.Text;
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
                try
                {
                    using (var read = command.ExecuteReader())
                    {
                        while (read.Read())
                        {

                            result = IDBEngine.Dr2T<T>(read);
                            break;
                        }
                    }
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.SQL, text, tick);
                }
                catch (Exception ex)
                {
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.Error, text + "\r\n ErrorMsg:\r\n" + ex.Message, tick);
                    throw;
                }
                return result;
            }
        }

        async Task<T> ExecuteQueryFirstAsync<T>(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            var conn = this.GetConnection();
            try
            {
                return await this.ExecuteQueryFirstAsync<T>(conn, text, dbParameters, IsProcedure);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        async Task<T> ExecuteQueryFirstAsync<T>(DBConnectionItem connectionItem, string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            long ts = System.Environment.TickCount64;
            T result = default;
            using (var command = connectionItem.SqlConnection.CreateCommand())
            {
                command.CommandText = text;

                command.CommandType = IsProcedure ? System.Data.CommandType.StoredProcedure : System.Data.CommandType.Text;
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
                try
                {
                    using (var read = await command.ExecuteReaderAsync())
                    {
                        while (await read.ReadAsync())
                        {
                            result = IDBEngine.Dr2T<T>(read);
                            break;
                        }
                    }
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.SQL, text, tick);
                }
                catch (Exception ex)
                {
                    long tick = System.Environment.TickCount64 - ts;
                    SqlLogUtil.SendLog(LogType.Error, text + "\r\n ErrorMsg:\r\n" + ex.Message, tick);
                    throw;
                }
                return result;
            }
        }
        IEnumerable<T> ExecuteQuery<T>(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            var conn = this.GetConnection();
            try
            {

                return this.ExecuteQuery<T>(conn, text, dbParameters, IsProcedure);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        IEnumerable<T> ExecuteQuery<T>(DBConnectionItem connectionItem, string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {

            long ts = System.Environment.TickCount64;
            using (var command = connectionItem.SqlConnection.CreateCommand())
            {


                command.CommandText = text;
                command.CommandType = IsProcedure ? System.Data.CommandType.StoredProcedure : System.Data.CommandType.Text;
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
                using (var read = command.ExecuteReader())
                {
                    while (read.Read())
                    {

                        yield return IDBEngine.Dr2T<T>(read);
                    }
                }
                SqlLogUtil.SendLog(LogType.SQL, text, System.Environment.TickCount64 - ts);
            }
            yield break;
        }

        IAsyncEnumerable<T> ExecuteQueryAsync<T>(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            var conn = this.GetConnection();
            try
            {
                return this.ExecuteQueryAsync<T>(conn, text, dbParameters, IsProcedure);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        async IAsyncEnumerable<T> ExecuteQueryAsync<T>(DBConnectionItem connectionItem, string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            long ts = System.Environment.TickCount64;

            using (var command = connectionItem.SqlConnection.CreateCommand())
            {
                command.CommandText = text;
                command.CommandType = IsProcedure ? System.Data.CommandType.StoredProcedure : System.Data.CommandType.Text;
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
                using (var read = await command.ExecuteReaderAsync())
                {
                    while (await read.ReadAsync())
                    {
                        yield return IDBEngine.Dr2T<T>(read);
                    }
                }
                long tick = System.Environment.TickCount64 - ts;
                SqlLogUtil.SendLog(LogType.SQL, text, tick);
            }
            yield break;
        }
        #endregion

        #region 插入 insert

        #region 单条插入
        int Insert(string sql) => this.Insert(dbname: null, sql);
        async Task<int> InsertAsync(string sql) => await this.InsertAsync(dbname: null, sql);

        int Insert(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.Insert(conn, sql: sql, null);
            }
            finally
            {
                conn.GiveBack();
            }

        }

        async Task<int> InsertAsync(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.InsertAsync(conn, sql: sql, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        int Insert(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.Insert(conn, sql, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }

        }

        async Task<int> InsertAsync(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.InsertAsync(conn, sql, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        int Insert(DBConnectionItem connectionItem, string sql) => this.Insert(connectionItem, sql: sql, null);
        async Task<int> InsertAsync(DBConnectionItem connectionItem, string sql) => await this.InsertAsync(connectionItem, sql: sql, null);
        int Insert(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters, bool lastIdentity = true) => this.ExecuteNonQuery(connectionItem, sql, dbParameters, false, lastIdentity);
        async Task<int> InsertAsync(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters, bool lastIdentity = true) => await this.ExecuteNonQueryAsync(connectionItem, sql, dbParameters, false, lastIdentity);
        int Insert<T>(T t, string tableSuffix = null, bool InsAutoIncrement = false)
        {
            var conn = this.GetConnection(null);
            try
            {
                return this.Insert(conn, t, tableSuffix, InsAutoIncrement);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        int Insert<T>(DBConnectionItem connectionItem, T t, string tableSuffix, bool InsAutoIncrement = false)
        {
            ParamSql ps = T2InSql(t, connectionItem.SqlConnection.Database, InsAutoIncrement);
            ps.SqlStr = EntityCache.RestoreTableName<T>(ps.SqlStr, tableSuffix);
            return Insert(connectionItem, ps.SqlStr, ps.Params);
        }

        async Task<int> InsertAsync<T>(T t, string tableSuffix = null, bool InsAutoIncrement = false)
        {
            var conn = this.GetConnection(null);
            try
            {
                return await this.InsertAsync(conn, t, tableSuffix, InsAutoIncrement);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        async Task<int> InsertAsync<T>(DBConnectionItem connectionItem, T t, string tableSuffix, bool InsAutoIncrement = false)
        {
            ParamSql ps = T2InSql(t, connectionItem.SqlConnection.Database, InsAutoIncrement);
            ps.SqlStr = EntityCache.RestoreTableName<T>(ps.SqlStr, tableSuffix);
            return await InsertAsync(connectionItem, ps.SqlStr, ps.Params);
        }



        long Insert64(string sql) => this.Insert64(dbname: null, sql);
        async Task<long> Insert64Async(string sql) => await this.Insert64Async(dbname: null, sql);

        long Insert64(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.Insert64(conn, sql: sql, null);
            }
            finally
            {
                conn.GiveBack();
            }

        }

        async Task<long> Insert64Async(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.Insert64Async(conn, sql: sql, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        long Insert64(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.Insert64(conn, sql, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }

        }

        async Task<long> Insert64Async(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.Insert64Async(conn, sql, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        long Insert64(DBConnectionItem connectionItem, string sql) => this.Insert64(connectionItem, sql: sql, null);
        async Task<long> Insert64Async(DBConnectionItem connectionItem, string sql) => await this.Insert64Async(connectionItem, sql: sql, null);
        long Insert64(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters) => this.ExecuteNonQuery64(connectionItem, sql, dbParameters, false, true);
        async Task<long> Insert64Async(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters) => await this.ExecuteNonQuery64Async(connectionItem, sql, dbParameters, false, true);
        long Insert64<T>(T t, string tableSuffix = null, bool InsAutoIncrement = false)
        {
            var conn = this.GetConnection(null);
            try
            {
                return this.Insert64(conn, t, tableSuffix, InsAutoIncrement);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        long Insert64<T>(DBConnectionItem connectionItem, T t, string tableSuffix, bool InsAutoIncrement = false)
        {
            ParamSql ps = T2InSql(t, connectionItem.SqlConnection.Database, InsAutoIncrement);
            ps.SqlStr = EntityCache.RestoreTableName<T>(ps.SqlStr, tableSuffix);
            return Insert64(connectionItem, ps.SqlStr, ps.Params);
        }

        async Task<long> Insert64Async<T>(T t, string tableSuffix = null, bool InsAutoIncrement = false)
        {
            var conn = this.GetConnection(null);
            try
            {
                return await this.Insert64Async(conn, t, tableSuffix, InsAutoIncrement);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        async Task<long> Insert64Async<T>(DBConnectionItem connectionItem, T t, string tableSuffix, bool InsAutoIncrement = false)
        {
            ParamSql ps = T2InSql(t, connectionItem.SqlConnection.Database, InsAutoIncrement);
            ps.SqlStr = EntityCache.RestoreTableName<T>(ps.SqlStr, tableSuffix);
            return await Insert64Async(connectionItem, ps.SqlStr, ps.Params);
        }


        #endregion

        #region 小批量插入(拼SQL的方式减少请求，支持事务)
        int BatchInsert<T>(DBConnectionItem connectionItem, List<T> ts, string tableSuffix, int splitCount = 300, bool InsAutoIncrement = false)
        {
            int result = 0;
            long st = System.Environment.TickCount;
            splitCount = splitCount == 0 ? DBFactory.Instance.SplitCount : splitCount;
            if (ts.Count > splitCount)
            {
                int batchCount = ts.Count / splitCount + 1;
                for (int i = 0; i < batchCount; i++)
                {
                    var tempdatas = ts.Skip(i * splitCount).Take(splitCount).ToList();
                    ParamSql ps = Ts2InSql(tempdatas, connectionItem.SqlConnection.Database, InsAutoIncrement);
                    ps.SqlStr=EntityCache.RestoreTableName<T>(ps.SqlStr, tableSuffix);
                    result += Insert(connectionItem, ps.SqlStr, ps.Params, false);

                }
            }
            else
            {
                ParamSql ps = Ts2InSql(ts, connectionItem.SqlConnection.Database,InsAutoIncrement);
                ps.SqlStr = EntityCache.RestoreTableName<T>(ps.SqlStr, tableSuffix);
                result += Insert(connectionItem, ps.SqlStr, ps.Params, false);
            }
            SqlLogUtil.SendLog(LogType.SQL, "BatchInsert :" + ts.Count, System.Environment.TickCount - st);
            return result;
        }
        async Task<int> BatchInsertAsync<T>(DBConnectionItem connectionItem, List<T> ts, string tableSuffix, int splitCount = 300, bool InsAutoIncrement = false)
        {
            int result = 0;
            long st = System.Environment.TickCount;
            splitCount = splitCount == 0 ? DBFactory.Instance.SplitCount : splitCount;
            if (ts.Count > splitCount)
            {
                int batchCount = ts.Count / splitCount + 1;
                for (int i = 0; i < batchCount; i++)
                {
                    var tempdatas = ts.Skip(i * splitCount).Take(splitCount).ToList();
                    ParamSql ps = Ts2InSql(tempdatas, connectionItem.SqlConnection.Database, InsAutoIncrement);
                    ps.SqlStr = EntityCache.RestoreTableName<T>(ps.SqlStr, tableSuffix);
                    result += await InsertAsync(connectionItem, ps.SqlStr, ps.Params);
                }
            }
            else
            {
                ParamSql ps = Ts2InSql(ts, connectionItem.SqlConnection.Database, InsAutoIncrement);
                ps.SqlStr = EntityCache.RestoreTableName<T>(ps.SqlStr, tableSuffix);
                result += await InsertAsync(connectionItem, ps.SqlStr, ps.Params);
            }
            SqlLogUtil.SendLog(LogType.SQL, "BatchInsert :" + ts.Count, System.Environment.TickCount - st);
            return result;
        }

        #endregion

        #region 大批量导入(各个数据库的文件导入方式，不支持事务)
        long BulkInsert<T>(DBConnectionItem connectionItem, List<T> ts, string tableSuffix, int splitCount);
        Task<long> BulkInsertAsync<T>(DBConnectionItem connectionItem, List<T> ts, string tableSuffix, int splitCount);

        #endregion

        #endregion

        #region 删除 delete
        int Delete(string sql) => this.Delete(dbname: null, sql);
        async Task<int> DeleteAsync(string sql) => await this.DeleteAsync(dbname: null, sql);

        int Delete(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.Delete(conn, sql, null);
            }
            finally
            {
                conn.GiveBack();
            }

        }

        async Task<int> DeleteAsync(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.DeleteAsync(conn, sql, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        int Delete(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.Delete(conn, sql, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }

        }

        async Task<int> DeleteAsync(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.DeleteAsync(conn, sql, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        int Delete(DBConnectionItem connectionItem, string sql) => this.Delete(connectionItem, sql, null);
        async Task<int> DeleteAsync(DBConnectionItem connectionItem, string sql) => await this.DeleteAsync(connectionItem, sql, null);
        int Delete(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters) => this.ExecuteNonQuery(connectionItem, sql, dbParameters);
        async Task<int> DeleteAsync(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters) => await this.ExecuteNonQueryAsync(connectionItem, sql, dbParameters);

        bool DropTable(string dbname, string tableName)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.DropTable(conn, tableName);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        bool DropTable(DBConnectionItem connectionItem, string tableName)
        {
            return this.ExecuteNonQuery(connectionItem, "DROP TABLE " + tableName, null) > -1;
        }

        async Task<bool> DropTableAsync(string dbname, string tableName)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.DropTableAsync(conn, tableName);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        async Task<bool> DropTableAsync(DBConnectionItem connectionItem, string tableName)
        {
            return await this.ExecuteNonQueryAsync(connectionItem, "DROP TABLE " + tableName, null) > -1;
        }
        #endregion

        #region 更改 Update
        int Update(string sql) => this.Update(dbname: null, sql);
        async Task<int> UpdateAsync(string sql) => await this.UpdateAsync(dbname: null, sql);

        int Update(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.Update(conn, sql, null);
            }
            finally
            {
                conn.GiveBack();
            }

        }

        async Task<int> UpdateAsync(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.UpdateAsync(conn, sql, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        int Update(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.Update(conn, sql, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }

        }

        async Task<int> UpdateAsync(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.UpdateAsync(conn, sql, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        int Update(DBConnectionItem connectionItem, string sql) => this.Update(connectionItem, sql, null);
        async Task<int> UpdateAsync(DBConnectionItem connectionItem, string sql) => await this.UpdateAsync(connectionItem, sql, null);
        int Update(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters) => this.ExecuteNonQuery(connectionItem, sql, dbParameters);
        async Task<int> UpdateAsync(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters) => await this.ExecuteNonQueryAsync(connectionItem, sql, dbParameters);
        #endregion

        #region 查询 select

        #region 查单个 只返回首条数据
        T QueryFirst<T>(string sql) => this.QueryFirst<T>(dbname: null, sql);
        T QueryFirst<T>(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryFirst<T>(conn, sql, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        async Task<T> QueryFirstAsync<T>(string sql) => await QueryFirstAsync<T>(dbname: null, sql);
        async Task<T> QueryFirstAsync<T>(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.QueryFirstAsync<T>(conn, sql, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        T QueryFirst<T>(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryFirst<T>(conn, sql, dbParameters);
            }
            finally { conn.GiveBack(); }
        }
        async Task<T> QueryFirstAsync<T>(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.QueryFirstAsync<T>(conn, sql, dbParameters);
            }
            finally { conn.GiveBack(); }
        }
        T QueryFirst<T>(DBConnectionItem connectionItem, string sql) => this.QueryFirst<T>(connectionItem, sql, null);
        async Task<T> QueryFirstAsync<T>(DBConnectionItem connectionItem, string sql) => await this.QueryFirstAsync<T>(connectionItem, sql, null);
        T QueryFirst<T>(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters) => this.ExecuteQueryFirst<T>(connectionItem, sql, dbParameters);
        async Task<T> QueryFirstAsync<T>(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters) => await this.ExecuteQueryFirstAsync<T>(connectionItem, sql, dbParameters);
        #endregion

        #region 查单个 首行首列
        T QueryScalar<T>(string sql) => this.QueryScalar<T>(dbname: null, sql);
        T QueryScalar<T>(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryScalar<T>(conn, sql, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        async Task<T> QueryScalarAsync<T>(string sql) => await QueryScalarAsync<T>(dbname: null, sql);
        async Task<T> QueryScalarAsync<T>(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.QueryScalarAsync<T>(conn, sql, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        T QueryScalar<T>(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryScalar<T>(conn, sql, dbParameters);
            }
            finally { conn.GiveBack(); }
        }
        async Task<T> QueryScalarAsync<T>(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.QueryScalarAsync<T>(conn, sql, dbParameters);
            }
            finally { conn.GiveBack(); }
        }
        T QueryScalar<T>(DBConnectionItem connectionItem, string sql) => this.QueryScalar<T>(connectionItem, sql, null);
        async Task<T> QueryScalarAsync<T>(DBConnectionItem connectionItem, string sql) => await this.QueryScalarAsync<T>(connectionItem, sql, null);
        T QueryScalar<T>(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters) => this.ExecuteScalar<T>(connectionItem, sql, dbParameters);
        async Task<T> QueryScalarAsync<T>(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters) => await this.ExecuteScalarAsync<T>(connectionItem, sql, dbParameters);
        #endregion

        #region 查多个 返回集合
        IEnumerable<T> Query<T>(string sql) => this.Query<T>(dbname: null, sql);
        IEnumerable<T> Query<T>(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.Query<T>(conn, sql, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        IAsyncEnumerable<T> QueryAsync<T>(string sql) => this.QueryAsync<T>(dbname: null, sql);
        IAsyncEnumerable<T> QueryAsync<T>(string dbname, string sql)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryAsync<T>(conn, sql, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        IEnumerable<T> Query<T>(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.Query<T>(conn, sql, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        IAsyncEnumerable<T> QueryAsync<T>(string dbname, string sql, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryAsync<T>(conn, sql, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        IEnumerable<T> Query<T>(DBConnectionItem connectionItem, string sql) => this.Query<T>(connectionItem, sql, null);
        IAsyncEnumerable<T> QueryAsync<T>(DBConnectionItem connectionItem, string sql) => this.QueryAsync<T>(connectionItem, sql, null);
        IEnumerable<T> Query<T>(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters) => this.ExecuteQuery<T>(connectionItem, sql, dbParameters);
        IAsyncEnumerable<T> QueryAsync<T>(DBConnectionItem connectionItem, string sql, IEnumerable<ParamItem> dbParameters) => this.ExecuteQueryAsync<T>(connectionItem, sql, dbParameters);
        #endregion


        #region 存储过程查单个 只返回首条数据
        T QueryFirstByProcedure<T>(string procedureName) => this.QueryFirstByProcedure<T>(dbname: null, procedureName);
        T QueryFirstByProcedure<T>(string dbname, string procedureName)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryFirstByProcedure<T>(conn, procedureName, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        async Task<T> QueryFirstByProcedureAsync<T>(string procedureName) => await QueryFirstByProcedureAsync<T>(dbname: null, procedureName);
        async Task<T> QueryFirstByProcedureAsync<T>(string dbname, string procedureName)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.QueryFirstByProcedureAsync<T>(conn, procedureName, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        T QueryFirstByProcedure<T>(string dbname, string procedureName, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryFirstByProcedure<T>(conn, procedureName, dbParameters);
            }
            finally { conn.GiveBack(); }
        }
        async Task<T> QueryFirstByProcedureAsync<T>(string dbname, string procedureName, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.QueryFirstByProcedureAsync<T>(conn, procedureName, dbParameters);
            }
            finally { conn.GiveBack(); }
        }
        T QueryFirstByProcedure<T>(DBConnectionItem connectionItem, string procedureName) => this.QueryFirstByProcedure<T>(connectionItem, procedureName, null);
        async Task<T> QueryFirstByProcedureAsync<T>(DBConnectionItem connectionItem, string procedureName) => await this.QueryFirstByProcedureAsync<T>(connectionItem, procedureName, null);
        T QueryFirstByProcedure<T>(DBConnectionItem connectionItem, string procedureName, IEnumerable<ParamItem> dbParameters) => this.ExecuteQueryFirst<T>(connectionItem, procedureName, dbParameters, true);
        async Task<T> QueryFirstByProcedureAsync<T>(DBConnectionItem connectionItem, string procedureName, IEnumerable<ParamItem> dbParameters) => await this.ExecuteQueryFirstAsync<T>(connectionItem, procedureName, dbParameters, true);
        #endregion

        #region 存储过程查单个 首行首列
        T QueryScalarByProcedure<T>(string procedureName) => this.QueryScalarByProcedure<T>(dbname: null, procedureName);
        T QueryScalarByProcedure<T>(string dbname, string procedureName)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryScalarByProcedure<T>(conn, procedureName, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        async Task<T> QueryScalarByProcedureAsync<T>(string procedureName) => await QueryScalarByProcedureAsync<T>(dbname: null, procedureName);
        async Task<T> QueryScalarByProcedureAsync<T>(string dbname, string procedureName)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.QueryScalarByProcedureAsync<T>(conn, procedureName, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        T QueryScalarByProcedure<T>(string dbname, string procedureName, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryScalarByProcedure<T>(conn, procedureName, dbParameters);
            }
            finally { conn.GiveBack(); }
        }
        async Task<T> QueryScalarByProcedureAsync<T>(string dbname, string procedureName, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.QueryScalarByProcedureAsync<T>(conn, procedureName, dbParameters);
            }
            finally { conn.GiveBack(); }
        }
        T QueryScalarByProcedure<T>(DBConnectionItem connectionItem, string procedureName) => this.QueryScalarByProcedure<T>(connectionItem, procedureName, null);
        async Task<T> QueryScalarByProcedureAsync<T>(DBConnectionItem connectionItem, string procedureName) => await this.QueryScalarByProcedureAsync<T>(connectionItem, procedureName, null);
        T QueryScalarByProcedure<T>(DBConnectionItem connectionItem, string procedureName, IEnumerable<ParamItem> dbParameters) => this.ExecuteScalar<T>(connectionItem, procedureName, dbParameters, true);
        async Task<T> QueryScalarByProcedureAsync<T>(DBConnectionItem connectionItem, string procedureName, IEnumerable<ParamItem> dbParameters) => await this.ExecuteScalarAsync<T>(connectionItem, procedureName, dbParameters, true);
        #endregion

        #region 存储过程查多个 返回集合
        IEnumerable<T> QueryByProcedure<T>(string procedureName) => this.QueryByProcedure<T>(dbname: null, procedureName);
        IEnumerable<T> QueryByProcedure<T>(string dbname, string procedureName)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryByProcedure<T>(conn, procedureName, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        IAsyncEnumerable<T> QueryByProcedureAsync<T>(string procedureName) => this.QueryByProcedureAsync<T>(dbname: null, procedureName);
        IAsyncEnumerable<T> QueryByProcedureAsync<T>(string dbname, string procedureName)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryByProcedureAsync<T>(conn, procedureName, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        IEnumerable<T> QueryByProcedure<T>(string dbname, string procedureName, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryByProcedure<T>(conn, procedureName, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        IAsyncEnumerable<T> QueryByProcedureAsync<T>(string dbname, string procedureName, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.QueryByProcedureAsync<T>(conn, procedureName, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }
        }
        IEnumerable<T> QueryByProcedure<T>(DBConnectionItem connectionItem, string procedureName) => this.QueryByProcedure<T>(connectionItem, procedureName, null);
        IAsyncEnumerable<T> QueryByProcedureAsync<T>(DBConnectionItem connectionItem, string procedureName) => this.QueryByProcedureAsync<T>(connectionItem, procedureName, null);
        IEnumerable<T> QueryByProcedure<T>(DBConnectionItem connectionItem, string procedureName, IEnumerable<ParamItem> dbParameters) => this.ExecuteQuery<T>(connectionItem, procedureName, dbParameters, true);
        IAsyncEnumerable<T> QueryByProcedureAsync<T>(DBConnectionItem connectionItem, string procedureName, IEnumerable<ParamItem> dbParameters) => this.ExecuteQueryAsync<T>(connectionItem, procedureName, dbParameters, true);
        #endregion


        /// <summary>
        /// CH的流导出，其它数据库暂不支持
        /// </summary>
        /// <param name="connectionItem"></param>
        /// <param name="commandText"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        Stream QueryStream(DBConnectionItem connectionItem, string commandText, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            throw new NotSupportedException("暂不支持");
        }

        Task<Stream> QueryStreamAsync(DBConnectionItem connectionItem, string commandText, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            throw new NotSupportedException("暂不支持");
        }

        Task ExportToCsvFileAsync(DBConnectionItem connectionItem, string commandText, string filePath, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false)
        {
            throw new NotSupportedException("暂不支持");
        }


        #endregion

        #region 执行存储过程
        /// <summary>
        /// 执行存储过程
        /// </summary>
        /// <param name="procedureName"></param>
        /// <returns></returns>
        int ExecuteProcedure(string procedureName) => this.ExecuteProcedure(dbname: null, procedureName);
        async Task<int> ExecuteProcedureAsync(string procedureName) => await this.ExecuteProcedureAsync(dbname: null, procedureName);

        int ExecuteProcedure(string dbname, string procedureName)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.ExecuteProcedure(conn, procedureName, null);
            }
            finally
            {
                conn.GiveBack();
            }

        }

        async Task<int> ExecuteProcedureAsync(string dbname, string procedureName)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.ExecuteProcedureAsync(conn, procedureName, null);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        int ExecuteProcedure(string dbname, string procedureName, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return this.ExecuteProcedure(conn, procedureName, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }

        }

        async Task<int> ExecuteProcedureAsync(string dbname, string procedureName, IEnumerable<ParamItem> dbParameters)
        {
            var conn = this.GetConnection(dbname);
            try
            {
                return await this.ExecuteProcedureAsync(conn, procedureName, dbParameters);
            }
            finally
            {
                conn.GiveBack();
            }
        }

        int ExecuteProcedure(DBConnectionItem connectionItem, string procedureName) => this.ExecuteProcedure(connectionItem, procedureName, null);
        async Task<int> ExecuteProcedureAsync(DBConnectionItem connectionItem, string procedureName) => await this.ExecuteProcedureAsync(connectionItem, procedureName, null);
        int ExecuteProcedure(DBConnectionItem connectionItem, string procedureName, IEnumerable<ParamItem> dbParameters) => this.ExecuteNonQuery(connectionItem, procedureName, dbParameters, true);
        async Task<int> ExecuteProcedureAsync(DBConnectionItem connectionItem, string procedureName, IEnumerable<ParamItem> dbParameters) => await this.ExecuteNonQueryAsync(connectionItem, procedureName, dbParameters, true);
        #endregion

        #endregion

        #region 数据转换

        /// <summary>
        /// 查询转实体
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="reader"></param>
        /// <returns></returns>
        protected static T Dr2T<T>(DbDataReader reader)
        {
            try
            {
                var type = typeof(T);
                if (type.IsPrimitive || type.Name == "string" || type.Name == "String")
                {
                    if (reader.FieldCount > 0)
                    {
                        try
                        {
                            return (T)Convert.ChangeType(reader.GetValue(0), type);
                        }
                        catch (Exception ex)
                        {
                            SqlLogUtil.SendLog(LogType.Error, ex.ToString());
                            throw;
                        }
                    }
                    else
                    {
                        return default;
                    }
                }
                else if (type.Name == "object" || type.Name == "Object")
                {
                    dynamic obj = new ExpandoObject();
                    var dic = (IDictionary<string, object>)obj;
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var name = reader.GetName(i);
                        dic[name] = reader.GetValue(i);
                    }
                    return obj;
                }
                else
                {
                    var t = Activator.CreateInstance<T>();
                    var ps = EntityCache.TryGetPropertyInfos<T>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var name = reader.GetName(i);
                        if (ps.TryGetValue(name, out var p))
                        {

                            if (!reader.IsDBNull(i))
                            {
                                var obj = reader.GetValue(i);
                                if (reader.GetFieldType(i) == p.PropertyType)
                                {
                                    p.SetValue(t, obj);
                                }
                                else
                                {
                                    object value = null;
                                    if (p.DBType == "JSONSTR")
                                    {
                                        value = JsonConvert.DeserializeObject((string)obj, p.PropertyType);
                                    }
                                    else
                                    {
                                        if (p.Nullable)
                                        {

                                            value = Convert.ChangeType(obj, p.PropertyType.GetGenericArguments()[0]);

                                        }
                                        else
                                        {
                                            value = Convert.ChangeType(obj, p.PropertyType);
                                        }
                                    }
                                    p.SetValue(t, value);
                                }
                            }
                            else
                            {
                                p.SetValue(t, default);
                            }
                        }
                    }
                    return t;
                }
            }
            catch (Exception ex)
            {
                SqlLogUtil.SendLog(LogType.Error, ex.ToString());
                throw;
            }
        }
        ParamSql T2InSql<T>(T t, string databaseName, bool InsAutoIncrement = false)
        {
            ParamSql ps = new ParamSql();
            if (t == null) { return null; }
            var nea = EntityCache.TryGetInfo<T>();
            string InsertSQL = "Insert into {0} ({1}) values({2});";
            ValueStringBuilder sb_Columns = new ValueStringBuilder(512);
            ValueStringBuilder sb_Values = new ValueStringBuilder(512);
            foreach (var item in nea.PropertyInfos)
            {

                if (!InsAutoIncrement && nea.AutoIncrements.Contains(item.Key))
                {
                    continue;
                }

                if (item.Value.PropertyType == typeof(DateTime))
                {
                    DateTime? value = (DateTime)item.Value.GetValue(t);
                    if (value == DateTime.MinValue)
                    {
                        value = ConvertToDBMinTime(value);
                    }
                    (string key, string skey) = EscapeParamKey(item.Key, item.Value.PropertyType.Name);
                    sb_Columns.Append(EscapeStr(item.Key) + ',');
                    sb_Values.Append(skey + ',');
                    ps.Params.Add(new ParamItem(key, value, System.Data.DbType.DateTime));

                }
                else
                {
                    var value = item.Value.GetValue(t);
                    if (value != null)
                    {
                        (string key, string skey) = EscapeParamKey(item.Key);
                        sb_Columns.Append(EscapeStr(item.Key) + ',');
                        sb_Values.Append(skey + ',');
                        if (item.Value.DBType == "JSONSTR")
                        {
                            var str = JsonConvert.SerializeObject(value);
                            ps.Params.Add(new ParamItem(key, str));
                        }
                        else
                        {
                            ps.Params.Add(new ParamItem(key, value));
                        }
                    }
                }

            }

            string sql = string.Format(InsertSQL, EscapeStr(databaseName, nea.TableName), sb_Columns.ToString().TrimEnd(','), sb_Values.ToString().TrimEnd(','));

            ps.SqlStr = sql;
            return ps;


        }
        ParamSql Ts2InSql<T>(List<T> ts, string databaseName, bool InsAutoIncrement = false)
        {
            ParamSql ps = new ParamSql();
            if (ts == null || ts.Count == 0) { return null; }
            var nea = EntityCache.TryGetInfo<T>();
            ValueStringBuilder sb_SQL = new ValueStringBuilder(ts.Count * 128);
            sb_SQL.Append($"Insert into {EscapeStr(databaseName, nea.TableName)} (");

            bool haveValue = false;
            foreach (var item in nea.PropertyInfos)
            {

                if (!InsAutoIncrement && nea.AutoIncrements.Contains(item.Key))
                {
                    continue;
                }
                if (haveValue)
                {
                    sb_SQL.Append(',');
                    sb_SQL.Append(EscapeStr(item.Key));
                }
                else
                {
                    sb_SQL.Append(EscapeStr(item.Key));
                    haveValue = true;
                }
            }
            sb_SQL.Append(") values (");
            for (int i = 0; i < ts.Count; i++)
            {
                var t = ts[i];
                haveValue = false;
                foreach (var item in nea.PropertyInfos)
                {

                    if (!InsAutoIncrement && nea.AutoIncrements.Contains(item.Key))
                    {
                        continue;
                    }

                    if (item.Value.PropertyType == typeof(DateTime))
                    {
                        DateTime? value = (DateTime)item.Value.GetValue(t);
                        if (value == DateTime.MinValue)
                        {
                            value = ConvertToDBMinTime(value);
                        }
                        (string key, string skey) = EscapeParamKey($"{item.Key}_{i}", item.Value.PropertyType.Name);

                        if (haveValue)
                        {
                            sb_SQL.Append(',');
                            sb_SQL.Append(skey);
                        }
                        else
                        {
                            sb_SQL.Append(skey);
                            haveValue = true;
                        }
                        ps.Params.Add(new ParamItem(key, value, System.Data.DbType.DateTime));

                    }
                    else
                    {
                        var value = item.Value.GetValue(t);
                        (string key, string skey) = EscapeParamKey($"{item.Key}_{i}", item.Value.PropertyType.Name);
                        if (haveValue)
                        {
                            sb_SQL.Append(',');
                            sb_SQL.Append(skey);
                        }
                        else
                        {
                            sb_SQL.Append(skey);
                            haveValue = true;
                        }
                        if (item.Value.DBType == "JSONSTR")
                        {
                            var str = JsonConvert.SerializeObject(value);
                            ps.Params.Add(new ParamItem(key, str));
                        }
                        else
                        {
                            ps.Params.Add(new ParamItem(key, value));
                        }
                    }
                }
                if (i == ts.Count - 1)
                {
                    sb_SQL.Append(")");
                }
                else
                {
                    sb_SQL.Append("),(");
                }
            }
            sb_SQL.Append(";");
            ps.SqlStr = sb_SQL.ToString();
            return ps;
        }

        string ConvertSelectLimit(string v, int count);
        string CreatePageSql(string tableName, string fieldsStr, string where, string groupBy, string orderBy, int pageNum, int pageSize, string dbVer)
        {
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr} from {tableName}");
            if (!string.IsNullOrEmpty(where))
            {
                sb_sql.Append(where);
            }
            if (!string.IsNullOrEmpty(groupBy))
            {
                sb_sql.Append(groupBy);
            }
            if (!string.IsNullOrEmpty(orderBy))
            {
                sb_sql.Append(orderBy);
            }

            sb_sql.Append($" limit {pageSize * (pageNum - 1)},{pageSize};");
            return sb_sql.ToString();
        }
        string CreatePageSql(string sql, int pageNum, int pageSize, string dbVer)
        {
            StringBuilder sb_sql = new StringBuilder($"select {sql}");
            sb_sql.Append($" limit {pageSize * (pageNum - 1)},{pageSize};");
            return sb_sql.ToString();
        }

        //兼容clickhouse
        string CreateDelSql(string dbTable, string whereStr, bool IsSync = true)
        {
            return $"Delete from {dbTable} where {whereStr}";
        }

        //兼容clickhouse
        string CreateUpdateSql(string dbTable, string setStr, string whereStr, bool IsSync = true)
        {
            return $"update {dbTable} set {setStr} where {whereStr};";
        }

        #endregion

    }
}
