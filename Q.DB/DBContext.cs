/*************************************************************************************
 *
 * 文 件 名:   DBContext.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/13 9:44
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
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using Q.DB.Interface;
using Q.DB.Models;
using Q.DB.Models.Filter.Filter;
using Q.DB.Query;

namespace Q.DB
{
    public class DBContext : IDisposable
    {
        public DBConnectionItem ConnectionItem { internal set; get; }
        IDBEngine Engine;
        public string DBName = string.Empty;
        string Tag = string.Empty;
        public DbTransaction Transaction { get => ConnectionItem.transaction; }

        public DBContext(string dbName = null, string tag = null)
        {
            Tag = tag ?? "Default";
            Engine = DBFactory.Instance.GetEngine(tag);
            DBName = dbName ?? Engine.DefaultDBName;
            ConnectionItem = Engine.GetConnection(DBName);

        }

        public void ChangeRuningDB(string dbname)
        {
            DBName = dbname ?? Engine.DefaultDBName;
            if (ConnectionItem.SqlConnection.Database != DBName)
            {
                ConnectionItem.SqlConnection.ChangeDatabase(DBName);
            }
        }

        #region 事务
        public void BeginTransaction()
        {
            ConnectionItem.BeginTransaction();
        }
        public void BeginTransaction(IsolationLevel iso)
        {
            ConnectionItem.BeginTransaction(iso);
        }


        public void Commit()
        {
            ConnectionItem.Commit();
        }
        public async Task CommitAsync()
        {
            await ConnectionItem.CommitAsync();
        }

        public void Rollback()
        {
            ConnectionItem.Rollback();
        }
        public async Task RollbackAsync()
        {
            await ConnectionItem.RollbackAsync();
        }
        #endregion

        #region 基本查询

        public int ExecuteNonQuery(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false, bool lastIdentity = false) => Engine.ExecuteNonQuery(ConnectionItem, text, dbParameters, IsProcedure, lastIdentity);
        public async Task<int> ExecuteNonQueryAsync(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false, bool lastIdentity = false) => await Engine.ExecuteNonQueryAsync(ConnectionItem, text, dbParameters, IsProcedure, lastIdentity);
        public T ExecuteScalar<T>(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false) => Engine.ExecuteScalar<T>(ConnectionItem, text, dbParameters, IsProcedure);
        public async Task<T> ExecuteScalarAsync<T>(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false) => await Engine.ExecuteScalarAsync<T>(ConnectionItem, text, dbParameters, IsProcedure);
        public T ExecuteQueryFirst<T>(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false) => Engine.ExecuteQueryFirst<T>(ConnectionItem, text, dbParameters, IsProcedure);
        public async Task<T> ExecuteQueryFirstAsync<T>(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false) => await Engine.ExecuteQueryFirstAsync<T>(ConnectionItem, text, dbParameters, IsProcedure);
        public IEnumerable<T> ExecuteQuery<T>(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false) => Engine.ExecuteQuery<T>(ConnectionItem, text, dbParameters, IsProcedure);
        public IAsyncEnumerable<T> ExecuteQueryAsync<T>(string text, IEnumerable<ParamItem> dbParameters = null, bool IsProcedure = false) => Engine.ExecuteQueryAsync<T>(ConnectionItem, text, dbParameters, IsProcedure);
        #endregion

        #region 表达式查询
        public IQingQuery<T> Query<T>(string tableSuffix = null)
        {
            return new QingQuery<T>(Engine, ConnectionItem, tableSuffix);
        }

        public IQingQuery<T> CreateQuery<T>(SqlFilter fc, string tableSuffix = null)
        {
            IQingQuery<T> query = new QingQuery<T>(Engine, ConnectionItem, tableSuffix);
            return query.ResolveFilterCondition(fc);
        }
        #endregion


        #region 插入


        public int Insert<T>(T t, string tableSuffix = null)
        {
            return Engine.Insert<T>(ConnectionItem, t, tableSuffix);
        }
        public async Task<int> InsertAsync<T>(T t, string tableSuffix = null)
        {
            return await Engine.InsertAsync<T>(ConnectionItem, t, tableSuffix);
        }
        public long Insert64<T>(T t, string tableSuffix = null)
        {
            return Engine.Insert64<T>(ConnectionItem, t, tableSuffix);
        }
        public async Task<long> Insert64Async<T>(T t, string tableSuffix = null)
        {
            return await Engine.Insert64Async<T>(ConnectionItem, t, tableSuffix);
        }
        public int BatchInsert<T>(List<T> ts, string tableSuffix = null, int splitCount = 300)
        {
            return Engine.BatchInsert<T>(ConnectionItem, ts, tableSuffix, splitCount);
        }
        public async Task<int> BatchInsertAsync<T>(List<T> ts, string tableSuffix = null, int splitCount = 300)
        {
            return await Engine.BatchInsertAsync<T>(ConnectionItem, ts, tableSuffix, splitCount);
        }
        public long BulkInsert<T>(List<T> ts, string tableSuffix = null, int splitCount = 10000)
        {
            return Engine.BulkInsert<T>(ConnectionItem, ts, tableSuffix, splitCount);
        }
        public async Task<long> BulkInsertAsync<T>(List<T> ts, string tableSuffix = null, int splitCount = 10000)
        {
            return await Engine.BulkInsertAsync<T>(ConnectionItem, ts, tableSuffix, splitCount);
        }

        #endregion

        #region 删除
        /// <summary>
        /// 同步删除
        /// ClickHouse 会先执行 SET mutations_sync = 2;
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expression"></param>
        /// <param name="tableSuffix"></param>
        /// <returns></returns>
        public int Delete<T>(Expression<Func<T, bool>> expression, string tableSuffix = null)
        {
            return Query<T>(tableSuffix).Remove(expression);
        }
        /// <summary>
        /// ClickHouse 会先执行 SET mutations_sync = 0;
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expression"></param>
        /// <param name="tableSuffix"></param>
        /// <returns></returns>
        public Task<int> DeleteAsync<T>(Expression<Func<T, bool>> expression, string tableSuffix = null)
        {
            return Query<T>(tableSuffix).RemoveAsync(expression);
        }
        #endregion

        #region 更新
        public int Update<T>(Expression<Func<T, T>> expressionNew, Expression<Func<T, bool>> expressionWhere, string tableSuffix = null)
        {
            try
            {


                var tableName = EntityCache.TryGetTableName<T>();

                ParamSql ps = new ParamSql();
                var dic_update = ExpressionResolver.ResolveUpdateExpression(expressionNew, Engine);
                StringBuilder sb_setStr = new StringBuilder();
                foreach (var item in dic_update)
                {
                    sb_setStr.Append($",{item.SqlStr}");
                    ps.Params.AddRange(item.Params);
                }
                string setStr = sb_setStr.ToString().TrimStart(',');
                var ps_where = ExpressionResolver.ResolveExpression(expressionWhere.Body, Engine);
                ps.Params.AddRange(ps_where.Params);

                string sql = $"update {Engine.EscapeStr(tableName + QDBTools.ConvertSuffixTableName(tableSuffix))} set {setStr} where {ps_where.SqlStr};";
                if (!string.IsNullOrEmpty(tableSuffix))
                {
                    string oldTableName = Engine.EscapeStr(tableName);
                    string newTableName = Engine.EscapeStr(tableName + QDBTools.ConvertSuffixTableName(tableSuffix));
                    sql = sql.Replace(oldTableName, newTableName);
                }
                return Engine.ExecuteNonQuery(ConnectionItem, sql, ps.Params);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<int> UpdateAsync<T>(Expression<Func<T, T>> expressionNew, Expression<Func<T, bool>> expressionWhere, string tableSuffix = null)
        {
            try
            {

                var tableName = EntityCache.TryGetTableName<T>();
                ParamSql ps = new ParamSql();
                var dic_update = ExpressionResolver.ResolveUpdateExpression(expressionNew, Engine);
                StringBuilder sb_setStr = new StringBuilder();
                foreach (var item in dic_update)
                {
                    sb_setStr.Append($",{item.SqlStr}");
                    ps.Params.AddRange(item.Params);
                }
                string setStr = sb_setStr.ToString().TrimStart(',');
                var ps_where = ExpressionResolver.ResolveExpression(expressionWhere.Body, Engine);
                ps.Params.AddRange(ps_where.Params);

                string sql = $"update {tableName}{QDBTools.ConvertSuffixTableName(tableSuffix)} set {setStr} where {ps_where.SqlStr};";

                return await Engine.ExecuteNonQueryAsync(ConnectionItem, sql, ps.Params);
            }
            catch (Exception)
            {

                throw;
            }
        }

        public int CompareChangeToUpdate<T>(T dest, T source, Expression<Func<T, bool>> where, string tableSuffix = null)
        {
            var ps = QDBTools.CreateCompareChangeSql(dest, source, where, Engine, tableSuffix);
            if (ps.SqlStr != null)
            {
                return Engine.ExecuteNonQuery(ConnectionItem, ps.SqlStr, ps.Params);
            }
            else
            {
                return 0;
            }
        }

        public async Task<int> CompareChangeToUpdateAsync<T>(T dest, T source, Expression<Func<T, bool>> where, string tableSuffix = null)
        {
            var ps = QDBTools.CreateCompareChangeSql(dest, source, where, Engine, tableSuffix);
            if (ps.SqlStr != null)
            {
                return await Engine.ExecuteNonQueryAsync(ConnectionItem, ps.SqlStr, ps.Params);
            }
            else
            {
                return 0;
            }
        }
        #endregion

        #region 创建表
        public bool CreateTable<T>(string tableSuffix = null, bool force = false, string tableEngine = "Default", string[] additionals = null)
        {
            var tableName = EntityCache.TryGetTableName<T>() + QDBTools.ConvertSuffixTableName(tableSuffix);
            return CreateTableByName<T>(tableName, force, tableEngine, additionals);

        }
        public bool CreateTableByName<T>(string tableName, bool force = false, string tableEngine = "Default", string[] additionals = null)
        {
            return Engine.CreateTable<T>(ConnectionItem, tableName, force, tableEngine, additionals);

        }
        public bool CreateTableIfNotExists<T>(string tableSuffix = null, string tableEngine = "Default", string[] additionals = null)
        {
            var tableName = EntityCache.TryGetTableName<T>() + QDBTools.ConvertSuffixTableName(tableSuffix);
            return CreateTableIfNotExistsByName<T>(tableName, tableEngine, additionals);

        }
        public bool CreateTableIfNotExistsByName<T>(string tableName, string tableEngine = "Default", string[] additionals = null)
        {
            return Engine.CreateTableIfNotExists<T>(ConnectionItem, tableName, tableEngine, additionals);
        }
        /// <summary>
        /// 检查表是否存在
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public bool CheckTableExisted(string tableName)
        {
            return Engine.CheckTableExisted(ConnectionItem, tableName);
        }
        /// <summary>
        /// 检查表是否存在
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableSuffix"></param>
        /// <returns></returns>
        public bool CheckTableExisted<T>(string tableSuffix = null)
        {
            var tableName = EntityCache.TryGetTableName<T>() + QDBTools.ConvertSuffixTableName(tableSuffix);
            return Engine.CheckTableExisted(ConnectionItem, tableName);
        }

        public bool RenameTable(string oldTableName, string newTableName)
        {
            return Engine.RenameTable(ConnectionItem, oldTableName, newTableName);
        }
        #endregion

        public bool DropTable(string tableName)
        {
            return Engine.DropTable(ConnectionItem, tableName);
        }
        public async Task<bool> DropTableAsync(string tableName)
        {
            return await Engine.DropTableAsync(ConnectionItem, tableName);
        }

        public void Dispose()
        {
            ConnectionItem.TransactionDispose();
            if (ConnectionItem != null)
            {
                ConnectionItem.GiveBack();
            }
        }

        public TEngine DBEngine<TEngine>()
        {
            return (TEngine)Engine;
        }
    }
}
