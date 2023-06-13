/*************************************************************************************
 *
 * 文 件 名:   QingQueryTN.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/15 10:42
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
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Q.DB.Interface;
using Q.DB.Models;

namespace Q.DB.Query
{
    #region T2
    internal class QingQuery<T, T2> : IQingQuery<T, T2>
    {
        internal DBConnectionItem dbConnection { set; get; }
        internal string WhereStr { get; set; } = string.Empty;
        internal string Fields { get; set; }
        internal string Limit { get; set; } = "";
        internal string OrderByStr { get; set; }
        internal string GroupByStr { get; set; }
        internal string JoinStr { get; set; }
        internal List<ParamItem> SqlParams = new();
        internal IDBEngine DBEngine { get; set; }
        internal string T1TableSuffix = null;
        internal string T2TableSuffix = null;



        public QingQuery(QingQuery<T> edbq, string joinStr, List<ParamItem> sqlParams, string t2TableSuffix = null)
        {
            this.dbConnection = edbq.dbConnection;
            this.Fields = edbq.Fields;
            this.Limit = edbq.Limit;
            this.OrderByStr = edbq.OrderByStr;
            this.JoinStr += joinStr;
            this.SqlParams = sqlParams;
            this.T1TableSuffix = edbq.TableSuffix;
            this.T2TableSuffix = t2TableSuffix;

        }


        public IQingQuery<T, T2> OrderBy<TResult>(Expression<Func<T, T2, TResult>> expression, bool Desc = false)
        {
            return ExpressionResolver.ResolveOrderByExpression(this, expression, Desc);
        }
        public IQingQuery<T, T2> GroupBy<TResult>(Expression<Func<T, T2, TResult>> expression)
        {
            return ExpressionResolver.ResolveGroupByExpression(this, expression);
        }

        public IQingQuery<T, T2> Select<TResult>(Expression<Func<T, T2, TResult>> expression)
        {
            return ExpressionResolver.ResolveSelectExpression(this, expression);
        }

        public IQingQuery<T, T2> Where(Expression<Func<T, T2, bool>> expression)
        {
            return ExpressionResolver.ResolveWhereExpression(this, expression);
        }
        public TResult QueryFirst<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            return DBEngine.QueryFirst<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> QueryFirstAsync<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            return DBEngine.QueryFirstAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public IEnumerable<TResult> QueryWithPage<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAll<TResult>();
        }
        public IAsyncEnumerable<TResult> QueryWithPageAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAllAsync<TResult>();
        }
        public PageResult<TResult> QueryPages<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = Count();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
                pageResult.ArrayData = QueryWithPage<TResult>(pageNum, pageSize);
            }
            return pageResult;
        }

        public async Task<PageResult<TResult>> QueryPagesAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = await CountAsync();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
#if NET7_0_OR_GREATER
                pageResult.ArrayData = QueryWithPageAsync<TResult>(pageNum, pageSize).ToBlockingEnumerable();
#else
                List<TResult> list = new List<TResult>();
                await foreach (var item in QueryWithPageAsync<TResult>(pageNum, pageSize))
                {
                    list.Add(item);
                }
                pageResult.ArrayData = list;
#endif
            }
            return pageResult;
        }

        public IEnumerable<TResult> QueryAll<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            return DBEngine.Query<TResult>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<TResult> QueryAllAsync<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            return DBEngine.QueryAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Max<TResult>(Expression<Func<T, T2, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }

        public Task<TResult> MaxAsync<TResult>(Expression<Func<T, T2, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Min<TResult>(Expression<Func<T, T2, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> MinAsync<TResult>(Expression<Func<T, T2, TResult>> expression)
        {
            ExpressionResolver.ResolveMinExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);

        }
        public int Count()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            return DBEngine.ExecuteScalar<int>(dbConnection, sql, SqlParams);
        }
        public Task<int> CountAsync()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            return DBEngine.ExecuteScalarAsync<int>(dbConnection, sql, SqlParams);

        }

        private string BuildSql()
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = Fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }
            if (!string.IsNullOrEmpty(OrderByStr))
            {
                sb_sql.Append($" Order By {OrderByStr.TrimStart(',')}");
            }
            if (!string.IsNullOrEmpty(Limit))
            {
                sb_sql.Append($"  {Limit}");
            }
            return sb_sql.ToString();
        }

        private string BuildSql_Custom_Fields_Func(string fields)
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }

            return sb_sql.ToString();
        }

        public IQingQuery<T, T2, T3> InnerJoin<T3>(Expression<Func<T, T2, T3, bool>> expressionJoinOn, string t3TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T3>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Inner Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3>(this, JoinStr, SqlParams, t3TableSuffix);
        }

        public IQingQuery<T, T2, T3> LeftJoin<T3>(Expression<Func<T, T2, T3, bool>> expressionJoinOn, string t3TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T3>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Left Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3>(this, JoinStr, SqlParams, t3TableSuffix);
        }
        public IQingQuery<T, T2, T3> RightJoin<T3>(Expression<Func<T, T2, T3, bool>> expressionJoinOn, string t3TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T3>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Right Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";
            return new QingQuery<T, T2, T3>(this, JoinStr, SqlParams, t3TableSuffix);
        }


    }
    #endregion

    #region T3

    internal class QingQuery<T, T2, T3> : IQingQuery<T, T2, T3>
    {

        internal DBConnectionItem dbConnection { set; get; }
        internal string WhereStr { get; set; } = string.Empty;
        internal string Fields { get; set; }
        internal string Limit { get; set; } = "";
        internal string OrderByStr { get; set; }

        internal string GroupByStr { set; get; }
        internal string JoinStr { get; set; }
        internal List<ParamItem> SqlParams = new List<ParamItem>();
        internal IDBEngine DBEngine { set; get; }
        internal string T1TableSuffix = null;
        internal string T2TableSuffix = null;
        internal string T3TableSuffix = null;

        public QingQuery(QingQuery<T, T2> edbq, string joinStr, List<ParamItem> sqlParams, string t3TableSuffix = null)
        {
            this.dbConnection = edbq.dbConnection;
            this.Fields = edbq.Fields;
            this.Limit = edbq.Limit;
            this.OrderByStr = edbq.OrderByStr;
            this.JoinStr += joinStr;
            this.SqlParams = sqlParams;
            this.DBEngine = edbq.DBEngine;
            this.T1TableSuffix = edbq.T1TableSuffix;
            this.T2TableSuffix = edbq.T2TableSuffix;
            this.T3TableSuffix = t3TableSuffix;
        }


        public IQingQuery<T, T2, T3> OrderBy<TResult>(Expression<Func<T, T2, T3, TResult>> expression, bool Desc = false)
        {
            return ExpressionResolver.ResolveOrderByExpression(this, expression, Desc);
        }

        public IQingQuery<T, T2, T3> Select<TResult>(Expression<Func<T, T2, T3, TResult>> expression)
        {
            return ExpressionResolver.ResolveSelectExpression(this, expression);

        }

        public IQingQuery<T, T2, T3> Where(Expression<Func<T, T2, T3, bool>> expression)
        {
            return ExpressionResolver.ResolveWhereExpression(this, expression);
        }

        public TResult QueryFirst<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            return DBEngine.QueryFirst<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> QueryFirstAsync<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            return DBEngine.QueryFirstAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public IEnumerable<TResult> QueryWithPage<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAll<TResult>();
        }
        public IAsyncEnumerable<TResult> QueryWithPageAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAllAsync<TResult>();
        }
        public PageResult<TResult> QueryPages<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = Count();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
                pageResult.ArrayData = QueryWithPage<TResult>(pageNum, pageSize);
            }
            return pageResult;
        }

        public async Task<PageResult<TResult>> QueryPagesAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = await CountAsync();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
#if NET7_0_OR_GREATER
                pageResult.ArrayData = QueryWithPageAsync<TResult>(pageNum, pageSize).ToBlockingEnumerable();
#else
                List<TResult> list = new List<TResult>();
                await foreach (var item in QueryWithPageAsync<TResult>(pageNum, pageSize))
                {
                    list.Add(item);
                }
                pageResult.ArrayData = list;
#endif
            }
            return pageResult;
        }

        public IEnumerable<TResult> QueryAll<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            return DBEngine.Query<TResult>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<TResult> QueryAllAsync<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            return DBEngine.QueryAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Max<TResult>(Expression<Func<T, T2, T3, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }

        public async Task<TResult> MaxAsync<TResult>(Expression<Func<T, T2, T3, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            return await DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Min<TResult>(Expression<Func<T, T2, T3, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }
        public async Task<TResult> MinAsync<TResult>(Expression<Func<T, T2, T3, TResult>> expression)
        {
            ExpressionResolver.ResolveMinExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            return await DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);

        }
        public int Count()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            return DBEngine.ExecuteScalar<int>(dbConnection, sql, SqlParams);

        }
        public async Task<int> CountAsync()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            return await DBEngine.ExecuteScalarAsync<int>(dbConnection, sql, SqlParams);

        }

        private string BuildSql()
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = Fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }
            if (!string.IsNullOrEmpty(OrderByStr))
            {
                sb_sql.Append($" Order By {OrderByStr.TrimStart(',')}");
            }
            if (!string.IsNullOrEmpty(Limit))
            {
                sb_sql.Append($"  {Limit}");
            }
            return sb_sql.ToString();
        }
        private string BuildSql_Custom_Fields_Func(string fields)
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }

            return sb_sql.ToString();
        }

        public IQingQuery<T, T2, T3, T4> InnerJoin<T4>(Expression<Func<T, T2, T3, T4, bool>> expressionJoinOn, string t4TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T4>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Inner Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4>(this, JoinStr, SqlParams, t4TableSuffix);
        }

        public IQingQuery<T, T2, T3, T4> LeftJoin<T4>(Expression<Func<T, T2, T3, T4, bool>> expressionJoinOn, string t4TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T4>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" left Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4>(this, JoinStr, SqlParams, t4TableSuffix);
        }
        public IQingQuery<T, T2, T3, T4> RightJoin<T4>(Expression<Func<T, T2, T3, T4, bool>> expressionJoinOn, string t4TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T4>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Right Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4>(this, JoinStr, SqlParams, t4TableSuffix);
        }

        public IQingQuery<T, T2, T3> GroupBy<TResult>(Expression<Func<T, T2, T3, TResult>> expression)
        {
            return ExpressionResolver.ResolveGroupByExpression(this, expression);
        }
    }

    #endregion

    #region T4

    internal class QingQuery<T, T2, T3, T4> : IQingQuery<T, T2, T3, T4>
    {


        internal DBConnectionItem dbConnection { set; get; }
        internal string WhereStr { get; set; } = string.Empty;
        internal string Fields { get; set; }
        internal string Limit { get; set; } = "";
        internal string OrderByStr { get; set; }
        internal string GroupByStr { set; get; }
        internal string JoinStr { get; set; }
        internal List<ParamItem> SqlParams = new List<ParamItem>();
        internal IDBEngine DBEngine { get; set; }
        internal string T1TableSuffix = null;
        internal string T2TableSuffix = null;
        internal string T3TableSuffix = null;
        internal string T4TableSuffix = null;
        public QingQuery(QingQuery<T, T2, T3> edbq, string joinStr, List<ParamItem> sqlParams, string t4TableSuffix = null)
        {
            this.DBEngine = edbq.DBEngine;
            this.dbConnection = edbq.dbConnection;
            this.Fields = edbq.Fields;
            this.Limit = edbq.Limit;
            this.OrderByStr = edbq.OrderByStr;
            this.JoinStr += joinStr;
            this.SqlParams = sqlParams;
            this.T1TableSuffix = edbq.T1TableSuffix;
            this.T2TableSuffix = edbq.T2TableSuffix;
            this.T3TableSuffix = edbq.T3TableSuffix;
            this.T4TableSuffix = t4TableSuffix;
        }

        private string BuildSql()
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = Fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }
            if (!string.IsNullOrEmpty(OrderByStr))
            {
                sb_sql.Append($" Order By {OrderByStr.TrimStart(',')}");
            }
            if (!string.IsNullOrEmpty(Limit))
            {
                sb_sql.Append($"  {Limit}");
            }
            return sb_sql.ToString();
        }
        private string BuildSql_Custom_Fields_Func(string fields)
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }

            return sb_sql.ToString();
        }


        public IQingQuery<T, T2, T3, T4> Select<TResult>(Expression<Func<T, T2, T3, T4, TResult>> expression)
        {
            return ExpressionResolver.ResolveSelectExpression(this, expression);
        }

        public IQingQuery<T, T2, T3, T4> Where(Expression<Func<T, T2, T3, T4, bool>> expression)
        {
            return ExpressionResolver.ResolveWhereExpression(this, expression);
        }

        public IQingQuery<T, T2, T3, T4> OrderBy<TResult>(Expression<Func<T, T2, T3, T4, TResult>> expression, bool Desc = false)
        {
            return ExpressionResolver.ResolveOrderByExpression(this, expression, Desc);
        }

        public TResult QueryFirst<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            return DBEngine.QueryFirst<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> QueryFirstAsync<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            return DBEngine.QueryFirstAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public IEnumerable<TResult> QueryWithPage<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAll<TResult>();
        }
        public IAsyncEnumerable<TResult> QueryWithPageAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAllAsync<TResult>();
        }
        public PageResult<TResult> QueryPages<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = Count();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
                pageResult.ArrayData = QueryWithPage<TResult>(pageNum, pageSize);
            }
            return pageResult;
        }

        public async Task<PageResult<TResult>> QueryPagesAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = await CountAsync();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
#if NET7_0_OR_GREATER
                pageResult.ArrayData = QueryWithPageAsync<TResult>(pageNum, pageSize).ToBlockingEnumerable();
#else
                List<TResult> list = new List<TResult>();
                await foreach (var item in QueryWithPageAsync<TResult>(pageNum, pageSize))
                {
                    list.Add(item);
                }
                pageResult.ArrayData = list;
#endif
            }
            return pageResult;
        }

        public IEnumerable<TResult> QueryAll<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            return DBEngine.Query<TResult>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<TResult> QueryAllAsync<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            return DBEngine.QueryAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Max<TResult>(Expression<Func<T, T2, T3, T4, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }

        public Task<TResult> MaxAsync<TResult>(Expression<Func<T, T2, T3, T4, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Min<TResult>(Expression<Func<T, T2, T3, T4, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> MinAsync<TResult>(Expression<Func<T, T2, T3, T4, TResult>> expression)
        {
            ExpressionResolver.ResolveMinExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);

        }
        public int Count()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            return DBEngine.ExecuteScalar<int>(dbConnection, sql, SqlParams);
        }
        public Task<int> CountAsync()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            return DBEngine.ExecuteScalarAsync<int>(dbConnection, sql, SqlParams);

        }

        public IQingQuery<T, T2, T3, T4, T5> LeftJoin<T5>(Expression<Func<T, T2, T3, T4, T5, bool>> expressionJoinOn, string t5TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T5>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Left Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5>(this, JoinStr, SqlParams, t5TableSuffix);
        }

        public IQingQuery<T, T2, T3, T4, T5> RightJoin<T5>(Expression<Func<T, T2, T3, T4, T5, bool>> expressionJoinOn, string t5TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T5>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Right Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5>(this, JoinStr, SqlParams, t5TableSuffix);
        }

        public IQingQuery<T, T2, T3, T4, T5> InnerJoin<T5>(Expression<Func<T, T2, T3, T4, T5, bool>> expressionJoinOn, string t5TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T5>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Inner Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5>(this, JoinStr, SqlParams, t5TableSuffix);
        }

        public IQingQuery<T, T2, T3, T4> GroupBy<TResult>(Expression<Func<T, T2, T3, T4, TResult>> expression)
        {
            return ExpressionResolver.ResolveGroupByExpression(this, expression);
        }
    }

    #endregion

    #region T5

    internal class QingQuery<T, T2, T3, T4, T5> : IQingQuery<T, T2, T3, T4, T5>
    {
        internal DBConnectionItem dbConnection { set; get; }
        internal string WhereStr { get; set; } = string.Empty;
        internal string Fields { get; set; }
        internal string Limit { get; set; } = "";
        internal string OrderByStr { get; set; }
        internal string GroupByStr { set; get; }
        internal string JoinStr { get; set; }
        internal List<ParamItem> SqlParams = new List<ParamItem>();
        internal IDBEngine DBEngine { get; set; }
        internal string T1TableSuffix = null;
        internal string T2TableSuffix = null;
        internal string T3TableSuffix = null;
        internal string T4TableSuffix = null;
        internal string T5TableSuffix = null;


        public QingQuery(QingQuery<T, T2, T3, T4> edbq, string joinStr, List<ParamItem> sqlParams, string t5TableSuffix = null)
        {
            this.DBEngine = edbq.DBEngine;
            this.dbConnection = edbq.dbConnection;
            this.Fields = edbq.Fields;
            this.Limit = edbq.Limit;
            this.OrderByStr = edbq.OrderByStr;
            this.JoinStr += joinStr;
            this.SqlParams = sqlParams;
            this.T1TableSuffix = edbq.T1TableSuffix;
            this.T2TableSuffix = edbq.T2TableSuffix;
            this.T3TableSuffix = edbq.T3TableSuffix;
            this.T4TableSuffix = edbq.T4TableSuffix;
            this.T5TableSuffix = t5TableSuffix;
        }

        private string BuildSql()
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = Fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }

            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }
            if (!string.IsNullOrEmpty(OrderByStr))
            {
                sb_sql.Append($" Order By {OrderByStr.TrimStart(',')}");
            }
            if (!string.IsNullOrEmpty(Limit))
            {
                sb_sql.Append($"  {Limit}");
            }
            return sb_sql.ToString();
        }

        private string BuildSql_Custom_Fields_Func(string fields)
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }

            return sb_sql.ToString();
        }

        public IQingQuery<T, T2, T3, T4, T5> Select<TResult>(Expression<Func<T, T2, T3, T4, T5, TResult>> expression)
        {
            return ExpressionResolver.ResolveSelectExpression(this, expression);
        }

        public IQingQuery<T, T2, T3, T4, T5> Where(Expression<Func<T, T2, T3, T4, T5, bool>> expression)
        {
            return ExpressionResolver.ResolveWhereExpression(this, expression);
        }
        public IQingQuery<T, T2, T3, T4, T5> OrderBy<TResult>(Expression<Func<T, T2, T3, T4, T5, TResult>> expression, bool Desc = false)
        {
            return ExpressionResolver.ResolveOrderByExpression(this, expression, Desc);
        }

        public TResult QueryFirst<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            return DBEngine.QueryFirst<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> QueryFirstAsync<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            return DBEngine.QueryFirstAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public IEnumerable<TResult> QueryWithPage<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAll<TResult>();
        }
        public IAsyncEnumerable<TResult> QueryWithPageAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAllAsync<TResult>();
        }
        public PageResult<TResult> QueryPages<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = Count();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
                pageResult.ArrayData = QueryWithPage<TResult>(pageNum, pageSize);
            }
            return pageResult;
        }

        public async Task<PageResult<TResult>> QueryPagesAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = await CountAsync();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
#if NET7_0_OR_GREATER
                pageResult.ArrayData = QueryWithPageAsync<TResult>(pageNum, pageSize).ToBlockingEnumerable();
#else
                List<TResult> list = new List<TResult>();
                await foreach (var item in QueryWithPageAsync<TResult>(pageNum, pageSize))
                {
                    list.Add(item);
                }
                pageResult.ArrayData = list;
#endif
            }
            return pageResult;
        }

        public IEnumerable<TResult> QueryAll<TResult>()
        {
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            return DBEngine.Query<TResult>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<TResult> QueryAllAsync<TResult>()
        {
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            return DBEngine.QueryAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Max<TResult>(Expression<Func<T, T2, T3, T4, T5, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }

        public Task<TResult> MaxAsync<TResult>(Expression<Func<T, T2, T3, T4, T5, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Min<TResult>(Expression<Func<T, T2, T3, T4, T5, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> MinAsync<TResult>(Expression<Func<T, T2, T3, T4, T5, TResult>> expression)
        {
            ExpressionResolver.ResolveMinExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);

        }
        public int Count()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            return DBEngine.ExecuteScalar<int>(dbConnection, sql, SqlParams);

        }
        public Task<int> CountAsync()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            return DBEngine.ExecuteScalarAsync<int>(dbConnection, sql, SqlParams);

        }
        public IQingQuery<T, T2, T3, T4, T5, T6> RightJoin<T6>(Expression<Func<T, T2, T3, T4, T5, T6, bool>> expressionJoinOn, string t6TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T6>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Right Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6>(this, JoinStr, SqlParams, t6TableSuffix);
        }
        public IQingQuery<T, T2, T3, T4, T5, T6> InnerJoin<T6>(Expression<Func<T, T2, T3, T4, T5, T6, bool>> expressionJoinOn, string t6TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T6>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Inner Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6>(this, JoinStr, jps.Params, t6TableSuffix);
        }

        public IQingQuery<T, T2, T3, T4, T5, T6> LeftJoin<T6>(Expression<Func<T, T2, T3, T4, T5, T6, bool>> expressionJoinOn, string t6TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T6>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Left Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6>(this, JoinStr, jps.Params, t6TableSuffix);
        }

        public IQingQuery<T, T2, T3, T4, T5> GroupBy<TResult>(Expression<Func<T, T2, T3, T4, T5, TResult>> expression)
        {
            return ExpressionResolver.ResolveGroupByExpression(this, expression);
        }
    }

    #endregion

    #region T6

    internal class QingQuery<T, T2, T3, T4, T5, T6> : IQingQuery<T, T2, T3, T4, T5, T6>
    {
        internal DBConnectionItem dbConnection { set; get; }
        internal string WhereStr { get; set; } = string.Empty;
        internal string Fields { get; set; }
        internal string Limit { get; set; } = "";
        internal string OrderByStr { get; set; }
        internal string GroupByStr { set; get; }
        internal string JoinStr { get; set; }
        internal List<ParamItem> SqlParams = new List<ParamItem>();
        internal IDBEngine DBEngine { get; set; }
        internal string T1TableSuffix = null;
        internal string T2TableSuffix = null;
        internal string T3TableSuffix = null;
        internal string T4TableSuffix = null;
        internal string T5TableSuffix = null;
        internal string T6TableSuffix = null;

        public QingQuery(QingQuery<T, T2, T3, T4, T5> edbq, string joinStr, List<ParamItem> sqlParams, string t6TableSuffix = null)
        {
            this.DBEngine = edbq.DBEngine;
            this.dbConnection = edbq.dbConnection;
            this.Fields = edbq.Fields;
            this.Limit = edbq.Limit;
            this.OrderByStr = edbq.OrderByStr;
            this.JoinStr += joinStr;
            this.SqlParams = sqlParams;
            this.T1TableSuffix = edbq.T1TableSuffix;
            this.T2TableSuffix = edbq.T2TableSuffix;
            this.T3TableSuffix = edbq.T3TableSuffix;
            this.T4TableSuffix = edbq.T4TableSuffix;
            this.T5TableSuffix = edbq.T5TableSuffix;
            this.T6TableSuffix = t6TableSuffix;
        }



        public IQingQuery<T, T2, T3, T4, T5, T6> OrderBy<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, TResult>> expression, bool Desc = false)
        {
            return ExpressionResolver.ResolveOrderByExpression(this, expression, Desc);
        }

        public TResult QueryFirst<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            return DBEngine.QueryFirst<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> QueryFirstAsync<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            return DBEngine.QueryFirstAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public IEnumerable<TResult> QueryWithPage<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAll<TResult>();
        }
        public IAsyncEnumerable<TResult> QueryWithPageAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAllAsync<TResult>();
        }
        public PageResult<TResult> QueryPages<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = Count();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
                pageResult.ArrayData = QueryWithPage<TResult>(pageNum, pageSize);
            }
            return pageResult;
        }

        public async Task<PageResult<TResult>> QueryPagesAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = await CountAsync();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
#if NET7_0_OR_GREATER
                pageResult.ArrayData = QueryWithPageAsync<TResult>(pageNum, pageSize).ToBlockingEnumerable();
#else
                List<TResult> list = new List<TResult>();
                await foreach (var item in QueryWithPageAsync<TResult>(pageNum, pageSize))
                {
                    list.Add(item);
                }
                pageResult.ArrayData = list;
#endif
            }
            return pageResult;
        }

        public IEnumerable<TResult> QueryAll<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            return DBEngine.Query<TResult>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<TResult> QueryAllAsync<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            return DBEngine.QueryAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Max<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }

        public Task<TResult> MaxAsync<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Min<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> MinAsync<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, TResult>> expression)
        {
            ExpressionResolver.ResolveMinExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);

        }
        public int Count()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            return DBEngine.ExecuteScalar<int>(dbConnection, sql, SqlParams);


        }
        public Task<int> CountAsync()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            return DBEngine.ExecuteScalarAsync<int>(dbConnection, sql, SqlParams);

        }
        public IQingQuery<T, T2, T3, T4, T5, T6, T7> InnerJoin<T7>(Expression<Func<T, T2, T3, T4, T5, T6, T7, bool>> expressionJoinOn, string t7TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T7>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Inner Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6, T7>(this, JoinStr, SqlParams, t7TableSuffix);
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7> LeftJoin<T7>(Expression<Func<T, T2, T3, T4, T5, T6, T7, bool>> expressionJoinOn, string t7TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T7>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Left Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6, T7>(this, JoinStr, SqlParams, t7TableSuffix);
        }
        public IQingQuery<T, T2, T3, T4, T5, T6, T7> RightJoin<T7>(Expression<Func<T, T2, T3, T4, T5, T6, T7, bool>> expressionJoinOn, string t7TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T7>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Right Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6, T7>(this, JoinStr, SqlParams, t7TableSuffix);
        }

        public IQingQuery<T, T2, T3, T4, T5, T6> Select<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, TResult>> expression)
        {
            return ExpressionResolver.ResolveSelectExpression(this, expression);

        }

        public IQingQuery<T, T2, T3, T4, T5, T6> Where(Expression<Func<T, T2, T3, T4, T5, T6, bool>> expression)
        {
            return ExpressionResolver.ResolveWhereExpression(this, expression);
        }

        private string BuildSql()
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = Fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }

            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }
            if (!string.IsNullOrEmpty(OrderByStr))
            {
                sb_sql.Append($" Order By {OrderByStr.TrimStart(',')}");
            }
            if (!string.IsNullOrEmpty(Limit))
            {
                sb_sql.Append($"  {Limit}");
            }
            return sb_sql.ToString();
        }

        private string BuildSql_Custom_Fields_Func(string fields)
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }

            return sb_sql.ToString();
        }

        public IQingQuery<T, T2, T3, T4, T5, T6> GroupBy<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, TResult>> expression)
        {
            return ExpressionResolver.ResolveGroupByExpression(this, expression);
        }
    }

    #endregion

    #region T7

    internal class QingQuery<T, T2, T3, T4, T5, T6, T7> : IQingQuery<T, T2, T3, T4, T5, T6, T7>
    {
        internal DBConnectionItem dbConnection { set; get; }
        internal string WhereStr { get; set; } = string.Empty;
        internal string Fields { get; set; }
        internal string Limit { get; set; } = "";
        internal string OrderByStr { get; set; }
        internal string GroupByStr { set; get; }
        internal string JoinStr { get; set; }

        internal List<ParamItem> SqlParams = new List<ParamItem>();
        internal IDBEngine DBEngine { get; set; }
        internal string T1TableSuffix = null;
        internal string T2TableSuffix = null;
        internal string T3TableSuffix = null;
        internal string T4TableSuffix = null;
        internal string T5TableSuffix = null;
        internal string T6TableSuffix = null;
        internal string T7TableSuffix = null;

        public QingQuery(QingQuery<T, T2, T3, T4, T5, T6> edbq, string joinStr, List<ParamItem> sqlParams, string t7TableSuffix = null)
        {
            this.DBEngine = edbq.DBEngine;
            this.dbConnection = edbq.dbConnection;
            this.Fields = edbq.Fields;
            this.Limit = edbq.Limit;
            this.OrderByStr = edbq.OrderByStr;
            this.JoinStr += joinStr;
            this.SqlParams = sqlParams;
            this.T1TableSuffix = edbq.T1TableSuffix;
            this.T2TableSuffix = edbq.T2TableSuffix;
            this.T3TableSuffix = edbq.T3TableSuffix;
            this.T4TableSuffix = edbq.T4TableSuffix;
            this.T5TableSuffix = edbq.T5TableSuffix;
            this.T6TableSuffix = edbq.T6TableSuffix;
            this.T7TableSuffix = t7TableSuffix;
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7> OrderBy<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, TResult>> expression, bool Desc = false)
        {
            return ExpressionResolver.ResolveOrderByExpression(this, expression, Desc);
        }

        public TResult QueryFirst<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            return DBEngine.QueryFirst<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> QueryFirstAsync<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            return DBEngine.QueryFirstAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public IEnumerable<TResult> QueryWithPage<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAll<TResult>();
        }
        public IAsyncEnumerable<TResult> QueryWithPageAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAllAsync<TResult>();
        }
        public PageResult<TResult> QueryPages<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = Count();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
                pageResult.ArrayData = QueryWithPage<TResult>(pageNum, pageSize);
            }
            return pageResult;
        }

        public async Task<PageResult<TResult>> QueryPagesAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = await CountAsync();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
#if NET7_0_OR_GREATER
                pageResult.ArrayData = QueryWithPageAsync<TResult>(pageNum, pageSize).ToBlockingEnumerable();
#else
                List<TResult> list = new List<TResult>();
                await foreach (var item in QueryWithPageAsync<TResult>(pageNum, pageSize))
                {
                    list.Add(item);
                }
                pageResult.ArrayData = list;
#endif
            }
            return pageResult;
        }

        public IEnumerable<TResult> QueryAll<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            return DBEngine.Query<TResult>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<TResult> QueryAllAsync<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            return DBEngine.QueryAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Max<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }

        public Task<TResult> MaxAsync<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Min<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> MinAsync<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, TResult>> expression)
        {
            ExpressionResolver.ResolveMinExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);

        }
        public int Count()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            return DBEngine.ExecuteScalar<int>(dbConnection, sql, SqlParams);


        }
        public Task<int> CountAsync()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            return DBEngine.ExecuteScalarAsync<int>(dbConnection, sql, SqlParams);

        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7> Select<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, TResult>> expression)
        {
            return ExpressionResolver.ResolveSelectExpression(this, expression);
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7> Where(Expression<Func<T, T2, T3, T4, T5, T6, T7, bool>> expression)
        {
            return ExpressionResolver.ResolveWhereExpression(this, expression);
        }
        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8> InnerJoin<T8>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, bool>> expressionJoinOn, string t8TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T8>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Inner Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6, T7, T8>(this, JoinStr, SqlParams, t8TableSuffix);
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8> LeftJoin<T8>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, bool>> expressionJoinOn, string t8TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T8>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Left Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6, T7, T8>(this, JoinStr, SqlParams, t8TableSuffix);
        }
        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8> RightJoin<T8>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, bool>> expressionJoinOn, string t8TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T8>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Right Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6, T7, T8>(this, JoinStr, SqlParams, t8TableSuffix);
        }

        private string BuildSql()
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = Fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }

            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }
            if (!string.IsNullOrEmpty(OrderByStr))
            {
                sb_sql.Append($" Order By {OrderByStr.TrimStart(',')}");
            }
            if (!string.IsNullOrEmpty(Limit))
            {
                sb_sql.Append($"  {Limit}");
            }
            return sb_sql.ToString();
        }

        private string BuildSql_Custom_Fields_Func(string fields)
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }

            return sb_sql.ToString();
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7> GroupBy<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, TResult>> expression)
        {
            return ExpressionResolver.ResolveGroupByExpression(this, expression);
        }
    }

    #endregion

    #region T8

    internal class QingQuery<T, T2, T3, T4, T5, T6, T7, T8> : IQingQuery<T, T2, T3, T4, T5, T6, T7, T8>
    {
        internal DBConnectionItem dbConnection { set; get; }
        internal string WhereStr { get; set; } = string.Empty;
        internal string Fields { get; set; }
        internal string Limit { get; set; } = "";
        internal string OrderByStr { get; set; }
        internal string GroupByStr { set; get; }
        internal string JoinStr { get; set; }

        internal List<ParamItem> SqlParams = new List<ParamItem>();
        internal IDBEngine DBEngine { get; set; }
        internal string T1TableSuffix = null;
        internal string T2TableSuffix = null;
        internal string T3TableSuffix = null;
        internal string T4TableSuffix = null;
        internal string T5TableSuffix = null;
        internal string T6TableSuffix = null;
        internal string T7TableSuffix = null;
        internal string T8TableSuffix = null;


        public QingQuery(QingQuery<T, T2, T3, T4, T5, T6, T7> edbq, string joinStr, List<ParamItem> sqlParams, string t8TableSuffix = null)
        {
            this.DBEngine = edbq.DBEngine;
            this.dbConnection = edbq.dbConnection;
            this.Fields = edbq.Fields;
            this.Limit = edbq.Limit;
            this.OrderByStr = edbq.OrderByStr;
            this.JoinStr += joinStr;
            this.SqlParams = sqlParams;
            this.T1TableSuffix = edbq.T1TableSuffix;
            this.T2TableSuffix = edbq.T2TableSuffix;
            this.T3TableSuffix = edbq.T3TableSuffix;
            this.T4TableSuffix = edbq.T4TableSuffix;
            this.T5TableSuffix = edbq.T5TableSuffix;
            this.T6TableSuffix = edbq.T6TableSuffix;
            this.T7TableSuffix = edbq.T7TableSuffix;
            this.T8TableSuffix = t8TableSuffix;
        }

        public TResult QueryFirst<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            return DBEngine.QueryFirst<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> QueryFirstAsync<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            return DBEngine.QueryFirstAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public IEnumerable<TResult> QueryWithPage<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAll<TResult>();
        }
        public IAsyncEnumerable<TResult> QueryWithPageAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAllAsync<TResult>();
        }
        public PageResult<TResult> QueryPages<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = Count();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
                pageResult.ArrayData = QueryWithPage<TResult>(pageNum, pageSize);
            }

            return pageResult;
        }

        public async Task<PageResult<TResult>> QueryPagesAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = await CountAsync();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
#if NET7_0_OR_GREATER
                pageResult.ArrayData = QueryWithPageAsync<TResult>(pageNum, pageSize).ToBlockingEnumerable();
#else
                List<TResult> list = new List<TResult>();
                await foreach (var item in QueryWithPageAsync<TResult>(pageNum, pageSize))
                {
                    list.Add(item);
                }
                pageResult.ArrayData = list;
#endif
            }
            return pageResult;
        }

        public IEnumerable<TResult> QueryAll<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            return DBEngine.Query<TResult>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<TResult> QueryAllAsync<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            return DBEngine.QueryAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Max<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }

        public Task<TResult> MaxAsync<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Min<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> MinAsync<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, TResult>> expression)
        {
            ExpressionResolver.ResolveMinExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);

        }
        public int Count()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            return DBEngine.ExecuteScalar<int>(dbConnection, sql, SqlParams);

        }
        public Task<int> CountAsync()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            return DBEngine.ExecuteScalarAsync<int>(dbConnection, sql, SqlParams);

        }
        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8> OrderBy<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, TResult>> expression, bool Desc = false)
        {
            return ExpressionResolver.ResolveOrderByExpression(this, expression, Desc);
        }
        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8> Select<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, TResult>> expression)
        {
            return ExpressionResolver.ResolveSelectExpression(this, expression);
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8> Where(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, bool>> expression)
        {
            return ExpressionResolver.ResolveWhereExpression(this, expression);
        }
        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> RightJoin<T9>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, bool>> expressionJoinOn, string t9TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T9>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Right Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9>(this, JoinStr, SqlParams, t9TableSuffix);
        }
        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> InnerJoin<T9>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, bool>> expressionJoinOn, string t9TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T9>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Inner Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9>(this, JoinStr, SqlParams, t9TableSuffix);
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> LeftJoin<T9>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, bool>> expressionJoinOn, string t9TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T9>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Left Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9>(this, JoinStr, SqlParams, t9TableSuffix);
        }

        private string BuildSql()
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = Fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }

            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }
            if (!string.IsNullOrEmpty(OrderByStr))
            {
                sb_sql.Append($" Order By {OrderByStr.TrimStart(',')}");
            }
            if (!string.IsNullOrEmpty(Limit))
            {
                sb_sql.Append($"  {Limit}");
            }
            return sb_sql.ToString();
        }

        private string BuildSql_Custom_Fields_Func(string fields)
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }

            return sb_sql.ToString();
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8> GroupBy<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, TResult>> expression)
        {
            return ExpressionResolver.ResolveGroupByExpression(this, expression);
        }
    }

    #endregion

    #region T9

    internal class QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> : IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9>
    {
        internal DBConnectionItem dbConnection { set; get; }
        internal string WhereStr { get; set; } = string.Empty;
        internal string Fields { get; set; }
        internal string Limit { get; set; } = "";
        internal string OrderByStr { get; set; }
        internal string GroupByStr { set; get; }
        internal string JoinStr { get; set; }

        internal List<ParamItem> SqlParams = new List<ParamItem>();
        internal IDBEngine DBEngine { get; set; }
        internal string T1TableSuffix = null;
        internal string T2TableSuffix = null;
        internal string T3TableSuffix = null;
        internal string T4TableSuffix = null;
        internal string T5TableSuffix = null;
        internal string T6TableSuffix = null;
        internal string T7TableSuffix = null;
        internal string T8TableSuffix = null;
        internal string T9TableSuffix = null;


        public QingQuery(QingQuery<T, T2, T3, T4, T5, T6, T7, T8> edbq, string joinStr, List<ParamItem> sqlParams, string t9TableSuffix = null)
        {
            this.DBEngine = edbq.DBEngine;
            this.dbConnection = edbq.dbConnection;
            this.Fields = edbq.Fields;
            this.Limit = edbq.Limit;
            this.OrderByStr = edbq.OrderByStr;
            this.JoinStr += joinStr;
            this.SqlParams = sqlParams;
            this.T1TableSuffix = edbq.T1TableSuffix;
            this.T2TableSuffix = edbq.T2TableSuffix;
            this.T3TableSuffix = edbq.T3TableSuffix;
            this.T4TableSuffix = edbq.T4TableSuffix;
            this.T5TableSuffix = edbq.T5TableSuffix;
            this.T6TableSuffix = edbq.T6TableSuffix;
            this.T7TableSuffix = edbq.T7TableSuffix;
            this.T8TableSuffix = edbq.T8TableSuffix;
            this.T9TableSuffix = t9TableSuffix;
        }
        public TResult QueryFirst<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            return DBEngine.QueryFirst<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> QueryFirstAsync<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            return DBEngine.QueryFirstAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public IEnumerable<TResult> QueryWithPage<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAll<TResult>();
        }
        public IAsyncEnumerable<TResult> QueryWithPageAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAllAsync<TResult>();
        }
        public PageResult<TResult> QueryPages<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = Count();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
                pageResult.ArrayData = QueryWithPage<TResult>(pageNum, pageSize);
            }
            return pageResult;
        }

        public async Task<PageResult<TResult>> QueryPagesAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = await CountAsync();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
#if NET7_0_OR_GREATER
                pageResult.ArrayData = QueryWithPageAsync<TResult>(pageNum, pageSize).ToBlockingEnumerable();
#else
                List<TResult> list = new List<TResult>();
                await foreach (var item in QueryWithPageAsync<TResult>(pageNum, pageSize))
                {
                    list.Add(item);
                }
                pageResult.ArrayData = list;
#endif
            }
            return pageResult;
        }

        public IEnumerable<TResult> QueryAll<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            return DBEngine.Query<TResult>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<TResult> QueryAllAsync<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            return DBEngine.QueryAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Max<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }

        public Task<TResult> MaxAsync<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Min<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> MinAsync<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>> expression)
        {
            ExpressionResolver.ResolveMinExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);

        }
        public int Count()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            return DBEngine.ExecuteScalar<int>(dbConnection, sql, SqlParams);

        }
        public Task<int> CountAsync()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            return DBEngine.ExecuteScalarAsync<int>(dbConnection, sql, SqlParams);

        }
        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> OrderBy<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>> expression, bool Desc = false)
        {
            return ExpressionResolver.ResolveOrderByExpression(this, expression, Desc);
        }


        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> Select<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>> expression)
        {
            return ExpressionResolver.ResolveSelectExpression(this, expression);
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> Where(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, bool>> expression)
        {
            return ExpressionResolver.ResolveWhereExpression(this, expression);
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> RightJoin<T10>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, bool>> expressionJoinOn,string  t10TableSuffix=null)
        {
            var nea = EntityCache.TryGetInfo<T10>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Right Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10>(this, JoinStr, SqlParams, t10TableSuffix);
        }
        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> InnerJoin<T10>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, bool>> expressionJoinOn, string t10TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T10>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Inner Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10>(this, JoinStr, SqlParams, t10TableSuffix);
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> LeftJoin<T10>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, bool>> expressionJoinOn, string t10TableSuffix = null)
        {
            var nea = EntityCache.TryGetInfo<T10>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            JoinStr += $" Left Join {dbConnection.SqlConnection.Database}.{nea.TableName} on ({jps.SqlStr})";

            return new QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10>(this, JoinStr, SqlParams, t10TableSuffix);
        }

        private string BuildSql()
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = Fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }

            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }
            if (!string.IsNullOrEmpty(OrderByStr))
            {
                sb_sql.Append($" Order By {OrderByStr.TrimStart(',')}");
            }
            if (!string.IsNullOrEmpty(Limit))
            {
                sb_sql.Append($"  {Limit}");
            }
            return sb_sql.ToString();
        }

        private string BuildSql_Custom_Fields_Func(string fields)
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }

            return sb_sql.ToString();
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> GroupBy<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>> expression)
        {
            return ExpressionResolver.ResolveGroupByExpression(this, expression);
        }
    }

    #endregion

    #region T10

    internal class QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> : IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10>
    {
        internal DBConnectionItem dbConnection { set; get; }
        internal string WhereStr { get; set; } = string.Empty;
        internal string Fields { get; set; }
        internal string Limit { get; set; } = "";
        internal string OrderByStr { get; set; }
        internal string GroupByStr { set; get; }
        internal string JoinStr { get; set; }

        internal List<ParamItem> SqlParams = new List<ParamItem>();
        internal IDBEngine DBEngine { get; set; }
        internal string T1TableSuffix = null;
        internal string T2TableSuffix = null;
        internal string T3TableSuffix = null;
        internal string T4TableSuffix = null;
        internal string T5TableSuffix = null;
        internal string T6TableSuffix = null;
        internal string T7TableSuffix = null;
        internal string T8TableSuffix = null;
        internal string T9TableSuffix = null;
        internal string T10TableSuffix = null;

        public QingQuery(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> edbq, string joinStr, List<ParamItem> sqlParams,string t10TableSuffix=null)
        {
            this.dbConnection = edbq.dbConnection;
            this.DBEngine = edbq.DBEngine;
            this.Fields = edbq.Fields;
            this.Limit = edbq.Limit;
            this.OrderByStr = edbq.OrderByStr;
            this.JoinStr += joinStr;
            this.SqlParams = sqlParams;
            this.T1TableSuffix = edbq.T1TableSuffix;
            this.T2TableSuffix = edbq.T2TableSuffix;
            this.T3TableSuffix = edbq.T3TableSuffix;
            this.T4TableSuffix = edbq.T4TableSuffix;
            this.T5TableSuffix = edbq.T5TableSuffix;
            this.T6TableSuffix = edbq.T6TableSuffix;
            this.T7TableSuffix = edbq.T7TableSuffix;
            this.T8TableSuffix = edbq.T8TableSuffix;
            this.T9TableSuffix = edbq.T9TableSuffix;
            this.T10TableSuffix = t10TableSuffix;
        }

        public TResult QueryFirst<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            sql = EntityCache.RestoreTableName<T10>(sql,T10TableSuffix);
            return DBEngine.QueryFirst<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> QueryFirstAsync<TResult>()
        {
            Limit = $"limit 1";
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            sql = EntityCache.RestoreTableName<T10>(sql, T10TableSuffix);
            return DBEngine.QueryFirstAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public IEnumerable<TResult> QueryWithPage<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAll<TResult>();
        }
        public IAsyncEnumerable<TResult> QueryWithPageAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            Limit = $"limit {pageSize * (pageNum - 1)},{pageSize}";
            return QueryAllAsync<TResult>();
        }
        public PageResult<TResult> QueryPages<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = Count();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
                pageResult.ArrayData = QueryWithPage<TResult>(pageNum, pageSize);
            }
            return pageResult;
        }

        public async Task<PageResult<TResult>> QueryPagesAsync<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = await CountAsync();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
#if NET7_0_OR_GREATER
                pageResult.ArrayData = QueryWithPageAsync<TResult>(pageNum, pageSize).ToBlockingEnumerable();
#else
                List<TResult> list = new List<TResult>();
                await foreach (var item in QueryWithPageAsync<TResult>(pageNum, pageSize))
                {
                    list.Add(item);
                }
                pageResult.ArrayData = list;
#endif
            }
            return pageResult;
        }

        public IEnumerable<TResult> QueryAll<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            sql = EntityCache.RestoreTableName<T10>(sql, T10TableSuffix);
            return DBEngine.Query<TResult>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<TResult> QueryAllAsync<TResult>()
        {
            var sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            sql = EntityCache.RestoreTableName<T10>(sql, T10TableSuffix);
            return DBEngine.QueryAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Max<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            sql = EntityCache.RestoreTableName<T10>(sql, T10TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }

        public Task<TResult> MaxAsync<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            sql = EntityCache.RestoreTableName<T10>(sql, T10TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);
        }
        public TResult Min<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            sql = EntityCache.RestoreTableName<T10>(sql, T10TableSuffix);
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }
        public Task<TResult> MinAsync<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>> expression)
        {
            ExpressionResolver.ResolveMinExpression(this, expression);
            string sql = BuildSql();
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            sql = EntityCache.RestoreTableName<T10>(sql, T10TableSuffix);
            return DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);

        }
        public int Count()
        {
            string sql = "";
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            sql = EntityCache.RestoreTableName<T10>(sql, T10TableSuffix);
            return DBEngine.ExecuteScalar<int>(dbConnection, sql, SqlParams);

        }
        public Task<int> CountAsync()
        {
            string sql = BuildSql_Custom_Fields_Func("Count(*)");
            sql = EntityCache.RestoreTableName<T>(sql, T1TableSuffix);
            sql = EntityCache.RestoreTableName<T2>(sql, T2TableSuffix);
            sql = EntityCache.RestoreTableName<T3>(sql, T3TableSuffix);
            sql = EntityCache.RestoreTableName<T4>(sql, T4TableSuffix);
            sql = EntityCache.RestoreTableName<T5>(sql, T5TableSuffix);
            sql = EntityCache.RestoreTableName<T6>(sql, T6TableSuffix);
            sql = EntityCache.RestoreTableName<T7>(sql, T7TableSuffix);
            sql = EntityCache.RestoreTableName<T8>(sql, T8TableSuffix);
            sql = EntityCache.RestoreTableName<T9>(sql, T9TableSuffix);
            sql = EntityCache.RestoreTableName<T10>(sql, T10TableSuffix);
            return DBEngine.ExecuteScalarAsync<int>(dbConnection, sql, SqlParams);

        }
        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> OrderBy<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>> expression, bool Desc = false)
        {
            return ExpressionResolver.ResolveOrderByExpression(this, expression, Desc);
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> Select<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>> expression)
        {
            return ExpressionResolver.ResolveSelectExpression(this, expression);
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> Where(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, bool>> expression)
        {
            return ExpressionResolver.ResolveWhereExpression(this, expression);
        }
        private string BuildSql()
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = Fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }
            if (!string.IsNullOrEmpty(OrderByStr))
            {
                sb_sql.Append($" Order By {OrderByStr.TrimStart(',')}");
            }
            if (!string.IsNullOrEmpty(Limit))
            {
                sb_sql.Append($"  {Limit}");
            }
            return sb_sql.ToString();
        }

        private string BuildSql_Custom_Fields_Func(string fields)
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {dbConnection.SqlConnection.Database}.{nea.TableName}");
            sb_sql.Append(JoinStr);
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr}");
            }

            return sb_sql.ToString();
        }

        public IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> GroupBy<TResult>(Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>> expression)
        {
            return ExpressionResolver.ResolveGroupByExpression(this, expression);
        }
    }

    #endregion

}
