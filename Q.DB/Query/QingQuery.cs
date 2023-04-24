/*************************************************************************************
 *
 * 文 件 名:   QingQuery.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/13 11:35
 * 
 * ======================================
 * 历史更新记录
 * 版本：V          修改时间：         修改人：
 * 修改内容：
 * ======================================
 * 
*************************************************************************************/

using System.Linq.Expressions;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Q.DB.Interface;
using Q.DB.Models;
using Q.DB.Models.Filter.Filter;

namespace Q.DB.Query
{
    public class QingQuery<T> : IQingQuery<T>
    {
        public string WhereStr, GroupByStr, Fields, OrderByStr, TableSuffix;
        public string Limit = "";
        public List<ParamItem> SqlParams = new List<ParamItem>();
        public DBConnectionItem dbConnection;
        public IDBEngine DBEngine;
        public QingQuery(IDBEngine dbengine, DBConnectionItem connection, string tableSuffix = null)
        {
            DBEngine = dbengine;
            dbConnection = connection;
            TableSuffix = tableSuffix;
        }
        public TResult Avg<TResult>(Expression<Func<T, TResult>> expression)
        {
            var ps = ExpressionResolver.ResolveExpression(expression.Body, DBEngine);
            string sql = BuildSql_Custom_Fields_Func("Avg(" + ps.SqlStr + ")");
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }

        public async Task<TResult> AvgAsync<TResult>(Expression<Func<T, TResult>> expression)
        {
            var ps = ExpressionResolver.ResolveExpression(expression.Body, DBEngine);
            string sql = BuildSql_Custom_Fields_Func("Avg(" + ps.SqlStr + ")");
            return await DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);
        }

        public ParamSql BuildFirstQuery()
        {
            ParamSql paraSql = new ParamSql();
            paraSql.SqlStr = BuildSql_Custom_LimitCount(1);
            paraSql.Params.AddRange(SqlParams);
            return paraSql;
        }

        public ParamSql BuildQuery()
        {
            ParamSql paraSql = new ParamSql();
            paraSql.SqlStr = BuildSql();
            paraSql.Params.AddRange(SqlParams);
            return paraSql;
        }

        public int Count()
        {
            long st = System.Environment.TickCount64;
            string sql;
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            var result = DBEngine.ExecuteScalar<int>(dbConnection, sql, SqlParams);
            SqlLogUtil.SendLog(LogType.SQL, sql, System.Environment.TickCount64 - st);
            return result;
        }

        public async Task<int> CountAsync()
        {
            long st = System.Environment.TickCount64;
            string sql;
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            var result = await DBEngine.ExecuteScalarAsync<int>(dbConnection, sql, SqlParams);
            SqlLogUtil.SendLog(LogType.SQL, sql, System.Environment.TickCount64 - st);
            return result;
        }
        public long Count64()
        {
            long st = System.Environment.TickCount64;
            string sql;
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            var result = DBEngine.ExecuteScalar<long>(dbConnection, sql, SqlParams);
            SqlLogUtil.SendLog(LogType.SQL, sql, System.Environment.TickCount64 - st);
            return result;
        }

        public async Task<long> Count64Async()
        {
            long st = System.Environment.TickCount64;
            string sql;
            if (string.IsNullOrEmpty(GroupByStr))
            {
                sql = BuildSql_Custom_Fields_Func("Count(*)");
            }
            else
            {
                sql = $"select Count(*) from ({BuildSql_Custom_Fields_Func("Count(*)")}) as TempTable";
            }
            var result = await DBEngine.ExecuteScalarAsync<long>(dbConnection, sql, SqlParams);
            SqlLogUtil.SendLog(LogType.SQL, sql, System.Environment.TickCount64 - st);
            return result;
        }

        public IQingQuery<T> GroupBy<TResult>(Expression<Func<T, TResult>> expression)
        {
            return ExpressionResolver.ResolveGroupByExpression(this, expression);

        }

        public IQingQuery<T, T2> InnerJoin<T2>(Expression<Func<T, T2, bool>> expressionJoinOn)
        {
            var tableName = EntityCache.TryGetTableName<T2>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            string joinStr = $" Inner Join {DBEngine.EscapeStr(dbConnection.SqlConnection.Database, tableName)} on ({jps.SqlStr})";

            return new QingQuery<T, T2>(this, joinStr, SqlParams);
        }

        public IQingQuery<T, T2> LeftJoin<T2>(Expression<Func<T, T2, bool>> expressionJoinOn)
        {
            var tableName = EntityCache.TryGetTableName<T2>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            string joinStr = $" Left Join {DBEngine.EscapeStr(dbConnection.SqlConnection.Database, tableName)} on ({jps.SqlStr})";

            return new QingQuery<T, T2>(this, joinStr, SqlParams);
        }

        public TResult Max<TResult>(Expression<Func<T, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }

        public async Task<TResult> MaxAsync<TResult>(Expression<Func<T, TResult>> expression)
        {
            ExpressionResolver.ResolveMaxExpression(this, expression);
            string sql = BuildSql();
            return await DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);
        }

        public TResult Min<TResult>(Expression<Func<T, TResult>> expression)
        {
            ExpressionResolver.ResolveMinExpression(this, expression);
            string sql = BuildSql();
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }

        public async Task<TResult> MinAsync<TResult>(Expression<Func<T, TResult>> expression)
        {
            ExpressionResolver.ResolveMinExpression(this, expression);
            string sql = BuildSql();
            return await DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);
        }

        public IQingQuery<T> OrderBy<TResult>(Expression<Func<T, TResult>> expression)
        {
            return ExpressionResolver.ResolveOrderByExpression(this, expression);

        }

        public IQingQuery<T> OrderBy(string fieldstr, bool desc = false)
        {
            string[] fields = fieldstr.Split(new char[] { ',', '，', '|' });
            foreach (var field in fields)
            {
                this.OrderByStr += $",{field}{(desc ? " Desc" : "")}";
            }
            return this;
        }

        public IQingQuery<T> OrderByDesc<TResult>(Expression<Func<T, TResult>> expression)
        {
            return ExpressionResolver.ResolveOrderByExpression(this, expression, true);
        }

        public IEnumerable<T> QueryAll()
        {
            string sql = BuildSql();
            return DBEngine.Query<T>(dbConnection, sql, SqlParams);
        }

        public IEnumerable<TResult> QueryAll<TResult>()
        {
            string sql = BuildSql();
            return DBEngine.Query<TResult>(dbConnection, sql, SqlParams);
        }

        public IEnumerable<T> QueryAll(Expression<Func<T, bool>> expression)
        {
            ExpressionResolver.ResolveWhereExpression(this, expression);
            return QueryAll();
        }

        public IAsyncEnumerable<T> QueryAllAsync()
        {
            string sql = BuildSql();
            return DBEngine.QueryAsync<T>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<TResult> QueryAllAsync<TResult>()
        {
            string sql = BuildSql();
            return DBEngine.QueryAsync<TResult>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<T> QueryAllAsync(Expression<Func<T, bool>> expression)
        {
            ExpressionResolver.ResolveWhereExpression(this, expression);
            return QueryAllAsync();
        }

        public T QueryFirst()
        {
            string sql = BuildSql_Custom_LimitCount(1);
            return DBEngine.QueryFirst<T>(dbConnection, sql, SqlParams);
        }

        public TResult QueryFirst<TResult>()
        {
            string sql = BuildSql_Custom_LimitCount(1);
            return DBEngine.QueryFirst<TResult>(dbConnection, sql, SqlParams);
        }

        public T QueryFirst(Expression<Func<T, bool>> expression)
        {
            ExpressionResolver.ResolveWhereExpression(this, expression);
            return QueryFirst();
        }

        public async Task<T> QueryFirstAsync()
        {
            string sql = BuildSql_Custom_LimitCount(1);
            return await DBEngine.QueryFirstAsync<T>(dbConnection, sql, SqlParams);
        }

        public async Task<TResult> QueryFirstAsync<TResult>()
        {
            string sql = BuildSql_Custom_LimitCount(1);
            return await DBEngine.QueryFirstAsync<TResult>(dbConnection, sql, SqlParams);
        }

        public async Task<T> QueryFirstAsync(Expression<Func<T, bool>> expression)
        {
            ExpressionResolver.ResolveWhereExpression(this, expression);
            return await QueryFirstAsync();
        }

        public PageResult<T> QueryPages(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<T> pageResult = new PageResult<T>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = Count64();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<T>();
            }
            else
            {
                pageResult.ArrayData = QueryWithPage(pageNum, pageSize);
            }
            return pageResult;
        }

        public PageResult<TResult> QueryPages<TResult>(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = Count64();
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

        public async Task<PageResult<T>> QueryPagesAsync(int PageNum, int PageSize)
        {
            int pageNum = PageNum > 0 ? PageNum : 1;
            int pageSize = PageSize > 0 ? PageSize : 1;
            PageResult<T> pageResult = new PageResult<T>();
            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.TotalCount = await Count64Async();
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<T>();
            }
            else
            {
#if NET7_0_OR_GREATER
                pageResult.ArrayData = QueryWithPageAsync<T>(pageNum, pageSize).ToBlockingEnumerable();
#else

                List<T> list = new List<T>();
                await foreach (var item in QueryWithPageAsync(pageNum, pageSize))
                {
                    list.Add(item);
                }
                pageResult.ArrayData = list;

#endif
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
            pageResult.TotalCount = await Count64Async();

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

        public IEnumerable<T> QueryTop(int top = 1)
        {
            string sql = BuildSql_Custom_LimitCount(top);
            return DBEngine.Query<T>(dbConnection, sql, SqlParams);
        }

        public IEnumerable<TResult> QueryTop<TResult>(int top = 1)
        {
            string sql = BuildSql_Custom_LimitCount(top);
            return DBEngine.Query<TResult>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<T> QueryTopAsync(int top = 1)
        {
            string sql = BuildSql_Custom_LimitCount(top);
            return DBEngine.QueryAsync<T>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<TResult> QueryTopAsync<TResult>(int top = 1)
        {
            string sql = BuildSql_Custom_LimitCount(top);
            return DBEngine.QueryAsync<TResult>(dbConnection, sql, SqlParams);
        }

        public IEnumerable<T> QueryWithPage(int pageNum, int pageSize)
        {
            var sql = BuildSql_Custom_LimitPage(pageNum, pageSize);
            return DBEngine.Query<T>(dbConnection, sql, SqlParams);

        }

        public IEnumerable<TResult> QueryWithPage<TResult>(int pageNum, int pageSize)
        {
            var sql = BuildSql_Custom_LimitPage(pageNum, pageSize);
            return DBEngine.Query<TResult>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<T> QueryWithPageAsync(int pageNum, int pageSize)
        {
            var sql = BuildSql_Custom_LimitPage(pageNum, pageSize);
            return DBEngine.QueryAsync<T>(dbConnection, sql, SqlParams);
        }

        public IAsyncEnumerable<TResult> QueryWithPageAsync<TResult>(int pageNum, int pageSize)
        {
            var sql = BuildSql_Custom_LimitPage(pageNum, pageSize);
            return DBEngine.QueryAsync<TResult>(dbConnection, sql, SqlParams);
        }

        public int Remove(Expression<Func<T, bool>> expression)
        {
            ExpressionResolver.ResolveWhereExpression(this, expression);
            var tableName = EntityCache.TryGetTableName<T>();

            //string sql = $"Delete from {DBEngine.EscapeStr(dbConnection.SqlConnection.Database, tableName + QDBTools.ConvertSuffixTableName(TableSuffix))} where {WhereStr} ";
            string dbTable = DBEngine.EscapeStr(dbConnection.SqlConnection.Database, tableName + QDBTools.ConvertSuffixTableName(TableSuffix));
            string sql = DBEngine.CreateDelSql(dbTable, WhereStr,true);
            if (!string.IsNullOrEmpty(TableSuffix))
            {
                string oldTableName = DBEngine.EscapeStr(tableName);
                string newTableName = DBEngine.EscapeStr(tableName + QDBTools.ConvertSuffixTableName(TableSuffix));
                sql = sql.Replace(oldTableName, newTableName);
            }

            return DBEngine.ExecuteNonQuery(dbConnection, sql, SqlParams);
        }

        public async Task<int> RemoveAsync(Expression<Func<T, bool>> expression)
        {
            ExpressionResolver.ResolveWhereExpression(this, expression);
            var tableName = EntityCache.TryGetTableName<T>();

            // string sql = $"Delete from {DBEngine.EscapeStr(dbConnection.SqlConnection.Database, tableName + QDBTools.ConvertSuffixTableName(TableSuffix))} where {WhereStr} ";
            string dbTable = DBEngine.EscapeStr(dbConnection.SqlConnection.Database, tableName + QDBTools.ConvertSuffixTableName(TableSuffix));
            string sql = DBEngine.CreateDelSql(dbTable, WhereStr,false);
            return await DBEngine.ExecuteNonQueryAsync(dbConnection, sql, SqlParams);
        }



        public IQingQuery<T> ResolveFilterCondition(SqlFilter fc)
        {
            var nea = EntityCache.TryGetInfo<T>();

            if (fc.OrWhere.Count > 0)
            {
                List<ParamSql> orSql = new List<ParamSql>();
                foreach (var item in fc.OrWhere)
                {
                    ParamSql ps = new ParamSql();
                    if (nea.PropertyInfos.ContainsKey(item.FieldName))
                    {
                        (string key, string skey) = DBEngine.EscapeParamKey();
                        ps.SqlStr = $"({item.FieldName} {item.Condition} {skey})";
                        ps.Params.Add(new ParamItem(key, item.Value));
                        orSql.Add(ps);
                    }
                }
                if (orSql.Count > 0)
                {
                    this.WhereStr += string.IsNullOrEmpty(this.WhereStr) ? "" : " and ";
                    List<string> sqlWhere = new List<string>();
                    orSql.ForEach(x =>
                    {
                        this.SqlParams.AddRange(x.Params);
                        sqlWhere.Add(x.SqlStr);
                    });
                    this.WhereStr += $"({string.Join(" or ", sqlWhere)})";
                }
            }

            if (fc.AndWhere.Count > 0)
            {
                foreach (var item in fc.AndWhere)
                {
                    if (nea.PropertyInfos.ContainsKey(item.FieldName))
                    {
                        this.WhereStr += string.IsNullOrEmpty(this.WhereStr) ? "" : " and ";
                        (string key, string skey) = DBEngine.EscapeParamKey();
                        this.WhereStr += $"({item.FieldName} {item.Condition} {skey})";
                        this.SqlParams.Add(new ParamItem(key, item.Value));
                    }
                }
            }

            if (fc.Orders.Count > 0)
            {
                foreach (var item in fc.Orders)
                {
                    if (nea.PropertyInfos.ContainsKey(item.FieldName))
                    {
                        this.OrderByStr += $",{item.FieldName}{(item.Desc ? " Desc" : "")}";
                    }
                }
            }
            return this;
        }

        public IQingQuery<T, T2> RightJoin<T2>(Expression<Func<T, T2, bool>> expressionJoinOn)
        {
            var tableName = EntityCache.TryGetTableName<T2>();
            var jps = ExpressionResolver.ResolveJoinExpression(this, expressionJoinOn);
            SqlParams.AddRange(jps.Params);
            string joinStr = $" Right Join {DBEngine.EscapeStr(dbConnection.SqlConnection.Database, tableName)} on ({jps.SqlStr})";

            return new QingQuery<T, T2>(this, joinStr, SqlParams);
        }
        public QingUnionQuery<TResult> UnionSelect<TResult>(Expression<Func<T, TResult>> expressionNew, Expression<Func<T, bool>> expressionWhere)
        {
            ExpressionResolver.ResolveWhereExpression(this, expressionWhere);
            var result = ExpressionResolver.ResolveExpression(expressionNew.Body, DBEngine);
            Fields = result.SqlStr;
            SqlParams.AddRange(result.Params);
            var sql = BuildSql();
            return new QingUnionQuery<TResult>(DBEngine, dbConnection, sql, SqlParams);
        }

        public IQingQuery<T> Select<TResult>(Expression<Func<T, TResult>> expression)
        {
            return ExpressionResolver.ResolveSelectExpression(this, expression);
        }

        public IQingQuery<T> SelectExcept<TResult>(Expression<Func<T, TResult>> expression)
        {
            return ExpressionResolver.ResolveSelectExceptExpression(this, expression);
        }

        public TResult Sum<TResult>(Expression<Func<T, TResult>> expression)
        {
            var ps = ExpressionResolver.ResolveExpression(expression.Body, DBEngine);
            string sql = BuildSql_Custom_Fields_Func("Sum(" + ps.SqlStr + ")");
            return DBEngine.ExecuteScalar<TResult>(dbConnection, sql, SqlParams);
        }

        public async Task<TResult> SumAsync<TResult>(Expression<Func<T, TResult>> expression)
        {
            var ps = ExpressionResolver.ResolveExpression(expression.Body, DBEngine);
            string sql = BuildSql_Custom_Fields_Func("Sum(" + ps.SqlStr + ")");
            return await DBEngine.ExecuteScalarAsync<TResult>(dbConnection, sql, SqlParams);
        }

        public int Update<TResult>(Expression<Func<T, TResult>> expressionNew, Expression<Func<T, bool>> expressionWhere)
        {
            var tableName = EntityCache.TryGetTableName<T>();

            ParamSql ps = new ParamSql();
            var dic_update = ExpressionResolver.ResolveUpdateExpression(expressionNew, DBEngine);
            StringBuilder sb_setStr = new StringBuilder();
            foreach (var item in dic_update)
            {
                sb_setStr.Append($",{item.SqlStr}");
                ps.Params.AddRange(item.Params);
            }
            string setStr = sb_setStr.ToString().TrimStart(',');
            var ps_where = ExpressionResolver.ResolveExpression(expressionWhere.Body, DBEngine);
            ps.Params.AddRange(ps_where.Params);

            string sql = DBEngine.CreateUpdateSql(DBEngine.EscapeStr(dbConnection.SqlConnection.Database, tableName + QDBTools.ConvertSuffixTableName(TableSuffix)), setStr, ps_where.SqlStr,true);
            if (!string.IsNullOrEmpty(TableSuffix))
            {
                sql = sql.Replace(DBEngine.EscapeStr(tableName), DBEngine.EscapeStr(tableName + QDBTools.ConvertSuffixTableName(TableSuffix)));
            }
            return DBEngine.ExecuteNonQuery(dbConnection, sql, ps.Params); ;
        }

        public async Task<int> UpdateAsync<TResult>(Expression<Func<T, TResult>> expressionNew, Expression<Func<T, bool>> expressionWhere)
        {
            var tableName = EntityCache.TryGetTableName<T>();

            ParamSql ps = new ParamSql();
            var dic_update = ExpressionResolver.ResolveUpdateExpression(expressionNew, DBEngine);
            StringBuilder sb_setStr = new StringBuilder();
            foreach (var item in dic_update)
            {
                sb_setStr.Append($",{item.SqlStr}");
                ps.Params.AddRange(item.Params);
            }
            string setStr = sb_setStr.ToString().TrimStart(',');
            var ps_where = ExpressionResolver.ResolveExpression(expressionWhere.Body, DBEngine);
            ps.Params.AddRange(ps_where.Params);


            string dbTable = DBEngine.EscapeStr(dbConnection.SqlConnection.Database, tableName + QDBTools.ConvertSuffixTableName(TableSuffix));
            string sql = DBEngine.CreateUpdateSql(dbTable, setStr, ps_where.SqlStr,false);
            return await DBEngine.ExecuteNonQueryAsync(dbConnection, sql, ps.Params);
        }

        public IQingQuery<T> Where(Expression<Func<T, bool>> expression)
        {
            return ExpressionResolver.ResolveWhereExpression(this, expression);
        }

        private string BuildSql()
        {
            string tableName = EntityCache.TryGetTableName<T>();
            var fieldsStr = Fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {DBEngine.EscapeStr(dbConnection.SqlConnection.Database, tableName + QDBTools.ConvertSuffixTableName(TableSuffix))}");
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr.TrimStart(',')}");
            }
            if (!string.IsNullOrEmpty(OrderByStr))
            {
                sb_sql.Append($" Order By {OrderByStr.TrimStart(',')}");
            }
            if (!string.IsNullOrEmpty(Limit))
            {
                sb_sql.Append($" {Limit}");
            }
            string sql = sb_sql.ToString();

            return sql;
        }

        private string BuildSql_Custom_Fields_Func(string fields)
        {
            var tableName = EntityCache.TryGetTableName<T>();
            var fieldsStr = fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {DBEngine.EscapeStr(dbConnection.SqlConnection.Database, tableName + QDBTools.ConvertSuffixTableName(TableSuffix))}");
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr.TrimStart(',')}");
            }
            string sql = sb_sql.ToString();

            return sql;
        }
        private string BuildSql_Custom_LimitCount(int count)
        {
            var tableName = EntityCache.TryGetTableName<T>();
            var fieldsStr = Fields ?? "*";

            StringBuilder sb_sql = new StringBuilder($"{fieldsStr.TrimStart(',')} from  {DBEngine.EscapeStr(dbConnection.SqlConnection.Database, tableName + QDBTools.ConvertSuffixTableName(TableSuffix))}");
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                sb_sql.Append($" Group By {GroupByStr.TrimStart(',')}");
            }
            if (!string.IsNullOrEmpty(OrderByStr))
            {
                sb_sql.Append($" Order By {OrderByStr.TrimStart(',')}");
            }
            string sql = "select " + DBEngine.ConvertSelectLimit(sb_sql.ToString(), count);
            return sql;
        }

        private string BuildSql_Custom_LimitPage(int pageNum, int pageSize)
        {
            var nea = EntityCache.TryGetInfo<T>();
            var fieldsStr = Fields ?? "*";

            string where = string.Empty;
            if (!string.IsNullOrEmpty(WhereStr))
            {
                where = " where " + WhereStr;
            }

            string groupBy = string.Empty;
            if (!string.IsNullOrEmpty(GroupByStr))
            {
                groupBy = " Group By  " + GroupByStr.TrimStart(',');
            }

            var orderBy = string.Empty;
            if (!string.IsNullOrEmpty(OrderByStr))
            {
                orderBy = $" Order By {OrderByStr.TrimStart(',')}";
            }
            else if (nea.PrimaryKeys.Count > 0)

            {
                orderBy = $" Order By {string.Join(',', nea.PrimaryKeys)}";

            }


            return DBEngine.CreatePageSql(DBEngine.EscapeStr(dbConnection.SqlConnection.Database, nea.TableName + QDBTools.ConvertSuffixTableName(TableSuffix)), fieldsStr.Trim(','), where, groupBy, orderBy, pageNum, pageSize, dbConnection.SqlConnection.ServerVersion);
        }
        /// <summary>
        /// 流式查询接口
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>

        public Stream QueryStream(string formatStr = null)
        {
            string sql = BuildSql();
            if (!string.IsNullOrEmpty(formatStr))
            {
                sql += " " + formatStr;
            }
            return DBEngine.QueryStream(dbConnection, sql, SqlParams);
        }

        public async Task<Stream> QueryStreamAsync(string formatStr = null)
        {
            string sql = BuildSql();
            if (!string.IsNullOrEmpty(formatStr))
            {
                sql += " " + formatStr;
            }
            return await DBEngine.QueryStreamAsync(dbConnection, sql, SqlParams);
        }

        public Stream QueryStream(Expression<Func<T, bool>> expression, string formatStr = null)
        {
            ExpressionResolver.ResolveWhereExpression(this, expression);
            return QueryStream(formatStr);
        }

        public async Task<Stream> QueryStreamAsync(Expression<Func<T, bool>> expression, string formatStr = null)
        {
            ExpressionResolver.ResolveWhereExpression(this, expression);
            return await QueryStreamAsync(formatStr);
        }

        public Stream QueryStream(int pageNum, int pageSize, string formatStr = null)
        {
            var sql = BuildSql_Custom_LimitPage(pageNum, pageSize);
            if (!string.IsNullOrEmpty(formatStr))
            {
                sql += " " + formatStr;
            }
            return DBEngine.QueryStream(dbConnection, sql, SqlParams);
        }

        public async Task<Stream> QueryStreamAsync(int pageNum, int pageSize, string formatStr = null)
        {
            var sql = BuildSql_Custom_LimitPage(pageNum, pageSize);
            if (!string.IsNullOrEmpty(formatStr))
            {
                sql += " " + formatStr;
            }
            return await DBEngine.QueryStreamAsync(dbConnection, sql, SqlParams);
        }

        public async Task ExportToCsvFileAsync(string filePath)
        {
            string sql = BuildSql();
            await DBEngine.ExportToCsvFileAsync(dbConnection, sql, filePath, SqlParams);
        }

        public async Task ExportToCsvFileAsync(Expression<Func<T, bool>> expression, string filePath)
        {
            ExpressionResolver.ResolveWhereExpression(this, expression);
            await ExportToCsvFileAsync(filePath);
        }

        public async Task ExportToCsvFileAsync(int pageNum, int pageSize, string filePath)
        {
            var sql = BuildSql_Custom_LimitPage(pageNum, pageSize);
            await DBEngine.ExportToCsvFileAsync(dbConnection, sql, filePath, SqlParams);
        }
    }
}
