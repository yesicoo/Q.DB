/*************************************************************************************
 *
 * 文 件 名:   QingUnionQuery.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/13 11:30
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
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Q.DB.Interface;
using Q.DB.Models;

namespace Q.DB.Query
{
    public class QingUnionQuery<TResult>
    {
        private DBConnectionItem dBConnectionItem;
        private string sql;
        private string orderBy = string.Empty;
        private string where = string.Empty;
        private List<ParamItem> SqlParams = new List<ParamItem>();
        private IDBEngine DBEngine;

        public QingUnionQuery(IDBEngine DBEngine, DBConnectionItem dBConnectionItem, string sql, List<ParamItem> sqlParams)
        {
            this.dBConnectionItem = dBConnectionItem;
            this.sql = sql;
            this.SqlParams = sqlParams;
            this.DBEngine = DBEngine;
        }

        public QingUnionQuery<TResult> UnionAll<T>(Expression<Func<T, TResult>> expressionNew, Expression<Func<T, bool>> expressionWhere)
        {



            var wherePS = ExpressionResolver.ResolveExpression(expressionWhere.Body, DBEngine);
            SqlParams.AddRange(wherePS.Params);
            var WhereStr = $"({wherePS.SqlStr})";

            var fidlsPS = ExpressionResolver.ResolveExpression(expressionNew.Body, DBEngine);
            SqlParams.AddRange(fidlsPS.Params);
            var Fields = fidlsPS.SqlStr;

            var tableName = EntityCache.TryGetTableName<T>();
            var fieldsStr = Fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {DBEngine.EscapeStr(dBConnectionItem.SqlConnection.Database, tableName)}");
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            this.sql = $"({this.sql}) Union All ({sb_sql.ToString()})";

            return this;
        }
        public QingUnionQuery<TResult> Union<T>(Expression<Func<T, TResult>> expressionNew, Expression<Func<T, bool>> expressionWhere)
        {
            var wherePS = ExpressionResolver.ResolveExpression(expressionWhere.Body, DBEngine);
            SqlParams.AddRange(wherePS.Params);
            var WhereStr = $"({wherePS.SqlStr})";

            var fidlsPS = ExpressionResolver.ResolveExpression(expressionNew.Body, DBEngine);
            SqlParams.AddRange(fidlsPS.Params);
            var Fields = fidlsPS.SqlStr;

            var tableName = EntityCache.TryGetTableName<T>();
            var fieldsStr = Fields ?? "*";
            StringBuilder sb_sql = new StringBuilder($"select {fieldsStr.TrimStart(',')} from {DBEngine.EscapeStr(dBConnectionItem.SqlConnection.Database, tableName)}");
            if (!string.IsNullOrEmpty(WhereStr))
            {
                sb_sql.Append($" where {WhereStr}");
            }
            this.sql = $"({this.sql}) Union  ({sb_sql.ToString()})";

            return this;
        }
        public IEnumerable<TResult> QueryAll()
        {
            string sql = $"select * from ({this.sql}) as UnionView {orderBy}";

            return DBEngine.Query<TResult>(dBConnectionItem, sql, SqlParams);

        }

        public IAsyncEnumerable<TResult> QueryAllAsync()
        {
            string sql = $"select * from ({this.sql}) as UnionView {orderBy}";

            return DBEngine.QueryAsync<TResult>(dBConnectionItem, sql, SqlParams);

        }

        public IEnumerable<TResult> QueryWithPage(int PageNum, int PageSize)
        {
            string sql = $" * from ({this.sql}) as UnionView {orderBy.TrimStart(',')}  {where}";

            sql = DBEngine.CreatePageSql(sql, PageNum, PageSize, dBConnectionItem.SqlConnection.ServerVersion);
            return DBEngine.Query<TResult>(dBConnectionItem, sql, SqlParams);

        }

        public IAsyncEnumerable<TResult> QueryWithPageAsync(int PageNum, int PageSize)
        {
            string sql = $" * from ({this.sql}) as UnionView {orderBy.TrimStart(',')}  {where}";

            sql = DBEngine.CreatePageSql(sql, PageNum, PageSize, dBConnectionItem.SqlConnection.ServerVersion);
            return DBEngine.QueryAsync<TResult>(dBConnectionItem, sql, SqlParams);


        }

        public PageResult<TResult> QueryPages(int PageNum, int PageSize)
        {
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = PageNum;
            pageResult.CurrentPageSize = PageSize;
            string sql = $" * from ({this.sql}) as UnionView {orderBy.TrimStart(',')} {where} ";
            sql = DBEngine.CreatePageSql(sql, PageNum, PageSize, dBConnectionItem.SqlConnection.ServerVersion);

            pageResult.ArrayData = DBEngine.Query<TResult>(dBConnectionItem, sql, SqlParams);
            pageResult.TotalCount = DBEngine.ExecuteScalar<int>(dBConnectionItem, $"select count(*) from ({this.sql}) as UnionView {where}", SqlParams);
            return pageResult;
        }

        public QingUnionQuery<TResult> Where(Expression<Func<TResult, bool>> expressionWhere)
        {
            var wherePS = ExpressionResolver.ResolveExpression(expressionWhere.Body, DBEngine);
            SqlParams.AddRange(wherePS.Params);
            if (string.IsNullOrEmpty(where))
            {
                where = $" where ({wherePS.SqlStr})";
            }
            else
            {
                where += $" and ({wherePS.SqlStr})";
            }

            return this;
        }

        public async Task<PageResult<TResult>> QueryPagesAsync(int PageNum, int PageSize)
        {
            PageResult<TResult> pageResult = new PageResult<TResult>();
            pageResult.CurrentPageNum = PageNum;
            pageResult.CurrentPageSize = PageSize;
            string sql = $" * from ({this.sql}) as UnionView {orderBy.TrimStart(',')} {where} ";
            sql = DBEngine.CreatePageSql(sql, PageNum, PageSize, dBConnectionItem.SqlConnection.ServerVersion);
            pageResult.TotalCount = await DBEngine.ExecuteScalarAsync<int>(dBConnectionItem, $"select count(*) from ({this.sql}) as UnionView {where}", SqlParams);
            if (pageResult.TotalCount == 0)
            {
                pageResult.ArrayData = new List<TResult>();
            }
            else
            {
#if NET7_0_OR_GREATER
                pageResult.ArrayData = DBEngine.QueryAsync<TResult>(dBConnectionItem, sql, SqlParams).ToBlockingEnumerable() ;
#else
                List<TResult> list = new List<TResult>();
                await foreach (var item in DBEngine.QueryAsync<TResult>(dBConnectionItem, sql, SqlParams))
                {
                    list.Add(item);
                }
                pageResult.ArrayData = list;
#endif
            }
            return pageResult;
        }

        public QingUnionQuery<TResult> OrderBy(Expression<Func<TResult, object>> expression, bool desc = false)
        {
            var ps = ExpressionResolver.ResolveExpression(expression.Body, DBEngine);
            orderBy += ",Order By" + ps.SqlStr + (desc ? " Desc" : "");
            this.SqlParams.AddRange(ps.Params);
            return this;
        }

        public int Count()
        {
            string sql = $"select count(*) from ({this.sql}) as UnionView ";

            return DBEngine.ExecuteScalar<int>(dBConnectionItem, sql, SqlParams);
        }

        public async Task<int> CountAsync()
        {
            string sql = $"select count(*) from ({this.sql}) as UnionView ";

            return await DBEngine.ExecuteScalarAsync<int>(dBConnectionItem, sql, SqlParams);
        }
    }
}
