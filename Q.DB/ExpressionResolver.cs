/*************************************************************************************
 *
 * 文 件 名:   ExpressionResolver.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/15 10:37
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
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Q.DB.Interface;
using Q.DB.Models;
using Q.DB.Query;

namespace Q.DB
{
    internal static class ExpressionResolver
    {

        #region Where
        internal static QingQuery<T> ResolveWhereExpression<T>(QingQuery<T> nt, Expression<Func<T, bool>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.WhereStr += string.IsNullOrEmpty(nt.WhereStr) ? $"({ps.SqlStr})" : $" and ({ps.SqlStr}) ";
            return nt;
        }

        internal static QingQuery<T, T2> ResolveWhereExpression<T, T2>(QingQuery<T, T2> nt, Expression<Func<T, T2, bool>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.WhereStr += string.IsNullOrEmpty(nt.WhereStr) ? $"({ps.SqlStr})" : $" and ({ps.SqlStr}) ";
            return nt;
        }

        internal static QingQuery<T, T2, T3> ResolveWhereExpression<T, T2, T3>(QingQuery<T, T2, T3> nt, Expression<Func<T, T2, T3, bool>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.WhereStr += string.IsNullOrEmpty(nt.WhereStr) ? $"({ps.SqlStr})" : $" and ({ps.SqlStr}) ";
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4> ResolveWhereExpression<T, T2, T3, T4>(QingQuery<T, T2, T3, T4> nt, Expression<Func<T, T2, T3, T4, bool>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.WhereStr += string.IsNullOrEmpty(nt.WhereStr) ? $"({ps.SqlStr})" : $" and ({ps.SqlStr}) ";
            return nt;
        }

        internal static QingQuery<T, T2, T3, T4, T5> ResolveWhereExpression<T, T2, T3, T4, T5>(QingQuery<T, T2, T3, T4, T5> nt, Expression<Func<T, T2, T3, T4, T5, bool>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.WhereStr += string.IsNullOrEmpty(nt.WhereStr) ? $"({ps.SqlStr})" : $" and ({ps.SqlStr}) ";
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4, T5, T6> ResolveWhereExpression<T, T2, T3, T4, T5, T6>(QingQuery<T, T2, T3, T4, T5, T6> nt, Expression<Func<T, T2, T3, T4, T5, T6, bool>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.WhereStr += string.IsNullOrEmpty(nt.WhereStr) ? $"({ps.SqlStr})" : $" and ({ps.SqlStr}) ";
            return nt;
        }

        internal static QingQuery<T, T2, T3, T4, T5, T6, T7> ResolveWhereExpression<T, T2, T3, T4, T5, T6, T7>(QingQuery<T, T2, T3, T4, T5, T6, T7> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, bool>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.WhereStr += string.IsNullOrEmpty(nt.WhereStr) ? $"({ps.SqlStr})" : $" and ({ps.SqlStr}) ";
            return nt;
        }

        internal static QingQuery<T, T2, T3, T4, T5, T6, T7, T8> ResolveWhereExpression<T, T2, T3, T4, T5, T6, T7, T8>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, bool>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.WhereStr += string.IsNullOrEmpty(nt.WhereStr) ? $"({ps.SqlStr})" : $" and ({ps.SqlStr}) ";
            return nt;
        }

        internal static QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> ResolveWhereExpression<T, T2, T3, T4, T5, T6, T7, T8, T9>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, bool>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.WhereStr += string.IsNullOrEmpty(nt.WhereStr) ? $"({ps.SqlStr})" : $" and ({ps.SqlStr}) ";
            return nt;
        }


        internal static QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> ResolveWhereExpression<T, T2, T3, T4, T5, T6, T7, T8, T9, T10>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, bool>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.WhereStr += string.IsNullOrEmpty(nt.WhereStr) ? $"({ps.SqlStr})" : $" and ({ps.SqlStr}) ";
            return nt;
        }

        #endregion

        #region Select
        internal static QingQuery<T> ResolveSelectExpression<T, TResult>(QingQuery<T> nt, Expression<Func<T, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields += ps.SqlStr;
            return nt;
        }

        internal static IQingQuery<T> ResolveSelectExceptExpression<T, TResult>(QingQuery<T> nt, Expression<Func<T, TResult>> expression)
        {

            var ttype = typeof(T);
            var nea = EntityCache.TryGetInfo<T>();
            var func = (expression.Body as NewExpression);
            var properties = ttype.GetProperties();

            if (expression.Body.NodeType == ExpressionType.New)
            {
                List<string> exceptNames = new List<string>();
                foreach (var item in func.Arguments)
                {
                    exceptNames.Add((item as MemberExpression).Member.Name);
                }
                StringBuilder sb_select = new StringBuilder();
                foreach (var propertie in properties)
                {
                    if (!exceptNames.Contains(propertie.Name))
                    {

                        sb_select.Append($"{nt.DBEngine.EscapeStr(nea.TableName, propertie.Name)},");

                    }
                }
                nt.Fields += sb_select.ToString().TrimEnd(',');
            }
            else if (expression.Body.NodeType == ExpressionType.MemberAccess)
            {
                var name = (expression.Body as MemberExpression).Member.Name;
                StringBuilder sb_select = new StringBuilder();
                foreach (var propertie in properties)
                {
                    if (propertie.Name != name)
                    {

                        sb_select.Append($"{nt.DBEngine.EscapeStr(nea.TableName, propertie.Name)},");
                    }
                }
                nt.Fields += sb_select.ToString().TrimEnd(',');
            }
            else
            {
                SqlLogUtil.SendLog(LogType.Error, "SelectExcept 参数类型不支持");
            }
            return nt;
        }

        internal static QingQuery<T, T2> ResolveSelectExpression<T, T2, TResult>(QingQuery<T, T2> nt, Expression<Func<T, T2, TResult>> expression)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields += ps.SqlStr;
            return nt;
        }

        internal static QingQuery<T, T2, T3> ResolveSelectExpression<T, T2, T3, TResult>(QingQuery<T, T2, T3> nt, Expression<Func<T, T2, T3, TResult>> expression)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields += ps.SqlStr;
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4> ResolveSelectExpression<T, T2, T3, T4, TResult>(QingQuery<T, T2, T3, T4> nt, Expression<Func<T, T2, T3, T4, TResult>> expression)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields += ps.SqlStr;
            return nt;
        }

        internal static QingQuery<T, T2, T3, T4, T5> ResolveSelectExpression<T, T2, T3, T4, T5, TResult>(QingQuery<T, T2, T3, T4, T5> nt, Expression<Func<T, T2, T3, T4, T5, TResult>> expression)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields += ps.SqlStr;
            return nt;
        }

        internal static QingQuery<T, T2, T3, T4, T5, T6> ResolveSelectExpression<T, T2, T3, T4, T5, T6, TResult>(QingQuery<T, T2, T3, T4, T5, T6> nt, Expression<Func<T, T2, T3, T4, T5, T6, TResult>> expression)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields += ps.SqlStr;
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4, T5, T6, T7> ResolveSelectExpression<T, T2, T3, T4, T5, T6, T7, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, TResult>> expression)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields += ps.SqlStr;
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4, T5, T6, T7, T8> ResolveSelectExpression<T, T2, T3, T4, T5, T6, T7, T8, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, TResult>> expression)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields += ps.SqlStr;
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> ResolveSelectExpression<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>> expression)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields += ps.SqlStr;
            return nt;
        }

        internal static QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> ResolveSelectExpression<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>> expression)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields += ps.SqlStr;
            return nt;
        }
        #endregion

        #region OrderBy

        internal static QingQuery<T> ResolveOrderByExpression<T, TResult>(QingQuery<T> nt, Expression<Func<T, TResult>> expression, bool Desc = false)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.OrderByStr += "," + ps.SqlStr + (Desc ? " Desc" : "");
            return nt;
        }

        internal static IQingQuery<T, T2> ResolveOrderByExpression<T, T2, TResult>(QingQuery<T, T2> nt, Expression<Func<T, T2, TResult>> expression, bool Desc = false)


        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.OrderByStr += "," + ps.SqlStr + (Desc ? " Desc" : "");
            return nt;
        }
        internal static IQingQuery<T, T2, T3> ResolveOrderByExpression<T, T2, T3, TResult>(QingQuery<T, T2, T3> nt, Expression<Func<T, T2, T3, TResult>> expression, bool Desc = false)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.OrderByStr += "," + ps.SqlStr + (Desc ? " Desc" : "");
            return nt;
        }
        internal static IQingQuery<T, T2, T3, T4> ResolveOrderByExpression<T, T2, T3, T4, TResult>(QingQuery<T, T2, T3, T4> nt, Expression<Func<T, T2, T3, T4, TResult>> expression, bool Desc = false)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.OrderByStr += "," + ps.SqlStr + (Desc ? " Desc" : "");
            return nt;
        }

        internal static IQingQuery<T, T2, T3, T4, T5> ResolveOrderByExpression<T, T2, T3, T4, T5, TResult>(QingQuery<T, T2, T3, T4, T5> nt, Expression<Func<T, T2, T3, T4, T5, TResult>> expression, bool Desc = false)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.OrderByStr += "," + ps.SqlStr + (Desc ? " Desc" : "");
            return nt;
        }

        internal static IQingQuery<T, T2, T3, T4, T5, T6> ResolveOrderByExpression<T, T2, T3, T4, T5, T6, TResult>(QingQuery<T, T2, T3, T4, T5, T6> nt, Expression<Func<T, T2, T3, T4, T5, T6, TResult>> expression, bool Desc = false)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.OrderByStr += "," + ps.SqlStr + (Desc ? " Desc" : "");
            return nt;
        }

        internal static IQingQuery<T, T2, T3, T4, T5, T6, T7> ResolveOrderByExpression<T, T2, T3, T4, T5, T6, T7, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, TResult>> expression, bool Desc = false)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.OrderByStr += "," + ps.SqlStr + (Desc ? " Desc" : "");
            return nt;
        }

        internal static IQingQuery<T, T2, T3, T4, T5, T6, T7, T8> ResolveOrderByExpression<T, T2, T3, T4, T5, T6, T7, T8, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, TResult>> expression, bool Desc = false)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.OrderByStr += "," + ps.SqlStr + (Desc ? " Desc" : "");
            return nt;
        }

        internal static IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> ResolveOrderByExpression<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>> expression, bool Desc = false)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.OrderByStr += "," + ps.SqlStr + (Desc ? " Desc" : "");
            return nt;
        }

        internal static IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> ResolveOrderByExpression<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>> expression, bool Desc = false)
        {

            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.OrderByStr += "," + ps.SqlStr + (Desc ? " Desc" : "");
            return nt;
        }
        #endregion

        #region Max
        internal static QingQuery<T> ResolveMaxExpression<T, TResult>(QingQuery<T> nt, Expression<Func<T, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Max({ps.SqlStr})";
            return nt;
        }

        internal static QingQuery<T, T2> ResolveMaxExpression<T, T2, TResult>(QingQuery<T, T2> nt, Expression<Func<T, T2, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Max({ps.SqlStr})";
            return nt;
        }

        internal static QingQuery<T, T2, T3> ResolveMaxExpression<T, T2, T3, TResult>(QingQuery<T, T2, T3> nt, Expression<Func<T, T2, T3, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Max({ps.SqlStr})";
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4> ResolveMaxExpression<T, T2, T3, T4, TResult>(QingQuery<T, T2, T3, T4> nt, Expression<Func<T, T2, T3, T4, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Max({ps.SqlStr})";
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4, T5> ResolveMaxExpression<T, T2, T3, T4, T5, TResult>(QingQuery<T, T2, T3, T4, T5> nt, Expression<Func<T, T2, T3, T4, T5, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Max({ps.SqlStr})";
            return nt;
        }

        internal static QingQuery<T, T2, T3, T4, T5, T6> ResolveMaxExpression<T, T2, T3, T4, T5, T6, TResult>(QingQuery<T, T2, T3, T4, T5, T6> nt, Expression<Func<T, T2, T3, T4, T5, T6, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Max({ps.SqlStr})";
            return nt;
        }

        internal static QingQuery<T, T2, T3, T4, T5, T6, T7> ResolveMaxExpression<T, T2, T3, T4, T5, T6, T7, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Max({ps.SqlStr})";
            return nt;
        }

        internal static QingQuery<T, T2, T3, T4, T5, T6, T7, T8> ResolveMaxExpression<T, T2, T3, T4, T5, T6, T7, T8, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Max({ps.SqlStr})";
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> ResolveMaxExpression<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Max({ps.SqlStr})";
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> ResolveMaxExpression<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Max({ps.SqlStr})";
            return nt;
        }
        #endregion

        #region Min
        internal static QingQuery<T> ResolveMinExpression<T, TResult>(QingQuery<T> nt, Expression<Func<T, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Min({ps.SqlStr})";
            return nt;
        }
        internal static QingQuery<T, T2> ResolveMinExpression<T, T2, TResult>(QingQuery<T, T2> nt, Expression<Func<T, T2, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Min({ps.SqlStr})";
            return nt;
        }
        internal static QingQuery<T, T2, T3> ResolveMinExpression<T, T2, T3, TResult>(QingQuery<T, T2, T3> nt, Expression<Func<T, T2, T3, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Min({ps.SqlStr})";
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4> ResolveMinExpression<T, T2, T3, T4, TResult>(QingQuery<T, T2, T3, T4> nt, Expression<Func<T, T2, T3, T4, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Min({ps.SqlStr})";
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4, T5> ResolveMinExpression<T, T2, T3, T4, T5, TResult>(QingQuery<T, T2, T3, T4, T5> nt, Expression<Func<T, T2, T3, T4, T5, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Min({ps.SqlStr})";
            return nt;
        }

        internal static QingQuery<T, T2, T3, T4, T5, T6> ResolveMinExpression<T, T2, T3, T4, T5, T6, TResult>(QingQuery<T, T2, T3, T4, T5, T6> nt, Expression<Func<T, T2, T3, T4, T5, T6, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Min({ps.SqlStr})";
            return nt;
        }

        internal static QingQuery<T, T2, T3, T4, T5, T6, T7> ResolveMinExpression<T, T2, T3, T4, T5, T6, T7, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Min({ps.SqlStr})";
            return nt;
        }

        internal static QingQuery<T, T2, T3, T4, T5, T6, T7, T8> ResolveMinExpression<T, T2, T3, T4, T5, T6, T7, T8, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Min({ps.SqlStr})";
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> ResolveMinExpression<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Min({ps.SqlStr})";
            return nt;
        }
        internal static QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> ResolveMinExpression<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.Fields = $"Min({ps.SqlStr})";
            return nt;
        }
        #endregion

        #region Join
        internal static ParamSql ResolveJoinExpression<T, T2>(QingQuery<T> nt, Expression<Func<T, T2, bool>> expressionJoinOn)
        {
            return ResolveExpression(expressionJoinOn.Body, nt.DBEngine);
        }

        internal static ParamSql ResolveJoinExpression<T, T2, T3>(QingQuery<T, T2> nt, Expression<Func<T, T2, T3, bool>> expressionJoinOn)
        {
            return ResolveExpression(expressionJoinOn.Body, nt.DBEngine);
        }
        internal static ParamSql ResolveJoinExpression<T, T2, T3, T4>(QingQuery<T, T2, T3> nt, Expression<Func<T, T2, T3, T4, bool>> expressionJoinOn)
        {
            return ResolveExpression(expressionJoinOn.Body, nt.DBEngine);
        }

        internal static ParamSql ResolveJoinExpression<T, T2, T3, T4, T5>(QingQuery<T, T2, T3, T4> nt, Expression<Func<T, T2, T3, T4, T5, bool>> expressionJoinOn)
        {
            return ResolveExpression(expressionJoinOn.Body, nt.DBEngine);
        }

        internal static ParamSql ResolveJoinExpression<T, T2, T3, T4, T5, T6>(QingQuery<T, T2, T3, T4, T5> nt, Expression<Func<T, T2, T3, T4, T5, T6, bool>> expressionJoinOn)
        {
            return ResolveExpression(expressionJoinOn.Body, nt.DBEngine);
        }
        internal static ParamSql ResolveJoinExpression<T, T2, T3, T4, T5, T6, T7>(QingQuery<T, T2, T3, T4, T5, T6> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, bool>> expressionJoinOn)
        {
            return ResolveExpression(expressionJoinOn.Body, nt.DBEngine);
        }

        internal static ParamSql ResolveJoinExpression<T, T2, T3, T4, T5, T6, T7, T8>(QingQuery<T, T2, T3, T4, T5, T6, T7> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, bool>> expressionJoinOn)
        {
            return ResolveExpression(expressionJoinOn.Body, nt.DBEngine);
        }

        internal static ParamSql ResolveJoinExpression<T, T2, T3, T4, T5, T6, T7, T8, T9>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, bool>> expressionJoinOn)
        {
            return ResolveExpression(expressionJoinOn.Body, nt.DBEngine);
        }

        internal static ParamSql ResolveJoinExpression<T, T2, T3, T4, T5, T6, T7, T8, T9, T10>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, bool>> expressionJoinOn)
        {
            return ResolveExpression(expressionJoinOn.Body, nt.DBEngine);
        }
        #endregion

        #region GroupBy
        internal static IQingQuery<T> ResolveGroupByExpression<T, TResult>(QingQuery<T> nt, Expression<Func<T, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.GroupByStr = ps.SqlStr;
            return nt;
        }

        internal static IQingQuery<T, T2> ResolveGroupByExpression<T, T2, TResult>(QingQuery<T, T2> nt, Expression<Func<T, T2, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.GroupByStr = ps.SqlStr;
            return nt;
        }
        internal static IQingQuery<T, T2, T3> ResolveGroupByExpression<T, T2, T3, TResult>(QingQuery<T, T2, T3> nt, Expression<Func<T, T2, T3, TResult>> expression)

        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.GroupByStr = ps.SqlStr;
            return nt;
        }

        internal static IQingQuery<T, T2, T3, T4> ResolveGroupByExpression<T, T2, T3, T4, TResult>(QingQuery<T, T2, T3, T4> nt, Expression<Func<T, T2, T3, T4, TResult>> expression)

        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.GroupByStr = ps.SqlStr;
            return nt;
        }
        internal static IQingQuery<T, T2, T3, T4, T5> ResolveGroupByExpression<T, T2, T3, T4, T5, TResult>(QingQuery<T, T2, T3, T4, T5> nt, Expression<Func<T, T2, T3, T4, T5, TResult>> expression)

        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.GroupByStr = ps.SqlStr;
            return nt;
        }

        internal static IQingQuery<T, T2, T3, T4, T5, T6> ResolveGroupByExpression<T, T2, T3, T4, T5, T6, TResult>(QingQuery<T, T2, T3, T4, T5, T6> nt, Expression<Func<T, T2, T3, T4, T5, T6, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.GroupByStr = ps.SqlStr;
            return nt;
        }

        internal static IQingQuery<T, T2, T3, T4, T5, T6, T7> ResolveGroupByExpression<T, T2, T3, T4, T5, T6, T7, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.GroupByStr = ps.SqlStr;
            return nt;
        }

        internal static IQingQuery<T, T2, T3, T4, T5, T6, T7, T8> ResolveGroupByExpression<T, T2, T3, T4, T5, T6, T7, T8, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, TResult>> expression)
        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.GroupByStr = ps.SqlStr;
            return nt;
        }
        internal static IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> ResolveGroupByExpression<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, TResult>> expression)

        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.GroupByStr = ps.SqlStr;
            return nt;
        }
        internal static IQingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> ResolveGroupByExpression<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(QingQuery<T, T2, T3, T4, T5, T6, T7, T8, T9, T10> nt, Expression<Func<T, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>> expression)

        {
            var ps = ResolveExpression(expression.Body, nt.DBEngine);
            nt.SqlParams.AddRange(ps.Params);
            nt.GroupByStr = ps.SqlStr;
            return nt;
        }
        #endregion


        #region Update
        internal static List<ParamSql> ResolveUpdateExpression<T, TResult>(Expression<Func<T, TResult>> expression, IDBEngine DBEngine)
        {
            if (expression.Body.NodeType == ExpressionType.MemberInit)
            {
                var func = (MemberInitExpression)expression.Body;
                List<ParamSql> pss = new List<ParamSql>();
                foreach (var item in func.Bindings)
                {
                    var assignment = (MemberAssignment)item;
                    var key = assignment.Member.Name;
                    var ps = ResolveExpression(assignment.Expression, DBEngine);
                    ps.SqlStr = $"{DBEngine.EscapeStr(key)}={ps.SqlStr}";
                    pss.Add(ps);
                }
                return pss;
            }
            else if (expression.Body.NodeType == ExpressionType.MemberAccess)
            {
                var nea = EntityCache.TryGetInfo<T>();
                var func = (MemberExpression)expression.Body;
                List<ParamSql> pss = new List<ParamSql>();
                var properties = func.Type.GetProperties();
                foreach (var propertiy in properties)
                {

                    if (nea.PropertyInfos.TryGetValue(propertiy.Name, out var np))
                    {
                        if (np != null)
                        {
                            continue;
                        }
                        var constant = (ConstantExpression)func.Expression;
                        var fieldInfoValue = ((FieldInfo)func.Member).GetValue(constant.Value);
                        var value = propertiy.GetValue(fieldInfoValue, null);
                        ParamSql _ps = DBEngine.CreateParamItemByPropertiy(propertiy, value);
                        pss.Add(_ps);
                    }
                }
                return pss;
            }
            else if (expression.Body.NodeType == ExpressionType.Add)
            {
                List<ParamSql> pss = new List<ParamSql>();
                var func = expression.Body as BinaryExpression;
                var lps = ResolveExpression(func.Left, DBEngine);
                var ps = BinaryExpression(func, DBEngine);
                ps.SqlStr= lps.SqlStr +" = "+ps.SqlStr;
                pss.Add(ps);
                return pss;
            }
            else if (expression.Body.NodeType == ExpressionType.New)
            {
                var nea = EntityCache.TryGetInfo<T>();
                List<ParamSql> pss = new List<ParamSql>();
                var func = expression.Body as NewExpression;
                ParamSql ps = new ParamSql();
                List<string> sqls = new List<string>();
                for (int i = 0; i < func.Arguments.Count; i++)
                {
                    var arg = func.Arguments[i];
                    var member = func.Members[i];
                    if (!nea.PropertyInfos.ContainsKey(member.Name)) {
                        continue;
                    }

                    switch (arg.NodeType)
                    {
                        case ExpressionType.MemberAccess:
                            {
                                var pr = ResolveExpression(arg, DBEngine);
                                ps.Params.AddRange(pr.Params);
                                if (((MemberExpression)arg).Member.Name != member.Name)
                                {
                                    pr.SqlStr = $"{DBEngine.EscapeStr(member.Name)} = {pr.SqlStr}";
                                }
                                sqls.Add(pr.SqlStr);
                            }
                            break;
                        case ExpressionType.Call:
                            {
                                var argsql = ResolveExpression(arg, DBEngine);
                                if (argsql.SqlStr.EndsWith("*"))
                                {
                                    continue;
                                }
                                else
                                {
                                    sqls.Add($"{DBEngine.EscapeStr(member.Name)} = {argsql.SqlStr}");
                                    ps.Params.AddRange(argsql.Params);
                                }
                            }
                            break;
                        case ExpressionType.Multiply:
                        case ExpressionType.Add:
                        case ExpressionType.Subtract:
                        case ExpressionType.Divide:
                            {
                                var mu = ResolveExpression(arg, DBEngine);
                                ps.Params.AddRange(mu.Params);
                                sqls.Add($"{DBEngine.EscapeStr(member.Name)} = {mu.SqlStr}");
                            }
                            break;
                        default:
                            {
                                sqls.Add($"{DBEngine.EscapeStr(member.Name)} = {arg}");
                            }
                            break;
                    }

                }
                ps.SqlStr = string.Join(",", sqls);
              
                pss.Add(ps);
                return pss;
            }
            return null;
        }
        #endregion

        #region 私有方法
        /// <summary>
        /// 通过Lambda解析为Sql
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        internal static ParamSql ResolveExpression(Expression func, IDBEngine engine)
        {
            ParamSql result = new ParamSql();
            switch (func.NodeType)
            {
                case ExpressionType.AndAlso:
                case ExpressionType.OrElse:
                case ExpressionType.Equal:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.LessThan:
                case ExpressionType.NotEqual:
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Negate:
                case ExpressionType.Subtract:
                case ExpressionType.Divide:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:

                    result = BinaryExpression(func as BinaryExpression, engine);
                    break;
                case ExpressionType.Constant:
                    result = ConstantExpression(func as ConstantExpression, engine);
                    break;
                case ExpressionType.Call:

                    result = MethodCallExpression(func as MethodCallExpression, engine);
                    break;
                case ExpressionType.Quote:
                case ExpressionType.Not:
                case ExpressionType.Convert:

                    result = UnaryExpression(func as UnaryExpression, engine);
                    break;
                case ExpressionType.MemberAccess:

                    result = MemberAccessExpression(func as MemberExpression, engine);
                    break;
                case ExpressionType.New:
                    result = NewExpression(func as NewExpression, engine);
                    break;
                case ExpressionType.MemberInit:
                    result = MemberInitExpression(func as MemberInitExpression, engine);
                    break;
                case ExpressionType.Lambda:
                    result = ResolveExpression((func as LambdaExpression).Body, engine);
                    break;
                case ExpressionType.NewArrayInit:
                    result = NewArrayExpression(func as NewArrayExpression, engine);
                    break;
                #region 暂不支持&不会识别的类型
                case ExpressionType.And:
                case ExpressionType.ArrayLength:
                case ExpressionType.ArrayIndex:
                case ExpressionType.Coalesce:
                case ExpressionType.Conditional:
                case ExpressionType.ConvertChecked:
                case ExpressionType.ExclusiveOr:
                case ExpressionType.Invoke:
                case ExpressionType.LeftShift:
                case ExpressionType.ListInit:
                case ExpressionType.Modulo:
                case ExpressionType.UnaryPlus:
                case ExpressionType.NegateChecked:
                case ExpressionType.NewArrayBounds:
                case ExpressionType.Or:
                case ExpressionType.Parameter:
                case ExpressionType.Power:
                case ExpressionType.RightShift:
                case ExpressionType.SubtractChecked:
                case ExpressionType.TypeAs:
                case ExpressionType.TypeIs:
                case ExpressionType.Assign:
                case ExpressionType.Block:
                case ExpressionType.DebugInfo:
                case ExpressionType.Decrement:
                case ExpressionType.Dynamic:
                case ExpressionType.Default:
                case ExpressionType.Extension:
                case ExpressionType.Goto:
                case ExpressionType.Increment:
                case ExpressionType.Index:
                case ExpressionType.Label:
                case ExpressionType.RuntimeVariables:
                case ExpressionType.Loop:
                case ExpressionType.Switch:
                case ExpressionType.Throw:
                case ExpressionType.Try:
                case ExpressionType.Unbox:
                case ExpressionType.AddAssign:
                case ExpressionType.AndAssign:
                case ExpressionType.DivideAssign:
                case ExpressionType.ExclusiveOrAssign:
                case ExpressionType.LeftShiftAssign:
                case ExpressionType.ModuloAssign:
                case ExpressionType.MultiplyAssign:
                case ExpressionType.OrAssign:
                case ExpressionType.PowerAssign:
                case ExpressionType.RightShiftAssign:
                case ExpressionType.SubtractAssign:
                case ExpressionType.AddAssignChecked:
                case ExpressionType.MultiplyAssignChecked:
                case ExpressionType.SubtractAssignChecked:
                case ExpressionType.PreIncrementAssign:
                case ExpressionType.PreDecrementAssign:
                case ExpressionType.PostIncrementAssign:
                case ExpressionType.PostDecrementAssign:
                case ExpressionType.TypeEqual:
                case ExpressionType.OnesComplement:
                case ExpressionType.IsTrue:
                case ExpressionType.IsFalse:
                    #endregion
                    break;
            }
            return result;
        }

        /// <summary>
        /// 判断一元表达式
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        private static ParamSql UnaryExpression(UnaryExpression func, IDBEngine engine)
        {
            ParamSql ps = new ParamSql();
            var result = GetOperator(func.NodeType);
            var psr = ResolveExpression(func.Operand, engine);
            if (result == "Not" && psr.SqlStr.Contains("like"))
            {
                ps.SqlStr = psr.SqlStr.Replace(" like ", " Not like ");
                ps.Params.AddRange(psr.Params);
            }
            else
            {
                ps.SqlStr = result + psr.SqlStr;
                ps.Params.AddRange(psr.Params);
            }

            return ps;
        }


        private static ParamSql NewExpression(NewExpression func, IDBEngine engine)
        {
            ParamSql ps = new ParamSql();
            List<string> sqls = new List<string>();
            for (int i = 0; i < func.Arguments.Count; i++)
            {
                var arg = func.Arguments[i];
                var member = func.Members[i];
                switch (arg.NodeType)
                {
                    case ExpressionType.MemberAccess:
                        {
                            var pr = ResolveExpression(arg, engine);
                            ps.Params.AddRange(pr.Params);
                            if (((MemberExpression)arg).Member.Name != member.Name)
                            {
                                pr.SqlStr += $" as '{member.Name}'";
                            }
                            sqls.Add(pr.SqlStr);
                        }
                        break;
                    case ExpressionType.Call:
                        {
                            var argsql = ResolveExpression(arg, engine);
                            ps.Params.AddRange(argsql.Params);
                            if (argsql.SqlStr.EndsWith("*"))
                            {
                                sqls.Add($"{argsql.SqlStr}");
                            }
                            else
                            {
                                sqls.Add($"{argsql.SqlStr} as '{member.Name}'");
                            }
                        }
                        break;
                    case ExpressionType.Multiply:
                    case ExpressionType.Add:
                    case ExpressionType.Subtract:
                    case ExpressionType.Divide:
                        {
                            var mu = ResolveExpression(arg, engine);
                            ps.Params.AddRange(mu.Params);
                            sqls.Add($"{mu.SqlStr} as '{member.Name}'");
                        }
                        break;
                    default:
                        {
                            sqls.Add($"{arg} as '{member.Name}'");
                        }
                        break;
                }

            }
            ps.SqlStr = string.Join(",", sqls);
            return ps;
        }


        private static ParamSql MemberInitExpression(MemberInitExpression func, IDBEngine engine)
        {
            ParamSql ps = new ParamSql();
            List<string> list_str = new List<string>();



            foreach (var item in func.Bindings)
            {
                var assignment = (MemberAssignment)item;
                var sql = ResolveExpression(assignment.Expression, engine);
                list_str.Add($"{sql.SqlStr} as '{assignment.Member.Name}'");
            }
            ps.SqlStr = string.Join(",", list_str);
            return ps;
        }

        private static ParamSql NewArrayExpression(NewArrayExpression func, IDBEngine engine)
        {
            ParamSql ps = new ParamSql();

            foreach (var item in func.Expressions)
            {
                var _ps = ResolveExpression(item, engine);
                ps.SqlStr += _ps.SqlStr + ",";
                ps.Params.AddRange(_ps.Params);
            }
            ps.SqlStr = ps.SqlStr.TrimEnd(',');
            return ps;
        }

        private static ParamSql BinaryExpression(BinaryExpression func, IDBEngine engine)
        {
            ParamSql ps = new ParamSql();
            var lps = ResolveExpression(func.Left, engine);
            var rps = ResolveExpression(func.Right, engine);
            var _operator = GetOperator(func.NodeType);
            if (rps?.SqlStr?.Trim() == "NULL")
            {
                if (_operator.Trim() == "=")
                {
                    _operator = " IS ";
                }
                else if (_operator.Trim() == "<>")
                {
                    _operator = " IS NOT ";
                }
            }
            if (func.NodeType == ExpressionType.AndAlso)
            {
                ps.SqlStr = $"({lps.SqlStr}) {_operator} ({rps.SqlStr})";
            }
            else
            {
                ps.SqlStr = $"{lps.SqlStr} {_operator} {rps.SqlStr}";
            }
            ps.Params.AddRange(lps.Params);
            ps.Params.AddRange(rps.Params);
            return ps;
        }

        private static ParamSql ConstantExpression(ConstantExpression func, IDBEngine engine)
        {
            ParamSql ps = new ParamSql();
            if (func.Value == null)
            {
                ps.SqlStr = "NULL";
            }
            else if (func.Value.ToString() == "")
            {
                ps.SqlStr = "\'\' ";
            }
            else if (func.Value.ToString() == "True")
            {
                ps.SqlStr = "1 = 1 ";
            }
            else if (func.Value.ToString() == "False")
            {
                ps.SqlStr = "0 = 1 ";
            }
            else
            {


                (string key, string skey) = engine.EscapeParamKey(typeName: func.Type.Name);
                ps.SqlStr = skey;
                if (func.Type.Name == "DateTime")
                {
                    ps.Params.Add(new ParamItem(key, func.Value, System.Data.DbType.DateTime));
                }
                else
                {
                    ps.Params.Add(new ParamItem(key, func.Value));
                }
            }

            return ps;
        }



        /// <summary>
        /// 判断包含变量的表达式
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        private static ParamSql MemberAccessExpression(MemberExpression func, IDBEngine engine)
        {
            ParamSql ps = new ParamSql();

            if (func.Expression != null && func.Expression.NodeType == ExpressionType.Parameter)
            {
                var tableName = EntityCache.TryGetTableName(func.Expression.Type);
                if (tableName != null)
                {
                    ps.SqlStr = $"{engine.EscapeStr(tableName, func.Member.Name)}";
                }
                else
                {
                    ps.SqlStr = $"{engine.EscapeStr(func.Member.Name)}";
                }
            }
            else if (func.Expression != null && func.Expression.NodeType == ExpressionType.Call)
            {

                return ResolveExpression(func.Expression, engine);
            }

            else
            {

                switch (func.Type.Name)
                {

                    case "String":
                        {
                            var getter = Expression.Lambda<Func<string>>(func).Compile();
                            (string key, string skey) = engine.EscapeParamKey(typeName: func.Type.Name);
                            ps.SqlStr = skey;
                            ps.Params.Add(new ParamItem(key, getter(), System.Data.DbType.String));
                        }
                        break;
                    case "Int32":
                        {
                            var getter = Expression.Lambda<Func<int>>(func).Compile();
                            (string key, string skey) = engine.EscapeParamKey(typeName: func.Type.Name);
                            ps.SqlStr = skey;
                            ps.Params.Add(new ParamItem(key, getter(), System.Data.DbType.Int32));

                        }
                        break;
                    case "Decimal":
                        {
                            var getter = Expression.Lambda<Func<decimal>>(func).Compile();
                            (string key, string skey) = engine.EscapeParamKey(typeName: func.Type.Name);
                            ps.SqlStr = skey;
                            ps.Params.Add(new ParamItem(key, getter(), System.Data.DbType.Decimal));

                        }
                        break;
                    case "DateTime":
                        {
                            var getter = Expression.Lambda<Func<DateTime>>(func).Compile();
                            (string key, string skey) = engine.EscapeParamKey(typeName: func.Type.Name);
                            ps.SqlStr = skey;
                            ps.Params.Add(new ParamItem(key, getter(), System.Data.DbType.DateTime));
                        }
                        break;
                    case "Int64":
                        {
                            var getter = Expression.Lambda<Func<long>>(func).Compile();
                            (string key, string skey) = engine.EscapeParamKey(typeName: func.Type.Name);
                            ps.SqlStr = skey;
                            ps.Params.Add(new ParamItem(key, getter(), System.Data.DbType.Int64));

                        }
                        break;
                    case "String[]":
                        {
                            var getter = Expression.Lambda<Func<string[]>>(func).Compile();
                            var arrayData = getter();
                            StringBuilder sb = new StringBuilder();
                            foreach (var item in arrayData)
                            {
                                (string key, string skey) = engine.EscapeParamKey();
                                sb.Append($"{skey},");
                                ps.Params.Add(new ParamItem(key, item, System.Data.DbType.String));
                            }
                            ps.SqlStr = sb.ToString().TrimEnd(',');


                        }
                        break;
                    case "Int32[]":
                        {
                            var getter = Expression.Lambda<Func<int[]>>(func).Compile();
                            var arrayData = getter();
                            StringBuilder sb = new StringBuilder();
                            foreach (var item in arrayData)
                            {
                                (string key, string skey) = engine.EscapeParamKey(typeName: func.Type.Name);
                                sb.Append($"{skey},");
                                ps.Params.Add(new ParamItem(key, item, System.Data.DbType.Int32));
                            }
                            ps.SqlStr = sb.ToString().TrimEnd(',');
                        }
                        break;
                    case "Int64[]":
                        {
                            var getter = Expression.Lambda<Func<long[]>>(func).Compile();
                            var arrayData = getter();
                            StringBuilder sb = new StringBuilder();

                            foreach (var item in arrayData)
                            {
                                (string key, string skey) = engine.EscapeParamKey(typeName: func.Type.Name);
                                sb.Append($"{skey},");
                                ps.Params.Add(new ParamItem(key, item, System.Data.DbType.Int64));
                            }
                            ps.SqlStr = sb.ToString().TrimEnd(',');
                        }
                        break;
                    case "Decimal[]":
                        {
                            var getter = Expression.Lambda<Func<decimal[]>>(func).Compile();
                            var arrayData = getter();
                            StringBuilder sb = new StringBuilder();
                            foreach (var item in arrayData)
                            {
                                (string key, string skey) = engine.EscapeParamKey(typeName: func.Type.Name);
                                sb.Append($"{skey},");
                                ps.Params.Add(new ParamItem(key, item, System.Data.DbType.Decimal));
                            }
                            ps.SqlStr = sb.ToString().TrimEnd(',');

                        }
                        break;
                    default:
                        {
                            var getter = Expression.Lambda<Func<object>>(func).Compile();
                            (string key, string skey) = engine.EscapeParamKey();
                            ps.SqlStr = skey;
                            ps.Params.Add(new ParamItem(key, getter().ToString(), System.Data.DbType.String));
                        }
                        break;
                }
            }
            return ps;
        }


        /// <summary>
        /// 判断包含函数的表达式
        /// </summary>
        /// <param name="func"></param>
        /// <returns></returns>
        private static ParamSql MethodCallExpression(MethodCallExpression func, IDBEngine engine)
        {
            ParamSql ps = new ParamSql();
            string methodName = func.Method.Name;
            switch (methodName)
            {
                case "Contains":
                    {
                        var field_ps = ResolveExpression(func.Object, engine);
                        ps.Params.AddRange(field_ps.Params);
                        var value_ps = ResolveExpression(func.Arguments[0], engine);
                        string key = value_ps.SqlStr.TrimStart('@');
                        string value = QDBTools.SafeKeyWord(value_ps.GetParamValue<string>(key));
                        ps.Params.Add(new ParamItem(key, value));
                        ps.SqlStr = $"{field_ps.SqlStr} like  concat('%',{value_ps.SqlStr},'%')";
                    }
                    break;
                case "StartsWith":
                    {


                        var field_ps = ResolveExpression(func.Object, engine);
                        ps.Params.AddRange(field_ps.Params);
                        var value_ps = ResolveExpression(func.Arguments[0], engine);
                        string key = value_ps.SqlStr.TrimStart('@');
                        string value = QDBTools.SafeKeyWord(value_ps.GetParamValue<string>(key));
                        ps.Params.Add(new ParamItem(key, value));
                        ps.SqlStr = $"{field_ps.SqlStr} like concat({value_ps.SqlStr},'%')";
                    }
                    break;
                case "EndsWith":
                    {
                        var field_ps = ResolveExpression(func.Object, engine);
                        ps.Params.AddRange(field_ps.Params);
                        var value_ps = ResolveExpression(func.Arguments[0], engine);
                        string key = value_ps.SqlStr.TrimStart('@');
                        string value = QDBTools.SafeKeyWord(value_ps.GetParamValue<string>(key));
                        ps.Params.Add(new ParamItem(key, value));
                        ps.SqlStr = $"{field_ps.SqlStr} like  concat('%',{value_ps.SqlStr})";
                    }
                    break;
                case "Equals":
                    {
                        var field_PS = ResolveExpression(func.Object, engine);
                        var value_PS = ResolveExpression(func.Arguments[0], engine);
                        ps.Params.AddRange(field_PS.Params);
                        ps.Params.AddRange(value_PS.Params);
                        ps.SqlStr = $"{field_PS.SqlStr} = {value_PS.SqlStr}";
                    }
                    break;
                case "IsNullOrEmpty":
                    {
                        var field_PS = ResolveExpression(func.Arguments[0], engine);
                        ps.Params.AddRange(field_PS.Params);
                        ps.SqlStr = $"{field_PS.SqlStr} IS NULL or {field_PS.SqlStr} =''";
                    }
                    break;


                default:


                    if (func.Method.ReflectedType.FullName == "Q.DB.Sqm")
                    {
                        switch (func.Method.Name)
                        {
                            case "Count":
                                if (func.Arguments.Count == 0)
                                {
                                    ps.SqlStr = $"COUNT(*)";
                                }
                                else
                                {
                                    var countsql = ResolveExpression(func.Arguments[0], engine);
                                    ps.SqlStr = $"COUNT({countsql.SqlStr})";
                                }
                                break;
                            case "Abs":
                                var abssql = ResolveExpression(func.Arguments[0], engine);
                                ps.SqlStr = $"ABS({abssql.SqlStr})";
                                break;
                            case "Min":
                                var minsql = ResolveExpression(func.Arguments[0], engine);
                                ps.SqlStr = $"MIN({minsql.SqlStr})";
                                break;
                            case "Max":
                                var maxsql = ResolveExpression(func.Arguments[0], engine);
                                ps.SqlStr = $"MAX({maxsql.SqlStr})";
                                break;
                            case "Avg":
                                var avgsql = ResolveExpression(func.Arguments[0], engine);
                                ps.SqlStr = $"AVG({avgsql.SqlStr})";
                                break;
                            case "Sum":
                                var sumsql = ResolveExpression(func.Arguments[0], engine);
                                ps.SqlStr = $"SUM({sumsql.SqlStr})";
                                break;
                            case "Length":
                                var lengthsql = ResolveExpression(func.Arguments[0], engine);
                                ps.SqlStr = $"LENGTH({lengthsql.SqlStr})";
                                break;
                            case "Distinct":
                                var distinctsql = ResolveExpression(func.Arguments[0], engine);
                                ps.SqlStr = $"DISTINCT({distinctsql.SqlStr})";
                                break;
                            case "Concat":
                                var concatsql = ResolveExpression(func.Arguments[0], engine);
                                ps.Params.AddRange(concatsql.Params);
                                ps.SqlStr = $"CONCAT({concatsql.SqlStr})";
                                break;
                            case "Concat_ws":
                                var spsql = ResolveExpression(func.Arguments[0], engine);
                                var concatwssql = ResolveExpression(func.Arguments[1], engine);
                                ps.Params.AddRange(spsql.Params);
                                ps.SqlStr = $"CONCAT_WS({spsql.SqlStr},{concatwssql.SqlStr})";
                                break;
                            case "Format":
                                var formatsql = ResolveExpression(func.Arguments[0], engine);
                                var numSql = ResolveExpression(func.Arguments[1], engine);
                                ps.Params.AddRange(formatsql.Params);
                                ps.Params.AddRange(numSql.Params);
                                ps.SqlStr = $"FORMAT({formatsql.SqlStr},{numSql.SqlStr})";
                                break;
                            case "GroupConcat":
                                var groupConcatsql = ResolveExpression(func.Arguments[0], engine);
                                ps.SqlStr = $"GROUP_CONCAT({groupConcatsql.SqlStr})";
                                break;
                            case "GroupConcat_ws":
                                var groupConcatsql2 = ResolveExpression(func.Arguments[0], engine);
                                var groupConcatspr = ResolveExpression(func.Arguments[1], engine);
                                ps.Params.AddRange(groupConcatspr.Params);
                                ps.SqlStr = $"GROUP_CONCAT({groupConcatsql2.SqlStr}  separator {groupConcatspr.SqlStr})";
                                break;
                            case "IFNULL":
                                var arg1 = ResolveExpression(func.Arguments[0], engine);
                                var arg2 = ResolveExpression(func.Arguments[1], engine);
                                ps.Params.AddRange(arg2.Params);
                                ps.SqlStr = $"IFNULL({arg1.SqlStr},{arg2.SqlStr})";
                                break;
                            case "In":
                                var in_arg1 = ResolveExpression(func.Arguments[0], engine);
                                var invalues = ResolveExpression(func.Arguments[1], engine);
                                ps.Params.AddRange(invalues.Params);
                                ps.SqlStr = $"{in_arg1.SqlStr} In ({invalues.SqlStr})";
                                break;
                            case "NotIn":
                                var notin_arg1 = ResolveExpression(func.Arguments[0], engine);
                                var notinvalues = ResolveExpression(func.Arguments[1], engine);
                                ps.Params.AddRange(notinvalues.Params);
                                ps.SqlStr = $"{notin_arg1.SqlStr} Not In ({notinvalues.SqlStr})";
                                break;
                            case "IsNullOrEmpty":
                                var isNullOrEmpty_arg = ResolveExpression(func.Arguments[0], engine);
                                ps.SqlStr = $"{isNullOrEmpty_arg.SqlStr} IS NULL OR {isNullOrEmpty_arg.SqlStr} =''";
                                break;
                            case "IsNotNullOrEmpty":
                                var isNotNullOrEmpty_arg = ResolveExpression(func.Arguments[0], engine);
                                ps.SqlStr = $"{isNotNullOrEmpty_arg.SqlStr} IS NOT NULL AND {isNotNullOrEmpty_arg.SqlStr} <>''";
                                break;
                            case "TableAll":
                                var tableName = EntityCache.TryGetTableName(func.Arguments[0].Type);
                                if (string.IsNullOrEmpty(tableName))
                                {
                                    ps.SqlStr = "*";
                                }
                                else
                                {
                                    ps.SqlStr = $"{engine.EscapeStr(tableName)}.*";
                                }
                                break;
                            default:
                                break;
                        }
                    }
                    else
                    {
                        if (func.Type == typeof(DateTime))
                        {
                            var invokeResult = Expression.Lambda<Func<DateTime>>(func).Compile().DynamicInvoke();
                            (string key, string skey) = engine.EscapeParamKey(typeName: func.Type.Name);
                            ps.SqlStr = skey;
                            ps.Params.Add(new ParamItem(key, invokeResult, System.Data.DbType.DateTime));
                        }
                        else
                        {
                            var result = Expression.Lambda(func).Compile().DynamicInvoke();

                            if (func.Type == typeof(ParamSql))
                            {
                                ps = (ParamSql)result;
                            }
                            else if (func.Type.BaseType == typeof(Array))
                            {
                                var arrayResult = (Array)result;

                                string sqlStr = "";
                                foreach (var item in arrayResult)
                                {
                                    (string key, string skey) = engine.EscapeParamKey();
                                    sqlStr += "," + skey;
                                    ps.Params.Add(new ParamItem(key, item));
                                }
                                ps.SqlStr = sqlStr.TrimStart(',');
                            }
                            else
                            {
                                (string key, string skey) = engine.EscapeParamKey();
                                ps.SqlStr = skey;
                                ps.Params.Add(new ParamItem(key, result));
                            }
                        }
                    }
                    break;
            }

            return ps;
        }


        /// <summary>
        /// 生成操作符
        /// </summary>
        /// <param name="expressiontype"></param>
        /// <returns></returns>
        private static string GetOperator(ExpressionType expressiontype)
        {
            switch (expressiontype)
            {
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                    return " AND ";
                case ExpressionType.Equal:
                    return " = ";
                case ExpressionType.GreaterThan:
                    return " > ";
                case ExpressionType.GreaterThanOrEqual:
                    return " >= ";
                case ExpressionType.LessThan:
                    return " < ";
                case ExpressionType.LessThanOrEqual:
                    return " <= ";
                case ExpressionType.NotEqual:
                    return " <> ";
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    return " OR ";
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                    return "+";
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                    return "-";
                case ExpressionType.Divide:
                    return "/";
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                    return "*";
                case ExpressionType.Not:
                    return "Not";
                default:
                    return "";
            }
        }

        private static bool IsNumberType(this Type dataType)
        {
            if (dataType == null)
                return false;

            return (dataType == typeof(int)
                    || dataType == typeof(double)
                    || dataType == typeof(Decimal)
                    || dataType == typeof(long)
                    || dataType == typeof(short)
                    || dataType == typeof(float)
                    || dataType == typeof(Int16)
                    || dataType == typeof(Int32)
                    || dataType == typeof(Int64)
                    || dataType == typeof(uint)
                    || dataType == typeof(UInt16)
                    || dataType == typeof(UInt32)
                    || dataType == typeof(UInt64)
                    || dataType == typeof(Single)
                   );
        }
        #endregion

    }
}
