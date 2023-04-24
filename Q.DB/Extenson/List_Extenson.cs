/*************************************************************************************
 *
 * 文 件 名:   List_Extenson.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/21 15:32
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

namespace Q.DB.Extenson
{
    internal static class List_Extenson
    {
        internal static IEnumerable<T> QueryWithPage<T>(this IEnumerable<T> ts, int pageNum, int pageSize)
        {
            pageNum = pageNum <= 0 ? 1 : pageNum;
            pageSize = pageSize <= 0 ? 1 : pageSize;

            return ts?.Skip((pageNum - 1) * pageSize)?.Take(pageSize);
        }

        internal static IEnumerable<T> DoSort<T>(this IEnumerable<T> ts, Expression<Func<T, object>> expression, bool desc = false)
        {
            if (expression != null)
            {
                if (!desc)
                {
                    ts = ts.OrderBy(expression.Compile()).ToList();
                }
                else
                {
                    ts = ts.OrderByDescending(expression.Compile()).ToList();
                }
            }
            return ts;
        }
    }
}
