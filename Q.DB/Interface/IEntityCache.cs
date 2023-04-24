/*************************************************************************************
 *
 * 文 件 名:   IEntityCache.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/21 15:10
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
using Q.DB.Models;

namespace Q.DB.Interface
{
    interface IEntityCache<T>
    {
        /// <summary>
        /// 初始化
        /// </summary>
        /// <param name="expression"></param>
        void Init(Expression<Func<T, bool>> expression, Expression<Func<T, object>> orderyExpression = null, bool desc = false);
        /// <summary>
        /// 重新载入
        /// </summary>
        /// <param name="dbID"></param>
        /// <returns></returns>
        List<T> ReLoad(string dbID = null, string tag = null);
        /// <summary>
        /// 条件查询
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="dbID"></param>
        /// <returns></returns>
        List<T> Query(Expression<Func<T, bool>> expression, string dbID = null, string tag = null);
        /// <summary>
        /// 条件排序查询第一个
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="orderyExpression"></param>
        /// <param name="orderByDesc"></param>
        /// <param name="dbID"></param>
        /// <returns></returns>
        T QueryFirst(Expression<Func<T, bool>> expression = null, Expression<Func<T, object>> orderyExpression = null, bool orderByDesc = false, string dbID = null, string tag = null);
        /// <summary>
        /// 查询所有
        /// </summary>
        /// <param name="dbID"></param>
        /// <returns></returns>
        List<T> QueryAll(string dbID=null, string tag = null);

        PageResult<T> QueryPages(int pageNum, int pageSize, Expression<Func<T, bool>> expression = null, Expression<Func<T, object>> orderyExpression = null, bool orderByDesc = false, string dbID = null,string tag=null);


        int Add(T t, string dbID = null, string tag = null);
        bool AddRange(IEnumerable<T> ts, string dbID = null, string tag = null);

        int Remove(Expression<Func<T, bool>> expression, string dbID = null, string tag = null);

        int Update(Expression<Func<T, T>> expressionNew, Expression<Func<T, bool>> expressionWhere, string dbID = null, string tag = null);

        bool RefreshItems(Expression<Func<T, bool>> expression, string dbID = null, string tag = null);

    }
}
