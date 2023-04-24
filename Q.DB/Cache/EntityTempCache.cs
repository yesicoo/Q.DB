/*************************************************************************************
 *
 * 文 件 名:   EntityTempCache.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/21 15:46
 * 
 * ======================================
 * 历史更新记录
 * 版本：V          修改时间：         修改人：
 * 修改内容：
 * ======================================
 * 
*************************************************************************************/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using Q.DB.Extenson;
using Q.DB.Interface;
using Q.DB.Models;

namespace Q.DB.Cache
{
    public class EntityTempCache<T> : IEntityCache<T>
    {
        ConcurrentDictionary<TimeKey, List<T>> _dts = new ConcurrentDictionary<TimeKey, List<T>>();
        Expression<Func<T, bool>> _loadExpression = null;
        Expression<Func<T, object>> _orderyExpression = null;
        bool _orderDesc = false;
        int _timeOut = 30;
        string DBName = string.Empty;
        System.Timers.Timer _timer = new System.Timers.Timer();



        public EntityTempCache()
        {
            StartTimer();
        }
        public EntityTempCache(int min)
        {
            _timeOut = min;
            StartTimer();
        }

      
        private List<T> Load(string dbID = null,string tag =null)
        {
            using (DBContext context = new DBContext(dbID, tag))
            {
                DBName = context.DBName;
                List<T> tResult = null;

                var query = context.Query<T>();
                if (_loadExpression != null)
                {
                    query = query.Where(_loadExpression);
                }

                if (_orderyExpression != null)
                {
                    if (!_orderDesc)
                    {
                        query = query.OrderBy(_orderyExpression);
                    }
                    else
                    {
                        query = query.OrderByDesc(_orderyExpression);
                    }
                }
                tResult = query.QueryAll().ToList();


           
                var timeKey = new TimeKey() { Key = DBName, LastTime = DateTime.Now };
                _dts.AddOrUpdate(timeKey, tResult, (oldkey, oldvalue) => tResult);
                SqlLogUtil.SendLog(LogType.Msg, $"加载 {timeKey.Key}[{timeKey.LastTime}]({EntityCache.TryGetTableName<T>() })临时缓存({_timeOut}min) 共{tResult.Count}条");
                return tResult;
            }
        }

        private void StartTimer()
        {
            _timer.Interval = 60000;
            _timer.Elapsed += _timer_Elapsed;
            _timer.Start();
        }

        /// <summary>
        /// 超时删除
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void _timer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            var exprieKeys = _dts.Where(x => x.Key.LastTime < DateTime.Now.AddMinutes(-_timeOut)).Select(x => x.Key);
            foreach (var exprieKey in exprieKeys)
            {
                SqlLogUtil.SendLog(LogType.Msg, $"{exprieKey.Key}[{exprieKey.LastTime}]({EntityCache.TryGetTableName<T>()})缓存超时 清理数据");
                _dts.TryRemove(exprieKey, out var removeData);
            }
        }

        /// <summary>
        /// 初始化
        /// </summary>
        /// <param name="expression">加载条件</param>
        /// <param name="min">数据缓存时常(分钟) 默认30</param>
        public void Init(Expression<Func<T, bool>> expression, int min, Expression<Func<T, object>> orderyExpression = null, bool desc = false)
        {
            _loadExpression = expression;
            _timeOut = min;
            _orderyExpression = orderyExpression;
            _orderDesc = desc;
            StartTimer();
        }
        public void Init(Expression<Func<T, bool>> expression, Expression<Func<T, object>> orderyExpression = null, bool desc = false)
        {
            _loadExpression = expression;
            _orderyExpression = orderyExpression;
            _orderDesc = desc;
            StartTimer();
        }
        public List<T> QueryAll(string dbID = null, string tag = null)
        {
            List<T> ts = null;
            string dbkey = dbID ?? DBName;
            var timeKey = _dts.Keys.FirstOrDefault(x => x.Key == dbkey);
            if (timeKey != null)
            {
                if (!_dts.TryGetValue(timeKey, out ts))
                {
                    ts = Load(dbID,tag);
                }
                else
                {
                    timeKey.LastTime = DateTime.Now;
                }
            }
            else
            {
                ts = Load(dbID,tag);
            }
            return ts;
        }
        public List<T> Query(Expression<Func<T, bool>> expression,string dbID = null, string tag = null)
        {
            return QueryAll(dbID,tag).Where(expression.Compile()).ToList();
        }

        public T QueryFirst(Expression<Func<T, bool>> expression = null, Expression<Func<T, object>> orderyExpression = null, bool orderByDesc = false,string dbID = null, string tag = null)
        {
            var query = QueryAll(dbID,tag) as IEnumerable<T>;
            if (expression != null)
            {
                query = query.Where(expression.Compile());

            }
            if (orderyExpression != null)
            {
                if (!orderByDesc)
                {
                    query = query.OrderBy(orderyExpression.Compile());
                }
                else
                {
                    query = query.OrderByDescending(orderyExpression.Compile());
                }
            }
            return query.FirstOrDefault();
        }
        public PageResult<T> QueryPages(int pageNum, int pageSize, Expression<Func<T, bool>> expression = null, Expression<Func<T, object>> orderyExpression = null, bool orderByDesc = false,string dbID = null, string tag = null)
        {
            PageResult<T> pageResult = new PageResult<T>();
            var ds = QueryAll(dbID,tag);

            pageResult.CurrentPageNum = pageNum;
            pageResult.CurrentPageSize = pageSize;
            pageResult.ArrayData = ds;
            if (expression != null)
            {
                pageResult.ArrayData = pageResult.ArrayData.Where(expression.Compile());
            }
            pageResult.TotalCount = pageResult.ArrayData.Count();
            if (orderyExpression != null)
            {
                if (orderByDesc)
                {
                    pageResult.ArrayData = pageResult.ArrayData.OrderByDescending(orderyExpression.Compile());
                }
                else
                {
                    pageResult.ArrayData = pageResult.ArrayData.OrderBy(orderyExpression.Compile());
                }
            }

            pageResult.ArrayData = pageResult.ArrayData.Skip((pageNum - 1) * pageSize).Take(pageSize);
            return pageResult;
        }



        public int Add(T t,string dbID = null, string tag = null)
        {
            var ds = QueryAll(dbID,tag);
            var nea = EntityCache.TryGetInfo<T>();
            using (DBContext context = new DBContext(dbID))
            {
                int result = context.Insert(t);
                if (result > 0)
                {

                    var autoPK = nea.AutoIncrements.Intersect(nea.PrimaryKeys).FirstOrDefault();
                    if (!string.IsNullOrEmpty(autoPK))
                    {
                        nea.PropertyInfos[autoPK].SetValue(t, result);
                    }

                    lock (ds)
                    {

                        //判断新增的是否符合初始化的时候设置的限定条件
                        ds.AddRange(ExpressionCheck(t));
                        ds.DoSort(_orderyExpression, _orderDesc);
                    }
                    return result;
                }
                else
                {
                    return 0;
                }

            }
        }
        public bool AddRange(IEnumerable<T> ts,string dbID = null, string tag = null)
        {
            var ds = QueryAll(dbID,tag);
            var nea = EntityCache.TryGetInfo<T>();

            using (DBContext context = new DBContext(dbID))
            {
                var autoPK = nea.AutoIncrements.Intersect(nea.PrimaryKeys).FirstOrDefault();
                if (!string.IsNullOrEmpty(autoPK))
                {
                    foreach (var t in ts)
                    {
                        int id = Add(t, dbID);
                        if (id < 1)
                        {
                            return false;
                        }
                        else
                        {
                            nea.PropertyInfos[autoPK].SetValue(t, id);
                        }
                    }
                    return true;
                }
                else
                {
                    if (context.BatchInsert(ts.ToList()) > 0)
                    {
                        lock (ds)
                        {
                            ds.AddRange(ExpressionCheck(ts.ToArray()));
                            ds.DoSort(_orderyExpression, _orderDesc);
                        }
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
        }

        public List<T> ReLoad(string dbID = null, string tag = null)
        {
            string dbkey = dbID ?? DBName;
            var timeKey = _dts.Keys.FirstOrDefault(x => x.Key == dbkey);
            if (timeKey != null)
            {
                _dts.TryRemove(timeKey, out var removeData);
            }
            return QueryAll(dbID,tag);

        }





        public bool RefreshItems(Expression<Func<T, bool>> expression, string dbID = null, string tag = null)
        {
            var ds = QueryAll(dbID, tag);
            lock (ds)
            {
                ds.RemoveAll(x => expression.Compile().Invoke(x));
                using (DBContext dbContext = new DBContext(dbID, tag))
                {
                    ds.AddRange(ExpressionCheck(dbContext.Query<T>().QueryAll(expression).ToArray()));
                }
                ds.DoSort(_orderyExpression, _orderDesc);
                return true;
            }
        }

       

        public int Remove(Expression<Func<T, bool>> expression,string dbID = null, string tag = null)
        {
            var ds = QueryAll(dbID,tag);
            lock (ds)
            {
                var delds = ds.Where(expression.Compile());

                using (DBContext context = new DBContext(dbID))
                {
                    if (context.Delete(expression) > 0)
                    {
                        ds.RemoveAll(x => expression.Compile().Invoke(x));
                        return 1;
                    }
                    else
                    {
                        return -1;
                    }
                }
            }
        }

        public int Update(Expression<Func<T, T>> expressionNew, Expression<Func<T, bool>> expressionWhere,string dbID = null, string tag = null)
        {

            var ds = QueryAll(dbID,tag);
            lock (ds)
            {
                using (DBContext context = new DBContext(dbID))
                {
                    if (context.Update(expressionNew, expressionWhere) > 0)
                    {
                        ds.RemoveAll(x => expressionWhere.Compile().Invoke(x));
                        ds.AddRange(ExpressionCheck(context.Query<T>().QueryAll(expressionWhere).ToArray()));
                        ds.DoSort(_orderyExpression, _orderDesc);
                        return 1;
                    }
                    else
                    {
                        return -1;
                    }
                }
            }
        }


        private IEnumerable<T> ExpressionCheck(params T[] ts)
        {
            if (_loadExpression != null)
            {
                return ts.Where(_loadExpression.Compile());
            }
            else
            {
                return ts;
            }
        }


    }

    internal class TimeKey
    {
        internal string Key { set; get; }
        internal DateTime LastTime { set; get; }
    }
}
