/*************************************************************************************
 *
 * 文 件 名:   Sqm.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/20 11:07
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
using System.Text;
using System.Threading.Tasks;
using Q.DB.Models;

namespace Q.DB
{
    public static class Sqm
    {
        public static T Abs<T>(T f)
        {
            return default(T);
        }
        public static int Count(object f)
        {
            return 0;
        }
        public static int Count()
        {
            return 0;
        }
        public static object IFNULL(object value1, object defaultvalue)
        {
            return null;
        }

        public static T Min<T>(T f)
        {
            return default(T);
        }
        public static T Max<T>(T f)
        {
            return default(T);
        }
        public static T Avg<T>(T f)
        {
            return default(T);
        }
        public static T Sum<T>(T f)
        {
            return default(T);
        }
        public static int Length(object f)
        {
            return 0;
        }

        public static object Distinct(object f)
        {
            return null;
        }

        public static object Concat(params object[] f)
        {
            return null;
        }

        /// <summary>
        /// 字段连接
        /// </summary>
        /// <param name="spStr">间隔字符</param>
        /// <param name="f"></param>
        /// <returns></returns>
        public static object Concat_ws(string spStr, params object[] f)
        {
            return null;
        }

        public static object GroupConcat(object f)
        {
            return null;
        }
        public static object GroupConcat_ws(object f, string spr)
        {
            return null;
        }

        public static bool In(object f, params object[] value)
        {
            return true;
        }
        public static bool In(object f, ParamSql QingQuery)
        {
            return true;
        }
        public static bool NotIn(object f, params object[] value)
        {
            return true;
        }
        //public static bool NotIn(object f, ParamSql QingQuery)
        //{
        //    return true;
        //}
        public static bool IsNullOrEmpty(object f)
        {
            return true;
        }
        public static bool IsNotNullOrEmpty(object f)
        {
            return true;
        }

        /// <summary>
        /// Table.* 全部字段的意思
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="f"></param>
        /// <returns></returns>
        public static object TableAll<T>(T x)
        {
            return null;
        }
        public static object Format(object f, int num)
        {
            return null;
        }
    }
}
