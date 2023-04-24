/*************************************************************************************
 *
 * 文 件 名:   Tools.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/15 14:33
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
using System.Data.SqlTypes;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Q.DB.Interface;
using Q.DB.Models;

namespace Q.DB
{
    public static class QDBTools
    {
        #region 随机生成字符串
        /// <summary>
        /// 随机生成字符串
        /// </summary>
        /// <param name="num">位数</param>
        /// <param name="type">
        /// 0 -区分大小写字符包含数字的随机字符串
        /// 1 -小写字符含数字字符串
        /// 2 -大写字符包含数字字符串
        /// 3 -小写字符随机字符串
        /// 4 -大写字符随机字符串
        /// 5 -仅数字
        /// </param>
        /// <returns></returns>
        public static string RandomCode(int num, int type = 0, bool firstUpper = false)
        {
            string chars = null;

            switch (type)
            {
                case 0:
                    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghicklnopqrstuvwxyz1234567890";
                    break;
                case 1:
                case 2:
                    chars = "abcdefghicklnopqrstuvwxyz1234567890";
                    break;
                case 3:
                case 4:
                    chars = "abcdefghicklnopqrstuvwxyz";
                    break;
                case 5:
                    chars = "1234567890";
                    break;
                default:
                    break;
            }
            if (!string.IsNullOrEmpty(chars))
            {
                int length = chars.Length;
                StringBuilder sb = new StringBuilder();

                for (int i = 0; i < num; i++)
                {
                    Random r = new Random(BitConverter.ToInt32(Guid.NewGuid().ToByteArray(), 0));
                    if (firstUpper && i == 0)
                    {
                        sb.Append("abcdefghicklnopqrstuvwxyz"[r.Next(0, 25)]);
                    }
                    else
                    {
                        sb.Append(chars[r.Next(0, length - 1)]);
                    }
                }
                var result = sb.ToString();
                if (type == 2 || type == 4)
                {
                    result = result.ToUpper();
                }
                return result;
            }
            else
            {
                return null;
            }
        }

        #endregion

        internal static string SafeKeyWord(string keyword)
        {
            return keyword.Replace("\\", "\\\\").Replace("%", "\\%").Replace("'", "\\'").Replace("_", "\\_");
        }

        public static string ConvertSuffixTableName(string tableSuffix)
        {
            string tasbSuffixStr = string.Empty;
            if (!string.IsNullOrEmpty(tableSuffix))
            {
                if (tableSuffix.Contains("["))
                {
                    if (tableSuffix.Contains("[DAY]"))
                    {
                        tasbSuffixStr = tableSuffix.Replace("[DAY]", DateTime.Now.ToString("yyyyMMdd"));
                    }
                    else if (tableSuffix.Contains("[MONTH]"))
                    {
                        tasbSuffixStr = tableSuffix.Replace("[MONTH]", DateTime.Now.ToString("yyyyMM"));
                    }
                    else if (tableSuffix.Contains("[YEAR]"))
                    {
                        tasbSuffixStr = tableSuffix.Replace("[YEAR]", DateTime.Now.ToString("yyyy"));
                    }
                    else if (tableSuffix.Contains("[HOUR]"))
                    {
                        tasbSuffixStr = tableSuffix.Replace("[HOUR]", DateTime.Now.ToString("yyyyMMddHH"));
                    }
                    else if (tableSuffix.Contains("[WEEK]"))
                    {
                        DateTime dt = DateTime.Today;
                        int count = dt.DayOfWeek - DayOfWeek.Monday;
                        if (count == -1) count = 6;
                        string res = dt.AddDays(-count).ToString("yyyyMMdd");

                        tasbSuffixStr = tableSuffix.Replace("[WEEK]", res);
                    }
                    else if (tableSuffix.Contains("[WEEK7]"))
                    {
                        string res = "";
                        DateTime dt = DateTime.Today;
                        if (dt.DayOfWeek == DayOfWeek.Sunday)
                            res = dt.ToString("yyyyMMdd");
                        else
                            res = dt.AddDays(7 - (dt.DayOfWeek - DayOfWeek.Sunday)).ToString("yyyyMMdd");
                        tasbSuffixStr = tableSuffix.Replace("[WEEK7]", res);
                    }
                    else
                    {
                        tasbSuffixStr = tableSuffix;
                    }
                }
                else
                {
                    tasbSuffixStr = tableSuffix;
                }
            }
            return tasbSuffixStr;
        }

        internal static ParamSql CreateCompareChangeSql<T>(T dest, T source, Expression<Func<T, bool>> where, IDBEngine DBEngine, string tableSuffix = null)
        {
            bool changed = false;
            ParamSql ps = new ParamSql();
            var nea = EntityCache.TryGetInfo<T>();
            string tableName = DBEngine.EscapeStr(nea.TableName + ConvertSuffixTableName(tableSuffix));
            StringBuilder sb_SQL = new StringBuilder(string.Format($"Update {tableName} set "));
            PropertyInfo[] pros = dest.GetType().GetProperties();
            foreach (var item in nea.PropertyInfos)
            {


                if (item.Value.PropertyType == typeof(DateTime))
                {
                    var selfValue = Convert.ToDateTime(item.Value.GetValue(dest)).ToString("yyyy-MM-dd HH:mm:ss");
                    var sourceValue = Convert.ToDateTime(item.Value.GetValue(source)).ToString("yyyy-MM-dd HH:mm:ss");
                    if (selfValue != sourceValue)
                    {
                        (string key, string skey) = DBEngine.EscapeParamKey(item.Key);
                        sb_SQL.Append($"{DBEngine.EscapeStr(item.Key)}={skey},");
                        ps.Params.Add(new ParamItem(key, selfValue));
                        changed = true;
                    }
                }
                else if (item.Value.PropertyType == typeof(decimal))
                {
                    var selfValue = Convert.ToDecimal(item.Value.GetValue(dest)).ToString("0.###################");
                    var sourceValue = Convert.ToDecimal(item.Value.GetValue(source)).ToString("0.###################");
                    if (selfValue != sourceValue)
                    {
                        (string key, string skey) = DBEngine.EscapeParamKey(item.Key);
                        sb_SQL.Append($"{DBEngine.EscapeStr(item.Key)}={skey},");
                        ps.Params.Add(new ParamItem(key, selfValue));
                        changed = true;
                    }
                }
                else
                {
                    var selfValue = item.Value.GetValue(dest)?.ToString();
                    var sourceValue = item.Value.GetValue(source)?.ToString();
                    if (selfValue != sourceValue)
                    {
                        (string key, string skey) = DBEngine.EscapeParamKey(item.Key);
                        sb_SQL.Append($"{DBEngine.EscapeStr(item.Key)}={skey},");
                        ps.Params.Add(new ParamItem(key, selfValue));
                        changed = true;
                    }
                }



            }
            if (changed)
            {
                var sqlWhere = ExpressionResolver.ResolveExpression(where.Body, DBEngine);
                ps.Params.AddRange(sqlWhere.Params);
                string whereSql = sqlWhere.SqlStr;
                if (!string.IsNullOrEmpty(tableSuffix))
                {
                    whereSql = whereSql.Replace(DBEngine.EscapeStr(nea.Name), tableName);
                }
                ps.SqlStr = $"{sb_SQL.ToString().TrimEnd(',')}  where {whereSql};";
            }
            else
            {
                ps.SqlStr = null;
            }

            return ps;
        }

        public static (string, string) ConvertToInsertCvs<T>(IEnumerable<T> ts, char splitChar = ',')
        {
            var nea = EntityCache.TryGetInfo<T>();

            StringBuilder sb_fields = new StringBuilder();
            foreach (var item in nea.PropertyInfos)
            {
                if (nea.AutoIncrements.Contains(item.Key))
                {
                    continue;
                }

                sb_fields.Append($"{splitChar}{item.Key}");
            }
            List<string> datas = new List<string>();
            foreach (var t in ts)
            {
                StringBuilder sb = new StringBuilder();
                foreach (var item in nea.PropertyInfos)
                {
                    if (nea.AutoIncrements.Contains(item.Key))
                    {
                        continue;
                    }

                    if (item.Value.PropertyType == typeof(DateTime))
                    {
                        sb.Append($"{splitChar}\"{((DateTime)(item.Value.GetValue(t))).ToString("yyyy-MM-dd HH:mm:ss")}\"");
                    }
                    else
                    {
                        sb.Append($"{splitChar}\"{item.Value.GetValue(t)}\"");
                    }
                }
                datas.Add(sb.ToString().TrimStart(splitChar));
            }
            return (sb_fields.ToString().TrimStart(splitChar), string.Join("\r\n", datas));
        }

        public static List<string> ConvertToInsertCvsWriteToFile<T>(IEnumerable<T> ts, string filePath, char splitChar = ',')
        {
            var nea = EntityCache.TryGetInfo<T>();

            List<string> fields = new List<string>();
            foreach (var item in nea.PropertyInfos)
            {
                if (nea.AutoIncrements.Contains(item.Key))
                {
                    continue;
                }

                fields.Add(item.Key);
            }
            using (StreamWriter sw = new StreamWriter(filePath))
            {
                foreach (var t in ts)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach (var item in nea.PropertyInfos)
                    {
                        if (nea.AutoIncrements.Contains(item.Key))
                        {
                            continue;
                        }

                        if (item.Value.PropertyType == typeof(DateTime))
                        {
                            sb.Append($"{splitChar}\"{((DateTime)(item.Value.GetValue(t))).ToString("yyyy-MM-dd HH:mm:ss")}\"");
                        }
                        else
                        {
                            sb.Append($"{splitChar}\"{item.Value.GetValue(t)}\"");
                        }
                       
                    }
                    sw.Write(sb.Append("\r\n").ToString().TrimStart(splitChar));
                }
            }
            return  fields;
        }

    }
}
