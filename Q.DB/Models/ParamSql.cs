/*************************************************************************************
 *
 * 文 件 名:   ParamSql.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/13 11:23
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
using System.Text;
using System.Threading.Tasks;

namespace Q.DB.Models
{
    public class ParamSql
    {
        public string SqlStr { get; set; }
        public List<ParamItem> Params { get; set; } = new List<ParamItem>();
        public ParamSql(string sql)
        {
            this.SqlStr = sql;
        }
        public ParamSql(string sql, ParamItem item)
        {
            this.SqlStr = sql;
            this.Params.Add(item);
        }
        public ParamSql()
        {
        }
        public void AppendParams(IEnumerable<ParamItem> dbparams)
        {
            Params.AddRange(dbparams);
        }
        public void AppendParams(string sql, IEnumerable<ParamItem> dbparams)
        {
            this.SqlStr += sql;
            Params.AddRange(dbparams);
        }
        internal T GetParamValue<T>(string name)
        {
            var item = Params.FirstOrDefault(x => x.Name == name);
            if(item == null)
            {
                return default;
            }
            return (T)Convert.ChangeType(item.Value, typeof(T));
        }
    }
}
