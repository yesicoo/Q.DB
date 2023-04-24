/*************************************************************************************
 *
 * 文 件 名:   ParamItem.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/15 14:51
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
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Q.DB.Models
{
    public class ParamItem
    {
        public string Name { get; set; }
        public object Value { get; set; }
        public DbType DbType { get; set; }
        public ParamItem() { }
        public ParamItem(string name, object value)
        {
            Name = name;
            Value = value;
        }
        public ParamItem(string name, object value,DbType dbType)
        {
            Name = name;
            Value = value;
            DbType = dbType;
        }
     
    }
}
