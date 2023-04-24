/*************************************************************************************
 *
 * 文 件 名:   EntityInfo.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/15 15:42
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
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Q.DB.Models
{
    public class EntityInfo
    {

        public string Name { set; get; }
        public string TableName { set; get; }
        public string Remark { get; set; }

        public List<string> PrimaryKeys { get; set; } = new List<string>();
        public List<string> AutoIncrements { get; set; } = new List<string>();
        public Dictionary<string, DBFieldInfo> PropertyInfos { set; get; } = new Dictionary<string, DBFieldInfo>();
    }


    public class DBIndexInfo
    {
        public string IndexName { set; get; }
        public string IndexType { set; get; }
        public string IndexFields { set; get; }
    }
    public class DBFieldInfo
    {
        public string Name { get; }
        public string DBFieldName { get; }
        public string DBType { get; }
        public bool Nullable { get; }
        public string DBLength { get; }
        public string Remark { get; set; }

        public Type PropertyType;
        public Action<object, object> SetValue;
        public Func<object, object> GetValue;

        public DBFieldInfo(string name, Type propertyType, Action<object, object> setValue, Func<object, object> getValue, string dbFieldName, string remark = null, string dbType = null, string dbLength = null,bool nullable=true)
        {
            this.Name = name;
            this.DBFieldName = dbFieldName;
            this.DBType = dbType;
            this.DBLength = dbLength;
            this.PropertyType = propertyType;
            this.SetValue = setValue;
            this.GetValue = getValue;
            this.Remark = remark;
            this.Nullable= nullable;
        }

    }
}
