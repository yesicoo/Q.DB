/*************************************************************************************
 *
 * 文 件 名:   QingAttribute.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/19 17:47
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

namespace Q.DB
{
    //表
    [AttributeUsage(AttributeTargets.Class)]
    public class QingEntityAttribute: Attribute
    {
        public string TableName { set; get; }
        public string Remark { set; get; }
    }


    //索引
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class QingIndexAttribute : Attribute
    {
        public string IndexName { set; get; }
        public string IndexType { set; get; }
        public string IndexFields { set; get; }
    }
    //字段
    [AttributeUsage(AttributeTargets.Property)]
    public class QingFieldAttribute : Attribute
    {
        /// <summary>
        /// 字段名称
        /// </summary>
        public string FieldName { set; get; }
        /// <summary>
        /// 数据库类型
        /// </summary>
        public string DBType { set; get; }
        /// <summary>
        /// 长度
        /// </summary>
        public string Length { set; get; }
        /// <summary>
        /// 是否主键
        /// </summary>
        public bool IsPrimaryKey { set; get; } = false;
        /// <summary>
        /// 是否自增
        /// </summary>
        public bool IsAutoIncrement { set; get; } = false;
        /// <summary>
        /// 是否排除
        /// </summary>
        public bool IsExclude { set; get; } = false;
        public bool Nullable { set; get; } = true;
        public string Remark { set; get; }
    }
}
