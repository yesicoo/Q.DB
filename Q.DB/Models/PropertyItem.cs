/*************************************************************************************
 *
 * 文 件 名:   PropertyItem.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/7 11:12
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
    internal class PropertyItem
    {
        internal string Name { get; set; }
        internal PropertyInfo Info { get; set; }
    }
}
