/*************************************************************************************
 *
 * 文 件 名:   DBConnInfo.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/6 16:12
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

namespace Q.DB.Models
{
    internal class DBConnInfo
    {

        public string Tag { set; get; }
        public string DBType { set; get; }
        public string DBConnStr { set; get; }
        public string DefaultDBName { get; internal set; }
    }
}
