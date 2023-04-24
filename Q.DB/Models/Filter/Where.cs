/*************************************************************************************
 *
 * 文 件 名:   Where.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/13 11:32
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

namespace Q.DB.Models.Filter
{
    public class Where
    {
        public string FieldName {  get; set; }
        public string Condition {  get; set; }
        public object Value {  get; set; }
    }
}
