/*************************************************************************************
 *
 * 文 件 名:   PageResult.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/13 11:29
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
    public class PageResult<T>
    {
        public long TotalCount { set; get; }
        public long CurrentPageNum { set; get; }
        public long CurrentPageSize { set; get; }
        public long TotalPages { get { return CurrentPageSize > 0 ? ((TotalCount + CurrentPageSize - 1) / CurrentPageSize) : 0; } }
        public IEnumerable<T> ArrayData { set; get; }
    }
}
