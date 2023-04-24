/*************************************************************************************
 *
 * 文 件 名:   SqlLogUtil.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/19 15:58
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
    public static class SqlLogUtil
    {
        public static Action<LogType, string> PrintOut;
        public static decimal SqlLongTime = 0;
        public  static void SendLog(LogType Type, string Log, decimal interval = 0)
        {
            if (Type == LogType.SQL)
            {
                if (interval >= SqlLongTime)
                    PrintOut?.Invoke(Type, $"[{interval} ms] {Log}");
            }
            else
            {
                PrintOut?.Invoke(Type, Log);
            }
        }
    }
    public enum LogType
    {
        Error,
        SQL,
        Msg

    }
}
