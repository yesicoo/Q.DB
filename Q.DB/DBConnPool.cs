/*************************************************************************************
 *
 * 文 件 名:   DBConnPool.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/7 14:18
 * 
 * ======================================
 * 历史更新记录
 * 版本：V          修改时间：         修改人：
 * 修改内容：
 * ======================================
 * 
*************************************************************************************/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Q.DB.Interface;
using Q.DB.Models;

namespace Q.DB
{
    public class DBConnPool
    {
        object _locker = new object();
        IDBEngine DBEngine = null;
        public DBConnPool(IDBEngine engine)
        {
            DBEngine = engine;
        }
        List<DBConnectionItem> _DBConnectionItems = new List<DBConnectionItem>(10);

        public DBConnectionItem GetConnection(string dbname)
        {
            DBConnectionItem item;
            lock (_locker)
            {
                item = _DBConnectionItems.FirstOrDefault(x => x.Available);
                if (item != null)
                {
                    item.Available = false;
                }
            }
            if (item != null)
            {
                lock (item)
                {
                    if (DBEngine.CheckConn(item.SqlConnection))
                    {
                        if (dbname != null && item.SqlConnection.Database != dbname)
                        {
                            item.SqlConnection.ChangeDatabase(dbname);
                        }
                        return item;
                    }
                    else
                    {
                        _DBConnectionItems.Remove(item);
                        item.Dispose();
                    }
                }
            }

            if (item == null || !item.Available)
            {
                item = new DBConnectionItem();
                item.SqlConnection = DBEngine.CreateConn();
                item.SqlConnection.Open();
                if (dbname != null && item.SqlConnection.Database != dbname)
                {
                    item.SqlConnection.ChangeDatabase(dbname);
                }
                item.Available = false;
                _DBConnectionItems.Add(item);
            }
            return item;
        }
    }
}
