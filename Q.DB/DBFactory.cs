/*************************************************************************************
 *
 * 文 件 名:   DBFactory.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/11/25 15:06
 * 
 * ======================================
 * 历史更新记录
 * 版本：V          修改时间：         修改人：
 * 修改内容：
 * ======================================
 * 
*************************************************************************************/

using Q.DB.Interface;
using Q.DB.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Q.DB
{
    public class DBFactory
    {
        Dictionary<string, IDBEngine> _DBEngines = new();

        #region 单例
        static DBFactory _instance;
        static object _lock_singleton = new object();
        /// <summary>
        /// 单例对象
        /// </summary>
        public static DBFactory Instance
        {
            get
            {
                if (_instance == null)
                {

                    lock (_lock_singleton)
                    {
                        if (_instance == null)
                            _instance = new DBFactory();
                    }
                }
                return _instance;
            }
        }

        public int SplitCount { get; internal set; }
        #endregion


        #region 注册引擎

        /// <summary>
        /// 注册引擎
        /// </summary>
        /// <param name="engine"></param>
        /// <param name="TagName"></param>
        /// <exception cref="Exception"></exception>
        public void RegistEngine(string TagName, IDBEngine engine)
        {
            if (_DBEngines.ContainsKey(TagName))
            {
                throw new Exception("已注册同名数据库引擎，请重新命名后载入");
            }
            else
            {
                _DBEngines.Add(TagName, engine);
            }
        }
        public void RegistEngine(IDBEngine engine)
        {
            if (_DBEngines.ContainsKey("Default"))
            {
                throw new Exception("已存在默认数据库引擎，请重新命名后载入");
            }
            else
            {
                _DBEngines.Add("Default", engine);
            }
        }
        #endregion

        public IDBEngine GetEngine(string tag = null)
        {
            if (tag == null)
            {
                tag = "Default";
            }
            if (_DBEngines.TryGetValue(tag, out var engine))
            {
                return engine;
            }
            else
            {
                throw new Exception("未注册数据库引擎");
            }
        }

    }
}
