/*************************************************************************************
 *
 * 文 件 名:   EntityCache.cs
 * 描    述: 
 * 版    本：  V1.0
 * 创 建 者：  XuQing
 * 创建时间：  2022/12/7 10:33
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
using System.Data.SqlTypes;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Q.DB.Models;

namespace Q.DB
{
    public static class EntityCache
    {
        private static Dictionary<string, EntityInfo> _Caches = new();

        public static Dictionary<string, DBFieldInfo> TryGetPropertyInfos<T>()
        {
            return TryGetInfo<T>()?.PropertyInfos;
        }

        public static EntityInfo TryGetInfo<T>()
        {
            return TryGetInfo(typeof(T));
        }

        public static EntityInfo TryGetInfo(Type type)
        {
            if (!_Caches.TryGetValue(type.FullName, out var cache))
            {
                cache = new EntityInfo();
                cache.Name = type.FullName;

                if (Attribute.IsDefined(type, typeof(QingEntityAttribute)))
                {
                    var qea = (QingEntityAttribute)Attribute.GetCustomAttribute(type, typeof(QingEntityAttribute));
                    cache.TableName = type.FullName;// QDBTools.RandomCode(8, 0);
                    cache.TableRealName = qea.TableName;
                    cache.Remark = qea.Remark;
                }
                else
                {
                    cache.TableName =  type.FullName;// QDBTools.RandomCode(8, 0);
                    cache.TableRealName = type.Name;
                }


                foreach (var item in type.GetProperties())
                {
                    if (!item.CanRead && !item.CanWrite)
                    {
                        continue;
                    }
                    if (Attribute.IsDefined(item, typeof(QingFieldAttribute)))
                    {
                        var qpa = (QingFieldAttribute)Attribute.GetCustomAttribute(item, typeof(QingFieldAttribute));
                        if (qpa.IsExclude)
                        {
                            continue;
                        }

                        string fieldName = string.IsNullOrEmpty(qpa.FieldName) ? item.Name : qpa.FieldName;

                        if (qpa.IsPrimaryKey)
                        {
                            cache.PrimaryKeys.Add(fieldName);
                        }
                        if (qpa.IsAutoIncrement)
                        {
                            cache.AutoIncrements.Add(fieldName);
                        }

                        DBFieldInfo qpi = new DBFieldInfo(
                            item.Name,
                            item.PropertyType,
                            item.SetValue,
                            item.GetValue,
                            fieldName,
                            qpa.Remark,
                            dbType: qpa.DBType,
                            dbLength: qpa.Length,
                            nullable: (qpa.IsNotNull ? false : (item.PropertyType.IsGenericType && item.PropertyType.GetGenericTypeDefinition().Equals(typeof(Nullable<>))))
                            );

                        cache.PropertyInfos.Add(fieldName, qpi);
                    }
                    else
                    {
                        var IsNullable = item.PropertyType.IsGenericType && item.PropertyType.GetGenericTypeDefinition().Equals(typeof(Nullable<>));
                        if (IsNullable)
                        {
                            cache.PropertyInfos.Add(item.Name, new DBFieldInfo(item.Name, item.PropertyType.GetGenericArguments()[0], item.SetValue, item.GetValue, item.Name, nullable: true));
                        }
                        else
                        {
                            cache.PropertyInfos.Add(item.Name, new DBFieldInfo(item.Name, item.PropertyType, item.SetValue, item.GetValue, item.Name, nullable: false));
                        }
                    }
                }
                _Caches.Add(type.FullName, cache);
            }
            return cache;
        }



        public static string TryGetTableName<T>()
        {

            return TryGetInfo<T>().TableName;
        }

        public static string TryGetTableName(Type type)
        {
            return TryGetInfo(type).TableName;
        }

        public static string TryGetRealTableName<T>()
        {

            return TryGetInfo<T>().TableRealName;
        }

        public static string TryGetRealTableName(Type type)
        {
            return TryGetInfo(type).TableRealName;
        }

        public static string RestoreTableName<T>(string sql,string tableSuffix=null)
        {
            var nea= TryGetInfo<T>();
            if (tableSuffix == null)
            {
                return sql.Replace(nea.TableName, nea.TableRealName);
            }
            else
            {
                return sql.Replace(nea.TableName, nea.TableRealName+ QDBTools.ConvertSuffixTableName(tableSuffix));
            }
        }
        public static string RestoreTableName(Type type,string sql, string tableSuffix = null)
        {
            var nea = TryGetInfo(type);
            if (tableSuffix == null)
            {
                return sql.Replace(nea.TableName, nea.TableRealName);
            }
            else
            {
                return sql.Replace(nea.TableName, nea.TableRealName + QDBTools.ConvertSuffixTableName(tableSuffix));
            }
        }

        public static List<DBIndexInfo> Index<T>()
        {
            return Index(typeof(T));

        }
        public static List<DBIndexInfo> Index(Type type)
        {
            List<DBIndexInfo> infos = new List<DBIndexInfo>();
            if (Attribute.IsDefined(type, typeof(QingIndexAttribute)))
            {
                var qias = (QingIndexAttribute[])Attribute.GetCustomAttributes(type, typeof(QingIndexAttribute));
                foreach (var item in qias)
                {
                    infos.Add(new DBIndexInfo { IndexName = item.IndexName, IndexFields = item.IndexFields, IndexType = item.IndexType });
                }
            }
            return infos;
        }
    }
}
