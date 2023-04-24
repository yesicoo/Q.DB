/*************************************************************************************
 *
 * 文 件 名:   DBConnectionItem.cs
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
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Q.DB.Models
{
    public class DBConnectionItem
    {
        public DbConnection SqlConnection { internal set;get; }

        internal DbTransaction transaction { set; get; } = null;
        internal bool Available { set; get; }
        internal DateTime SleepTime { set; get; } = DateTime.MaxValue;
        internal DBConnInfo ConnInfo { set; get; }
        internal string Tag { set; get; }


        internal void GiveBack()
        {
            this.transaction?.Dispose();
            this.transaction = null;
            this.Available = true;
            this.SleepTime = DateTime.Now;
        }

        internal void Dispose()
        {
            this.transaction?.Dispose();
            this.transaction = null;
            this.SqlConnection.Dispose();
            this.SqlConnection = null;
        }

        internal DbTransaction BeginTransaction()
        {
            transaction = SqlConnection.BeginTransaction();
            return transaction;
        }
        internal DbTransaction BeginTransaction(IsolationLevel iso)
        {
            transaction = SqlConnection.BeginTransaction(iso);
            return transaction;
        }
        internal async Task<DbTransaction> BeginTransactionAsync()
        {
            transaction = await SqlConnection.BeginTransactionAsync();
            return transaction;
        }
        internal async Task<DbTransaction> BeginTransactionAsync(IsolationLevel iso)
        {
            transaction = await SqlConnection.BeginTransactionAsync(iso);
            return transaction;
        }

        internal void Commit()
        {
            try
            {
                transaction?.Commit();

            }
            catch (Exception)
            {
                Rollback();
                TransactionDispose();
                throw;
            }
        }

        internal async Task CommitAsync()
        {
            try
            {
                await transaction?.CommitAsync();

            }
            catch (Exception)
            {
               await RollbackAsync();
                TransactionDispose();
                throw;
            }
        }

        internal void Rollback()
        {
            transaction?.Rollback();
            TransactionDispose();
        }
        internal async Task RollbackAsync()
        {
            await transaction?.RollbackAsync();
            TransactionDispose();
        }


        internal void TransactionDispose()
        {
            if (transaction != null)
            {
                lock (transaction)
                {
                    transaction.Dispose();
                    transaction = null;
                }
            }
        }

    }
}
