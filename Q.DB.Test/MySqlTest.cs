using System.Diagnostics;
using Bogus;
using Newtonsoft.Json;
using Q.DB.MySql;
using Q.DB.Test.Entitys;

namespace Q.DB.Test
{

    public class Tests
    {
        string dbName = string.Empty;
        [OneTimeSetUp]
        public void Setup()
        {
            string sqlConnStr = "";
            Trace.Listeners.Add(new ConsoleTraceListener());
            DBFactory.Instance.RegistEngine(new MySqlEngine(sqlConnStr));
            dbName = "qdb_test_mysql";
            SqlLogUtil.PrintOut = (t, l) =>
            {
                Console.WriteLine(l);
            };
        }

        [Test, Order(0)]
        public void t1_�������ݿ�()
        {

            DBFactory.Instance.GetEngine().ExecuteNonQuery($"DROP DataBase IF EXISTS {dbName};Create DataBase {dbName};");
            var res = DBFactory.Instance.GetEngine().ExecuteScalar<int>($"select  count(*) from information_schema.SCHEMATA where SCHEMA_NAME = '{dbName}'; ");
            Assert.IsTrue(res == 1, "���ݿⴴ���ɹ�");

        }
        [Test, Order(1)]
        public void t2_�������Ա�()
        {
            using (DBContext context = new DBContext(dbName))
            {
                context.CreateTableIfNotExists<Customer>();
                var res = context.ExecuteScalar<int>($"select count(*) from information_schema.TABLES where TABLE_SCHEMA = '{dbName}' AND TABLE_NAME = 'Customer'; ");
                Assert.IsTrue(res == 1, "�ͻ������ɹ�");
            }
        }

        [Test, Order(3)]
        public void t3_�������ݲ���_��ѯ()
        {
            var customer = DataHelper.CreateCustomers(1)[0];
            using (DBContext context = new DBContext(dbName))
            {
                int id = context.Insert(customer);
                var dbcustomer = context.Query<Customer>().QueryFirst(x => x.Id == id);
                Assert.IsTrue(dbcustomer.Name == customer.Name && dbcustomer.Email == customer.Email);
            }
        }
        [Test, Order(4)]
        public void t4_�������ݲ���BatchInsert_1_0000()
        {
            var customers = DataHelper.CreateCustomers(1_0000);
            using (DBContext context = new DBContext(dbName))
            {
                int res = context.BatchInsert(customers);
                Console.WriteLine("�������:" + res);
                Assert.IsTrue(res == 1_0000);
            }
        }

        [Test, Order(5)]
        public void t5_�������ݲ���BatchInsert_10_0000()
        {
            var customers = DataHelper.CreateCustomers(10_0000);
            using (DBContext context = new DBContext(dbName))
            {
                int res = context.BatchInsert(customers);
                Console.WriteLine("�������:" + res);
                Assert.IsTrue(res == 10_0000);
            }
        }

        [Test, Order(6)]
        public void t6_�������ݵ���BulkInsert_10_0000()
        {
            var customers = DataHelper.CreateCustomers(10_0000);
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.BulkInsert(customers);
                Console.WriteLine("�������:" + res);
                Assert.IsTrue(res == 10_0000);
            }
        }

        [Test, Order(7)]
        public void t7_01_��������()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Count64();
                Console.WriteLine("���ҽ��:" + res);
                Assert.IsTrue(res == 21_0001);
            }
        }

        [Test, Order(7)]
        public void t7_02_�����������50������()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Where(x => x.Age > 50).Count64();
                Console.WriteLine("���ҽ��:" + res);
                Assert.IsTrue(res > 0);
            }
        }

        [Test, Order(7)]
        public void t7_03_����������ڵ���20����С��50������()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Where(x => x.Age >= 20 && x.Age < 50).Count64();
                Console.WriteLine("���ҽ��:" + res);
                Assert.IsTrue(res > 0);
            }
        }
        [Test, Order(7)]
        public void t7_04_��������С��20���ߴ��ڵ���50������()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Where(x => x.Age < 20 || x.Age >= 50).Count64();
                Console.WriteLine("���ҽ��:" + res);
                Assert.IsTrue(res > 0);
            }
        }

        [Test, Order(7)]
        public void t7_05_����������_��_���߰���_��_������С��20���ߴ��ڵ���50������()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Where(x => (x.Name.StartsWith("��") || x.Name.Contains("��")) && (x.Age < 20 || x.Age >= 50)).Count64();
                Console.WriteLine("���ҽ��:" + res);
                Assert.IsTrue(res > 0);
            }
        }

        [Test, Order(7)]
        public void t7_06_�������ֲ�����_��_������С��20���ߴ��ڵ���50������()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Where(x =>  !x.Name.Contains("��") && (x.Age < 20 || x.Age >= 50)).Count64();
                Console.WriteLine("���ҽ��:" + res);
                Assert.IsTrue(res > 0);
            }
        }
        [Test, Order(7)]
        public void t7_07_����������10_20_30_40_50�������()
        {
            using (DBContext context = new DBContext(dbName))
            {
                var res = context.Query<Customer>().Where(x => Sqm.In(x.Age, 10, 20, 30, 40, 50)).Count64();

                Console.WriteLine("���ҽ��:" + res);
                Assert.IsTrue(res > 0);
            }
        }
        [Test, Order(7)]
        public void t7_08_�������䲻��10_20_30_40_50�������()
        {
            using (DBContext context = new DBContext(dbName))
            {
                var res = context.Query<Customer>().Where(x => Sqm.NotIn(x.Age, 10, 20, 30, 40, 50)).Count64();

                Console.WriteLine("���ҽ��:" + res);
                Assert.IsTrue(res > 0);
            }
        }

        [Test, Order(7)]
        public void t7_09_�����������()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Max<long>(x => x.Age);
                Console.WriteLine("���ҽ��:" + res);
                Assert.IsTrue(res > 0);
            }
        }

        [Test, Order(7)]
        public void t7_10_����������䲢�Ҽ�5����()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Select(x => x.Age + 5).OrderByDesc(x => x.Age).QueryFirst<long>();
                Console.WriteLine("���ҽ��:" + res);
                Assert.IsTrue(res > 0);
            }
        }

        [Test, Order(7)]
        public void t7_11_������������绰ƴ����һ�𷵻�ǰ10��()
        {
            using (DBContext context = new DBContext(dbName))
            {
                var res = context.Query<Customer>().Select(x => Sqm.Concat(x.Name, "_", x.Age, "_", x.Age + 5, "_", x.Phone)).QueryTop<string>(10);

                // ���﷢�� �� IEnumerable ��β�������ִ�ж�β�ѯ�����ԣ�ȡֵ�ͼ����������β���֮ǰ��תList
                var resList = res.ToList();
                Console.WriteLine("���ҽ��:" + JsonConvert.SerializeObject(resList, Formatting.Indented));
                Assert.IsTrue(resList.Count > 0);
            }
        }

        [Test, Order(7)]
        public void t7_12_��ѯÿ�����������()
        {
            using (DBContext context = new DBContext(dbName))
            {
                var res = context.Query<Customer>().GroupBy(x=>x.Age).Select(x=>new {x.Age,Count=Sqm.Count()}).QueryAll<object>();

                // ���﷢�� �� IEnumerable ��β�������ִ�ж�β�ѯ�����ԣ�ȡֵ�ͼ����������β���֮ǰ��תList
                var resList = res.ToList();
                Console.WriteLine("���ҽ��:" + JsonConvert.SerializeObject(resList, Formatting.Indented));
                Assert.IsTrue(resList.Count > 0);
            }
        }

        [Test, Order(8)]
        public void t8_��������_���ֶ�()
        {
            using (DBContext context = new DBContext(dbName))
            {
              var customer=  context.Query<Customer>().QueryFirst(x => x.Id == 10);
                Console.WriteLine("ԭ���ݣ�"+customer.Age);
               var res= context.Query<Customer>().Update(n => n.Age + 1, w => w.Id == 10);
                if (res == 0)
                {
                    Assert.IsTrue(res != 0, "����ִ��ʧ��");
                }
                else
                {
                    var newcustomer = context.Query<Customer>().QueryFirst(x => x.Id == 10);
                    Console.WriteLine("���º����ݣ�" + newcustomer.Age);
                    Assert.IsTrue((customer.Age + 1) == newcustomer.Age);
                }
            }
        }

        [Test, Order(9)]
        public void t9_��������_���ֶ�()
        {
            using (DBContext context = new DBContext(dbName))
            {
                var customer = context.Query<Customer>().QueryFirst(x => x.Id == 11);
                Console.WriteLine("ԭ���ݣ�\r\n" +JsonConvert.SerializeObject(customer,Formatting.Indented));
                var res = context.Query<Customer>().Update(n => new {ZipCode="666",Age = n.Age + 1, ContactName=Sqm.Concat(n.Name,"_",n.Phone) }, w => w.Id == 11);
                if (res == 0)
                {
                    Assert.IsTrue(res != 0, "����ִ��ʧ��");
                }
                else
                {
                    var newcustomer = context.Query<Customer>().QueryFirst(x => x.Id == 11);
                    Console.WriteLine("���º�\r\n" + JsonConvert.SerializeObject(newcustomer, Formatting.Indented));
                    Assert.IsTrue((customer.Age + 1) == newcustomer.Age);
                }
            }
        }




        [Test, Order(999999)]
        public void ɾ�����ݿ�()
        {
            DBFactory.Instance.GetEngine().ExecuteNonQuery($"drop DataBase {dbName};");
            var res = DBFactory.Instance.GetEngine().ExecuteScalar<int>($"select count(*) from information_schema.SCHEMATA where SCHEMA_NAME = '{dbName}'; ");
            Assert.IsTrue(res == 0, "�������ݿ�ɾ���ɹ�");
        }
    }
}