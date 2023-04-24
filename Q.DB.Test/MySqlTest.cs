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
        public void t1_创建数据库()
        {

            DBFactory.Instance.GetEngine().ExecuteNonQuery($"DROP DataBase IF EXISTS {dbName};Create DataBase {dbName};");
            var res = DBFactory.Instance.GetEngine().ExecuteScalar<int>($"select  count(*) from information_schema.SCHEMATA where SCHEMA_NAME = '{dbName}'; ");
            Assert.IsTrue(res == 1, "数据库创建成功");

        }
        [Test, Order(1)]
        public void t2_创建测试表()
        {
            using (DBContext context = new DBContext(dbName))
            {
                context.CreateTableIfNotExists<Customer>();
                var res = context.ExecuteScalar<int>($"select count(*) from information_schema.TABLES where TABLE_SCHEMA = '{dbName}' AND TABLE_NAME = 'Customer'; ");
                Assert.IsTrue(res == 1, "客户表创建成功");
            }
        }

        [Test, Order(3)]
        public void t3_单条数据插入_查询()
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
        public void t4_批量数据插入BatchInsert_1_0000()
        {
            var customers = DataHelper.CreateCustomers(1_0000);
            using (DBContext context = new DBContext(dbName))
            {
                int res = context.BatchInsert(customers);
                Console.WriteLine("入库数量:" + res);
                Assert.IsTrue(res == 1_0000);
            }
        }

        [Test, Order(5)]
        public void t5_批量数据插入BatchInsert_10_0000()
        {
            var customers = DataHelper.CreateCustomers(10_0000);
            using (DBContext context = new DBContext(dbName))
            {
                int res = context.BatchInsert(customers);
                Console.WriteLine("入库数量:" + res);
                Assert.IsTrue(res == 10_0000);
            }
        }

        [Test, Order(6)]
        public void t6_批量数据导入BulkInsert_10_0000()
        {
            var customers = DataHelper.CreateCustomers(10_0000);
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.BulkInsert(customers);
                Console.WriteLine("入库数量:" + res);
                Assert.IsTrue(res == 10_0000);
            }
        }

        [Test, Order(7)]
        public void t7_01_查找数量()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Count64();
                Console.WriteLine("查找结果:" + res);
                Assert.IsTrue(res == 21_0001);
            }
        }

        [Test, Order(7)]
        public void t7_02_查找年龄大于50的数量()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Where(x => x.Age > 50).Count64();
                Console.WriteLine("查找结果:" + res);
                Assert.IsTrue(res > 0);
            }
        }

        [Test, Order(7)]
        public void t7_03_查找年龄大于等于20并且小于50的数量()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Where(x => x.Age >= 20 && x.Age < 50).Count64();
                Console.WriteLine("查找结果:" + res);
                Assert.IsTrue(res > 0);
            }
        }
        [Test, Order(7)]
        public void t7_04_查找年龄小于20或者大于等于50的数量()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Where(x => x.Age < 20 || x.Age >= 50).Count64();
                Console.WriteLine("查找结果:" + res);
                Assert.IsTrue(res > 0);
            }
        }

        [Test, Order(7)]
        public void t7_05_查找名字姓_张_或者包含_杰_字年龄小于20或者大于等于50的数量()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Where(x => (x.Name.StartsWith("张") || x.Name.Contains("杰")) && (x.Age < 20 || x.Age >= 50)).Count64();
                Console.WriteLine("查找结果:" + res);
                Assert.IsTrue(res > 0);
            }
        }

        [Test, Order(7)]
        public void t7_06_查找名字不包含_杰_字年龄小于20或者大于等于50的数量()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Where(x =>  !x.Name.Contains("杰") && (x.Age < 20 || x.Age >= 50)).Count64();
                Console.WriteLine("查找结果:" + res);
                Assert.IsTrue(res > 0);
            }
        }
        [Test, Order(7)]
        public void t7_07_查找年龄在10_20_30_40_50岁的数量()
        {
            using (DBContext context = new DBContext(dbName))
            {
                var res = context.Query<Customer>().Where(x => Sqm.In(x.Age, 10, 20, 30, 40, 50)).Count64();

                Console.WriteLine("查找结果:" + res);
                Assert.IsTrue(res > 0);
            }
        }
        [Test, Order(7)]
        public void t7_08_查找年龄不在10_20_30_40_50岁的数量()
        {
            using (DBContext context = new DBContext(dbName))
            {
                var res = context.Query<Customer>().Where(x => Sqm.NotIn(x.Age, 10, 20, 30, 40, 50)).Count64();

                Console.WriteLine("查找结果:" + res);
                Assert.IsTrue(res > 0);
            }
        }

        [Test, Order(7)]
        public void t7_09_查找年龄最大()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Max<long>(x => x.Age);
                Console.WriteLine("查找结果:" + res);
                Assert.IsTrue(res > 0);
            }
        }

        [Test, Order(7)]
        public void t7_10_查找最大年龄并且加5返回()
        {
            using (DBContext context = new DBContext(dbName))
            {
                long res = context.Query<Customer>().Select(x => x.Age + 5).OrderByDesc(x => x.Age).QueryFirst<long>();
                Console.WriteLine("查找结果:" + res);
                Assert.IsTrue(res > 0);
            }
        }

        [Test, Order(7)]
        public void t7_11_查找姓名年龄电话拼接在一起返回前10条()
        {
            using (DBContext context = new DBContext(dbName))
            {
                var res = context.Query<Customer>().Select(x => Sqm.Concat(x.Name, "_", x.Age, "_", x.Age + 5, "_", x.Phone)).QueryTop<string>(10);

                // 这里发现 对 IEnumerable 多次操作，会执行多次查询。所以，取值和计算数量两次操作之前先转List
                var resList = res.ToList();
                Console.WriteLine("查找结果:" + JsonConvert.SerializeObject(resList, Formatting.Indented));
                Assert.IsTrue(resList.Count > 0);
            }
        }

        [Test, Order(7)]
        public void t7_12_查询每个年龄的条数()
        {
            using (DBContext context = new DBContext(dbName))
            {
                var res = context.Query<Customer>().GroupBy(x=>x.Age).Select(x=>new {x.Age,Count=Sqm.Count()}).QueryAll<object>();

                // 这里发现 对 IEnumerable 多次操作，会执行多次查询。所以，取值和计算数量两次操作之前先转List
                var resList = res.ToList();
                Console.WriteLine("查找结果:" + JsonConvert.SerializeObject(resList, Formatting.Indented));
                Assert.IsTrue(resList.Count > 0);
            }
        }

        [Test, Order(8)]
        public void t8_条件更新_单字段()
        {
            using (DBContext context = new DBContext(dbName))
            {
              var customer=  context.Query<Customer>().QueryFirst(x => x.Id == 10);
                Console.WriteLine("原数据："+customer.Age);
               var res= context.Query<Customer>().Update(n => n.Age + 1, w => w.Id == 10);
                if (res == 0)
                {
                    Assert.IsTrue(res != 0, "更新执行失败");
                }
                else
                {
                    var newcustomer = context.Query<Customer>().QueryFirst(x => x.Id == 10);
                    Console.WriteLine("更新后数据：" + newcustomer.Age);
                    Assert.IsTrue((customer.Age + 1) == newcustomer.Age);
                }
            }
        }

        [Test, Order(9)]
        public void t9_条件更新_多字段()
        {
            using (DBContext context = new DBContext(dbName))
            {
                var customer = context.Query<Customer>().QueryFirst(x => x.Id == 11);
                Console.WriteLine("原数据：\r\n" +JsonConvert.SerializeObject(customer,Formatting.Indented));
                var res = context.Query<Customer>().Update(n => new {ZipCode="666",Age = n.Age + 1, ContactName=Sqm.Concat(n.Name,"_",n.Phone) }, w => w.Id == 11);
                if (res == 0)
                {
                    Assert.IsTrue(res != 0, "更新执行失败");
                }
                else
                {
                    var newcustomer = context.Query<Customer>().QueryFirst(x => x.Id == 11);
                    Console.WriteLine("更新后：\r\n" + JsonConvert.SerializeObject(newcustomer, Formatting.Indented));
                    Assert.IsTrue((customer.Age + 1) == newcustomer.Age);
                }
            }
        }




        [Test, Order(999999)]
        public void 删除数据库()
        {
            DBFactory.Instance.GetEngine().ExecuteNonQuery($"drop DataBase {dbName};");
            var res = DBFactory.Instance.GetEngine().ExecuteScalar<int>($"select count(*) from information_schema.SCHEMATA where SCHEMA_NAME = '{dbName}'; ");
            Assert.IsTrue(res == 0, "测试数据库删除成功");
        }
    }
}