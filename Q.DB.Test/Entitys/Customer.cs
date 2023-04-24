using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Q.DB.Test.Entitys
{
    public class Customer
    {
        [QingField(IsAutoIncrement = true,IsPrimaryKey =true)]
        public int Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }

        public string Address { get; set; }
        public string City { get; set; }
        public string Country { get; set; }
        public string ZipCode { get; set; }
        public string Phone { get; set; }
        public string Email { get; set; }
        public string ContactName { get; set; }
    }
}
