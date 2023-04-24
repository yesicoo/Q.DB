using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Bogus;
using Q.DB.Test.Entitys;

namespace Q.DB.Test
{
    internal static class DataHelper
    {
        public static List<Customer> CreateCustomers(int count)
        {
           return new Faker<Customer>("zh_CN")
                 .RuleFor(x => x.Email, f => f.Internet.Email())
                 .RuleFor(x => x.Name, f =>  f.Name.LastName()+ f.Name.FirstName())
                 .RuleFor(x => x.Age, f => f.Random.Number(18, 80))
                 .RuleFor(x => x.Address, f => f.Address.FullAddress())
                 .RuleFor(x => x.City, f => f.Address.City())
                 .RuleFor(x => x.ContactName, f => f.Name.JobArea())
                 .RuleFor(x => x.Phone, f => f.Phone.PhoneNumber())
                 .RuleFor(x => x.ZipCode, f => f.Address.ZipCode())
                 .RuleFor(x => x.Country, f => f.Address.Country())
                 .Generate(count);
        }
    }
}
