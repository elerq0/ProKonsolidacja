using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tester
{
    class Program
    {
        static void Main(string[] args)
        {
            //new App().Run();

            object a = null;
            if (a == null)
                Console.WriteLine("output is null");
            else
                Console.WriteLine(a.ToString());
            Console.ReadLine();
        }
    }
}
