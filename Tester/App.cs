using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PROLog;
using PROSql;

namespace Tester
{
    class App
    {
        private readonly LogFile logFile;
        private readonly SQL sql;

        public App()
        {
            logFile = new LogFile(Properties.Settings.Default.LogFilePath);
            sql = new SQL(Properties.Settings.Default.SQLServerName,
                                Properties.Settings.Default.SQLDatabase,
                                Properties.Settings.Default.SQLUsername,
                                Properties.Settings.Default.SQLPassword,
                                Properties.Settings.Default.SQLNT);
        }


        public void Run()
        {
            sql.Connect();

            PROSQLParam[] paramss = new PROSQLParam[3];
            paramss[0] = new PROSQLParam()
            {
                name = "@Typ",
                type = System.Data.SqlDbType.VarChar,
                size = 3,
                value = "Knt",
                direction = System.Data.ParameterDirection.Input
            };

            paramss[1] = new PROSQLParam()
            {
                name = "@NazwaZ",
                type = System.Data.SqlDbType.VarChar,
                size = 80,
                value = "ETC",
                direction = System.Data.ParameterDirection.Input
            };

            paramss[2] = new PROSQLParam()
            {
                name = "@BazaZ",
                type = System.Data.SqlDbType.VarChar,
                size = 10,
                value = "CDN_KGL",
                direction = System.Data.ParameterDirection.Input
            };


            Console.WriteLine(sql.ExecuteFunction("select CDN.PROConvertValue(@Typ, @NazwaZ, @BazaZ)", paramss).ToString());
            sql.Disconnect();
            Console.ReadLine();
        }
    }
}
