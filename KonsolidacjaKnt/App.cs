using System;
using System.Data;
using PROLog;


namespace KonsolidacjaKnt
{
    class App
    {
        private readonly LogFile logFile;
        private readonly LogFile errLogFile;
        private DataModule dataModule;

        public App()
        {
            logFile = new LogFile(Properties.Settings.Default.LogFilePath);
            errLogFile = new LogFile(Properties.Settings.Default.LogFilePath.Replace(".log", "err.log"));
            dataModule = new DataModule(logFile, errLogFile); 
        }

        public void Run()
        {
            int count = 0, success = 0, failure = 0;
            DateTime startDate = DateTime.Now;
            DateTime endDate;
            TimeSpan diff;
            try
            {
                int NewKntId, KnsId;
                string KnsKod;
                DataTable kntConsolidacionList = dataModule.GetKntConsolidationList();
                count = kntConsolidacionList.Rows.Count;

                int i = 0;

                foreach (DataRow row in kntConsolidacionList.Rows)
                {
                    i = 0;
                    Start:
                    try
                    {
                        i += 1;

                        KnsId = row.Field<int>("Kns_KnsId");
                        KnsKod = row.Field<string>("Kns_Kod");

                        NewKntId = dataModule.CreateContractor(row);
                        dataModule.SetTargetKntId(KnsId, NewKntId, KnsKod);
                        success += 1;
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine(e.Message);
                        if (i < 3)
                        {
                            Console.WriteLine("Ponawianie");
                            goto Start;
                        }

                        logFile.Write(e.Message);
                        errLogFile.Write(e.Message);
                        failure += 1;
                    }
                }
            }
            catch(Exception e)
            {
                logFile.Write(e.Message);
                Console.WriteLine(e.Message);
            }
            dataModule.Dispose();

            endDate = DateTime.Now;
            diff = endDate.Subtract(startDate);
            string diffStr = diff.ToString(@"hh\:mm\:ss");
            Console.WriteLine("Completed in " + diffStr);
            Console.WriteLine("Count = " + count);
            Console.WriteLine("Success = " + success);
            Console.WriteLine("Failure = " + failure);

            Console.WriteLine("Click to exit.");
            Console.ReadLine();
        }
    }
}
