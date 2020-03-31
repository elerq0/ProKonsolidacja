using System;
using System.Data;
using PROLog;


namespace KonsolidacjaTwr
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
                int NewTwrId, ZrdTwrId, Import;
                string TwrKod;
                DataTable twrConsolidacionList = dataModule.GetTwrConsolidationList();
                count = twrConsolidacionList.Rows.Count;
                                
                foreach (DataRow row in twrConsolidacionList.Rows)
                {
                    Import = row.Field<int>("Import");
                    ZrdTwrId = row.Field<int>("Ktr_ZrdTwrId");
                    TwrKod = row.Field<string>("KTr_Kod");

                    try
                    {
                        if (Import == 1)
                        {
                            NewTwrId = dataModule.CreateGood(row);
                            dataModule.AfterImport(ZrdTwrId, NewTwrId, TwrKod, "Dodano pomyślnie");
                        }
                        else
                        {
                            dataModule.AfterImport(ZrdTwrId, 0, TwrKod, "Pominięto");
                        }

                        success += 1;
                    }
                    catch (Exception e)
                    {
                        logFile.Write(e.Message);
                        errLogFile.Write(e.Message);
                        Console.WriteLine(e.Message);
                        failure += 1;

                        dataModule.AfterImport(ZrdTwrId, 0, TwrKod, "Error");
                    }
                }
            }
            catch (Exception e)
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
