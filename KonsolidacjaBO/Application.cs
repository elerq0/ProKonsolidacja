using System;
using System.Threading;

namespace ProfessoftBaseFusion
{
    class Application
    {
        public static void Main(string[] args)
        {
            new Controller().Run();

            // dodać procedury do pobierania danych oraz ustawiania stanu
            //      >> DataModule 
            //          >> GetSupplyDataTable
            //          >> SetStatusForDocument
        }
    }
}
