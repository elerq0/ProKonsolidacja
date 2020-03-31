using System;
using System.Data;
using PROOptima;
using PROSql;
using PROLog;

namespace ProfessoftBaseFusion
{
    class DataModule
    {
        protected LogFile logFile;
        protected Optima optima;
        protected SQL sql;

        public DataModule(LogFile logFile)
        {
            this.logFile = logFile;
            try
            {
                optima = new Optima(Properties.Settings.Default.OptimaPath, true);
                logFile.Write("Stworzono obiekt AppOptima");

                sql = new SQL(Properties.Settings.Default.SQLServername,
                                Properties.Settings.Default.SQLDatabase,
                                Properties.Settings.Default.SQLUsername,
                                Properties.Settings.Default.SQLPassword,
                                Properties.Settings.Default.SQLNT);
                logFile.Write("Stworzono obiekt AppSQL");
            }
            catch (Exception e)
            {
                optima = null;
                sql = null;
                throw new Exception(e.Message);
            }
        }

        private void OptimaConnect()
        {
            if (optima.Login(Properties.Settings.Default.OptimaUsername,
                                Properties.Settings.Default.OptimaPassword,
                                Properties.Settings.Default.OptimaCompany))
                logFile.Write("Zalogowano do ERP Optima");
        }

        private void OptimaDisconnect()
        {
            optima.LogOut();
            logFile.Write("Wylogowano z ERP Optima");
        }

        public void OptimaSave()
        {
            optima.Save();
            logFile.Write("Zapisano zmiany w ERP Optima");
        }

        private void SqlConnect()
        {
            if (sql.Connect())
                logFile.Write("Nawiązano połączenie z serwerem SQL");
        }

        private void SqlDisconnect()
        {
            sql.Disconnect();
            logFile.Write("Zamknięto połączenie z serwerem SQL");
        }

        public void Dispose()
        {
            SqlDisconnect();
            OptimaDisconnect();
        }

        public DataTable GetSupplyDataTable()
        {
            DataTable dt;
            try
            {
                SqlConnect();
                dt = sql.Execute("select * from CDN_KGL_KON.CDN.PROKnsStanMagazynow");
                logFile.Write("Stworzono listę dostaw" );
            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy pobieraniu listy dostaw " + e.Message);
            }
            return dt;
        }

        public CDNHlmn.IDokumentHaMag CreateDocument(string firma, string mag_symbol, string doc_name, string knt_kod, DateTime date, string doc_type)
        {
            try
            {
                OptimaConnect();

                CDNHlmn.IDokumentHaMag doc = optima.CreateNewDocumentHaMag();

                switch(doc_type)
                {
                    case "PWBO":
                        doc.Rodzaj = 303000;
                        doc.TypDokumentu = 303;
                        break;
                    case "PZBO":
                        doc.Rodzaj = 307000;
                        doc.TypDokumentu = 307;
                        doc.NumerObcy = doc_name;
                        break;
                    default:
                        throw new Exception("Nieznany typ dokumentu: [" + doc_type + "]");
                }

                doc.Numerator.DefinicjaDokumentu = optima.GetNumeratorBySymbol(doc_type);
                doc.DataDok = date;
                doc.MagazynZrodlowyID = optima.GetWarehouseBySymbol(mag_symbol).ID;
                doc.Bufor = 0;
                doc.Podmiot = optima.GetContractorByName(knt_kod);

                optima.AddOrEditAtributeDocumentHaMag(doc, Properties.Settings.Default.CompanyAtrName, firma);
                optima.AddOrEditAtributeDocumentHaMag(doc, Properties.Settings.Default.DocumentAtrName, doc_name);

                logFile.Write("Stworzono dokument na podstawie dokumentu dostawy [" + doc_name + "]");
                return doc;
            }
            catch(Exception e)
            {
                throw new Exception("Błąd przy tworzeniu dokumentu dla dokumentu dostawy " + doc_name + ", " + e.Message);
            }
        }

        public void AddDocumentPosition(CDNHlmn.IDokumentHaMag doc, string twr_code, decimal twr_ammount, decimal twr_value)
        {
            try
            {
                OptimaConnect();

                CDNTwrb1.ITowar good = optima.GetGoodByCode(twr_code);

                CDNHlmn.IElementHaMag elem = doc.Elementy.AddNew();
                elem.Towar = good;
                elem.Ilosc = Convert.ToDouble(twr_ammount);
                elem.WartoscNetto = twr_value;

                logFile.Write("Dodano pozycję o kodzie: [" + twr_code + "].");
            }
            catch(Exception e)
            {
                throw new Exception("Błąd przy dodawaniu pozycji o kodzie: [" + twr_code + "], " + e.Message);
            }
        }

        public void CreateDefAtributes()
        {
            OptimaConnect();
            optima.CreateDefAtribute(Properties.Settings.Default.CompanyAtrName, 1, 4);
            optima.CreateDefAtribute(Properties.Settings.Default.DocumentAtrName, 1, 4);
            OptimaSave();
        }

        public void SetStatusForDocument(string docname, string firma, int status)
        {
            SqlConnect();
            sql.Execute(@"update CDN_KGL_KON.CDN.PROKnsStanMagazynow set Stan = " + status + " where Dokument = '" + docname + "' and Firma = '" + firma + "'");
        }

    }
}
