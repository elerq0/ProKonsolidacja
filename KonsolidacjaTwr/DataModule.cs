using System;
using System.Data;
using PROLog;
using PROOptima;
using PROSql;

namespace KonsolidacjaTwr
{
    class DataModule
    {
        private readonly LogFile logFile;
        private readonly LogFile errLogFile;
        private readonly Optima optima;
        private readonly SQL sql;

        private readonly string OptimaTargetCompany = Properties.Settings.Default.OptimaCompany;

        private readonly Boolean debug = false;
        public DataModule(LogFile logFile, LogFile errLogFile)
        {
            this.logFile = logFile;
            this.errLogFile = errLogFile;
            try
            {
                optima = new Optima(Properties.Settings.Default.OptimaPath, true);
                logFile.Write("Stworzono obiekt AppOptima");

                sql = new SQL(Properties.Settings.Default.SQLServerName,
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

        private void OptimaConnect(string company)
        {
            if (optima.Login(Properties.Settings.Default.OptimaUsername,
                                Properties.Settings.Default.OptimaPassword,
                                company))
                logFile.Write("Zalogowano do ERP Optima, firma: [" + company + "]");
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

        public void OptimaSaveWithoutSessionReset()
        {
            optima.SaveWithoutSessionRefresh();
            logFile.Write("Zapisano zmiany w ERP Optima bez resetu sesji");
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

        public DataTable GetTwrConsolidationList()
        {
            if (debug)
                return GetTwrConsolidationListMock();

            try
            {
                SqlConnect();
                DataTable dt = sql.Execute("exec " + Properties.Settings.Default.SQLProcGetRecordSet);

                logFile.Write("Stworzono listę towarów do konsolidacji, " + dt.Rows.Count + " pozycji.");
                return dt;
            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy tworzeniu listy towarów do konsolidacji: " + e.Message);
            }
        }

        private DataTable GetTwrConsolidationListMock()
        {
            try
            {
                SqlConnect();
                DataTable dt = sql.Execute("select 1 as Import, 'CDN_Dotykacka' as KTr_ZrdBaza, 132 as KTr_ZrdTwrId, 'DOTPR58 TESTowyoo' as KTr_Kod, TwG_Kod, KCN_Kod, Knt_Kod, CDN.Towary.*, SprKat.Kat_KodSzczegol as SprKat_KodSzczegol, ZakKat.Kat_KodSzczegol as ZakKat_KodSzczegol  from CDN.Towary join CDN.TwrGrupy on TwG_GIDNumer = Twr_TwGGIDNumer and TwG_GIDTyp = -16 left join CDN.KodyCN on KCN_KCNId = Twr_KCNId left join CDN.Kontrahenci on Knt_KntId = Twr_KntId left join CDN.Kategorie SprKat on SprKat.Kat_KatID = Twr_KatId left join CDN.Kategorie ZakKat on ZakKat.Kat_KatID = Twr_KatZakId  where Twr_TwrId = 88");

                logFile.Write("Stworzono listę towarów do konsolidacji, " + dt.Rows.Count + " pozycji.");
                return dt;
            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy tworzeniu listy towarów do konsolidacji: " + e.Message);
            }
        }

        private DataTable GetJMZ(string baza, int TwrId, string TwrKod)
        {
            try
            {
                SqlConnect();
                DataTable dt = sql.Execute("select * from " + baza + ".CDN.TwrJMZ where TwJZ_TwrID = " + TwrId);

                logFile.Write("Pobrano dane JM z bazy [" + baza + "], TwrKod = [" + TwrKod + "]");
                return dt;
            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy pobieraniu JM [" + TwrKod + "] z bazy " + baza + ", " + e.Message);
            }
        }

        private DataTable GetGroups(string baza, string TwrKod)
        {
            try
            {
                SqlConnect();
                DataTable dt = sql.Execute("select * from " + baza + ".CDN.Towary " +
                    " join " + baza + ".CDN.TwrGrupy on TwG_Kod = Twr_Kod " +
                    " join " + baza + ".CDN.fnTwrGrupy() on TwG_GrONumer = GIDNumer " +
                    " where Twr_Kod = '" + TwrKod + "'");

                logFile.Write("Pobrano Grupy z bazy [" + baza + "], TwrKod = [" + TwrKod + "]");
                return dt;
            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy pobieraniu Grup [" + TwrKod + "] z bazy " + baza + ", " + e.Message);
            }
        }

        private DataTable GetAtributesData(string baza, string TwrCode)
        {
            DataTable dt;
            try
            {
                SqlConnect();

                dt = sql.Execute("select DeA_Kod, TwA_WartoscTxt, TwA_Zalezny, TwA_TwAId " +
                            "from " + baza + ".CDN.TwrAtrybuty " +
                            "join " + baza + ".CDN.DefAtrybuty on TwA_DeAId = DeA_DeAId " +
                            "join " + baza + ".CDN.Towary on TwA_TwrId = Twr_TwrId " +
                            "where Twr_Kod = '" + TwrCode + "'");

                logFile.Write("Pobrano listę atrybutów dla towaru o kodzie [" + TwrCode + "] o ilości: " + dt.Rows.Count + " pozycji.");

            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy pobieraniu listy atrybutów dla towaru o kodzie [" + TwrCode + "] z bazy [" + baza + "], " + e.Message);
            }

            return dt;
        }

        private DataTable GetAtributesKntData(string baza, int TwAID, string TwaCode)
        {
            DataTable dt;
            try
            {
                SqlConnect();

                dt = sql.Execute("select Knt_Kod, TKA_WartoscTxt " +
                            "from " + baza + ".CDN.TwrKntAtrybuty " +
                            "join " + baza + ".CDN.Kontrahenci on TKA_PodmiotId = Knt_KntId and TKA_PodmiotTyp = 1 " +
                            "where TKA_TwAId = " + TwAID);

                logFile.Write("Pobrano listę wartości atrybutu kontrahenta o kodzie [" + TwaCode + "] o ilości: " + dt.Rows.Count + " pozycji.");

            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy pobieraniu listy wartości atrybutu kontrahenta o kodzie [" + TwaCode + "] z bazy [" + baza + "], " + e.Message);
            }

            return dt;
        }


        private string GetConvertedValue(string type, string baza, string code)
        {
            try
            {

                PROSQLParam[] paramss = new PROSQLParam[3];
                paramss[0] = new PROSQLParam()
                {
                    name = "@Typ",
                    type = System.Data.SqlDbType.VarChar,
                    size = 3,
                    value = type,
                    direction = System.Data.ParameterDirection.Input
                };

                paramss[1] = new PROSQLParam()
                {
                    name = "@NazwaZ",
                    type = System.Data.SqlDbType.VarChar,
                    size = 80,
                    value = code,
                    direction = System.Data.ParameterDirection.Input
                };

                paramss[2] = new PROSQLParam()
                {
                    name = "@BazaZ",
                    type = System.Data.SqlDbType.VarChar,
                    size = 16,
                    value = baza,
                    direction = System.Data.ParameterDirection.Input
                };

                Object output = sql.ExecuteFunction("select " + Properties.Settings.Default.SQLFuncConvertValue + "(@Typ, @NazwaZ, @BazaZ)", paramss);
                if (output == null || output == DBNull.Value)
                    throw new Exception("Nie znaleziono wartości!");

                return output.ToString();
            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy pobieraniu skonwertowanego obiektu typu [" + type + "] o nazwie źródłowej [" + code + "] " + e.Message);
            }
        }

        private DataTable GetReplacementsData(string baza, int TwrId, string TwrCode)
        {
            {
                try
                {
                    SqlConnect();
                    DataTable dt = sql.Execute("select * from " + baza + ".CDN.Zamienniki join " + baza + ".CDN.Towary on Twr_TwrId = ZAM_ZamTwrId where ZAM_TwrId = " + TwrId);

                    logFile.Write("Pobrano dane zamienników towaru o id [" + TwrId + "] z bazy [" + baza + "], " + dt.Rows.Count + " pozycji");
                    return dt;
                }
                catch (Exception e)
                {
                    throw new Exception("Błąd przy pobieraniu zamienników towaru o kodzie źródłowym [" + TwrCode + "] z bazy " + baza + ", " + e.Message);
                }

            }
        }

        public void AfterImport(int ZrdTwrId, int NewTwrId, string Kod, string result)
        {
            if (debug)
                return;

            try
            {
                SqlConnect();
                sql.Execute("exec " + Properties.Settings.Default.SQLProcAfterImport + " " + ZrdTwrId + ", " + NewTwrId + ", '" + Kod + "', '" + result + "' ");
                logFile.Write("Zakonczono import dla towaru o kodzie docelowym [" + Kod + "]");
            }
            catch (Exception e)
            {
                SqlDisconnect();
                throw new Exception("Błąd w logu dla towaru o docelowym kodzie [" + Kod + "]: " + e.Message);
            }
        }

        public int CreateGood(DataRow row)
        {
            int NewTwrId;
            try
            {
                OptimaConnect(OptimaTargetCompany);

                CDNTwrb1.Towar targetGood = optima.GetGoodCollection().AddNew();

                try
                {
                    //--------------------------------------------------- OGÓLNE -------------------------------------------------------//
                    {
                        targetGood.Kod = row.Field<string>("KTr_Kod");
                        if (debug)
                            targetGood.NumerKat = row.Field<string>("Twr_NumerKat") + '0';
                        else
                            targetGood.NumerKat = row.Field<string>("Twr_NumerKat");

                        targetGood.TwGGIDNumer = optima.GetGoodGroupByCode(row.Field<string>("TwG_Kod")).GIDNumer;
                        targetGood.Typ = row.Field<byte>("Twr_Typ");
                        targetGood.Produkt = row.Field<Int16>("Twr_Produkt");

                        targetGood.Nazwa = row.Field<string>("Twr_Nazwa");

                        
                        if (!row.IsNull("SprKat_KodSzczegol"))
                            targetGood.Kategoria = optima.GetCategoryByCode(GetConvertedValue("Kat", row.Field<string>("KTr_ZrdBaza"), row.Field<string>("SprKat_KodSzczegol")));
                        if (!row.IsNull("ZakKat_KodSzczegol"))
                            targetGood.KategoriaZak = optima.GetCategoryByCode(GetConvertedValue("Kat", row.Field<string>("KTr_ZrdBaza"), row.Field<string>("ZakKat_KodSzczegol")));
                        

                        targetGood.JM = row.Field<string>("Twr_JM"); // konwersja, ale jest wymagana
                        targetGood.JMCalkowite = row.Field<byte>("Twr_JMCalkowite");

                        if (debug)
                            ;// targetGood.EAN = row.Field<string>("Twr_EAN") + '0';
                        else
                            targetGood.EAN = row.Field<string>("Twr_EAN");
                        //targetGood.TwCNumer - nie ma?

                        if (!row.IsNull("KCN_Kod"))
                            targetGood.KCNID = optima.GetCNCodeByCode(row.Field<string>("KCN_Kod")).ID;

                        targetGood.SWW = row.Field<string>("Twr_SWW");

                        targetGood.Flaga = row.Field<Int16>("Twr_Flaga");
                        targetGood.Zrodlowa = row.Field<Decimal>("Twr_Zrodlowa");
                        targetGood.Stawka = row.Field<Decimal>("Twr_Stawka");
                        targetGood.FlagaZak = row.Field<Int16>("Twr_FlagaZak");
                        targetGood.ZrodlowaZak = row.Field<Decimal>("Twr_ZrodlowaZak");
                        targetGood.StawkaZak = row.Field<Decimal>("Twr_StawkaZak");
                        targetGood.SplitPay = row.Field<byte>("Twr_SplitPay");

                        //cenniki konwersja
                    }

                    //--------------------------------------------------- DODATKOWE ----------------------------------------------------//
                    {
                        // Główna
                        targetGood.URL = row.Field<string>("Twr_URL");
                        targetGood.Opis = row.Field<string>("Twr_Opis");
                        targetGood.KopiujOpis = row.Field<byte>("Twr_KopiujOpis");
                        targetGood.UdostepniajWCenniku = row.Field<byte>("Twr_UdostepniajWCenniku");
                        targetGood.EdycjaNazwy = row.Field<byte>("Twr_EdycjaNazwy");
                        targetGood.EdycjaOpisu = row.Field<byte>("Twr_EdycjaOpisu");
                        targetGood.Nieaktywny = row.Field<byte>("Twr_NieAktywny");
                        if (!row.IsNull("Twr_WagaKG"))
                            targetGood.WagaKG = (double)row.Field<decimal>("Twr_WagaKG");

                        targetGood.TypKosztuUslugi = row.Field<byte>("Twr_KosztUslugiTyp");
                        targetGood.KosztUslugi = row.Field<decimal>("Twr_KosztUslugi");

                        targetGood.IloscMin = (double)row.Field<decimal>("Twr_IloscMin");
                        targetGood.IloscMinJM = row.Field<string>("Twr_IloscMinJM");
                        targetGood.IloscMax = (double)row.Field<decimal>("Twr_IloscMax");
                        targetGood.IloscMaxJM = row.Field<string>("Twr_IloscMaxJM");
                        targetGood.IloscZam = (double)row.Field<decimal>("Twr_IloscZam");
                        targetGood.IloscZamJM = row.Field<string>("Twr_IloscZamJM");

                        
                        if (!row.IsNull("Knt_Kod"))
                        {
                            try
                            {
                                string KntName = GetConvertedValue("Knt", row.Field<string>("KTr_ZrdBaza"), row.Field<string>("Knt_Kod"));
                                targetGood.DostawcaID = optima.GetContractorByName(KntName).ID;
                            }
                            catch(Exception)
                            {

                            }
                        }
                        

                        targetGood.KodDostawcy = row.Field<string>("Twr_KodDostawcy");

                        targetGood.PLU = row.Field<string>("Twr_PLU");
                        targetGood.NazwaFiskalna = row.Field<string>("Twr_NazwaFiskalna");

                        // Intrastat
                        targetGood.KrajPochodzenia = row.Field<string>("Twr_KrajPochodzenia");
                        targetGood.Masa = (double)row.Field<decimal>("Twr_Masa");
                        targetGood.JMPomPrzelicznikL = row.Field<decimal>("Twr_JmPomPrzelicznikL");
                        targetGood.JMPomPrzelicznikM = (int)row.Field<decimal>("Twr_JmPomPrzelicznikM");
                    }

                    //--------------------------------------------------- JEDNOSTKI I KODY ---------------------------------------------//
                    {
                        DataTable JMZs = GetJMZ(row.Field<string>("KTr_ZrdBaza"), row.Field<int>("Twr_TwrId"), row.Field<string>("Twr_Kod"));
                        foreach (DataRow jmz in JMZs.Rows)
                        {
                            CDNTwrb1.TwrJMZ targetJM = targetGood.JednostkiMiary.AddNew();
                            targetJM.JM = jmz.Field<string>("TwJZ_JM");
                            targetJM.JMPrzelicznikL = jmz.Field<decimal>("TwJZ_JMPrzelicznikL");
                            targetJM.JMPrzelicznikM = (int)jmz.Field<decimal>("TwJZ_JMPrzelicznikM");
                            targetJM.CenaJednostkowa = jmz.Field<byte>("TwJZ_CenaJednostkowa");
                        }

                        // Kody nie używane
                    }

                    //--------------------------------------------------- GRUPY ---------------------------------------------------------//
                    {

                        DataTable Groups = GetGroups(row.Field<string>("KTr_ZrdBaza"), row.Field<string>("Twr_Kod"));
                        foreach (DataRow grupa in Groups.Rows)
                        {
                            try
                            {
                                if (row.Field<string>("TwG_Kod") != grupa.Field<string>("Kod"))
                                {
                                    CDNTwrb1.TwrGrupa TwrGrupa = optima.GetGoodGroupByCode(grupa.Field<string>("Kod"));
                                    CDNTwrb1.TwrGrupa targetGrupa = targetGood.Grupy.AddNew();
                                    targetGrupa.GRONumer = TwrGrupa.GIDNumer;
                                    targetGrupa.GIDTyp = 16;
                                }
                            }
                            catch (Exception)
                            {
                                logFile.Write("Nie znaleziono grupy o kodzie = [" + grupa.Field<string>("Kod") + "]");
                            }

                            OptimaSaveWithoutSessionReset();
                        }

                    }

                    //--------------------------------------------------- ZAMIENNIKI ---------------------------------------------------//
                    {
                        
                        DataTable replacements = GetReplacementsData(row.Field<string>("KTr_ZrdBaza"), row.Field<int>("Twr_TwrId"), row.Field<string>("Twr_Kod"));
                        foreach (DataRow replacement in replacements.Rows)
                        {
                            int ZamId = optima.GetGoodByCode(GetConvertedValue("Twr", row.Field<string>("KTr_ZrdBaza"), replacement.Field<string>("Twr_Kod"))).ID;

                            CDNTwrb1.Zamiennik targetReplacement = targetGood.Zamienniki.AddNew();
                            targetReplacement.ZamTwrID = ZamId;

                            OptimaSaveWithoutSessionReset();
                        }
                        
                    }
                    

                    //--------------------------------------------------- ATRYBUTY ------------------------------------------------------//
                    {
                        DataTable attributes = GetAtributesData(row.Field<string>("KTr_ZrdBaza"), row.Field<string>("Twr_Kod"));
                        foreach (DataRow attribute in attributes.Rows)
                        {
                            CDNTwrb1.TwrAtrybut targetAttribute = targetGood.Atrybuty.AddNew();
                            targetAttribute.DeAID = optima.GetDefAtribute(attribute.Field<string>("DeA_Kod"), 1).ID;

                            if (attribute.Field<byte>("TwA_Zalezny") == 0)
                                targetAttribute.Wartosc = attribute.Field<string>("TwA_WartoscTxt");
                            else
                            {
                                targetAttribute.Zalezny = 1;
                                DataTable attributesKnt = GetAtributesKntData(row.Field<string>("KTr_ZrdBaza"), attribute.Field<int>("TwA_TwAId"), attribute.Field<string>("DeA_Kod"));
                                foreach (DataRow attributeKnt in attributesKnt.Rows)
                                {
                                    CDNTwrb1.TwrKntAtrybut targetKntAttribute = targetAttribute.TwrKntAtrybuty.AddNew();
                                    targetKntAttribute.PodmiotTyp = 1;
                                    targetKntAttribute.PodmiotID = optima.GetContractorByName(GetConvertedValue("Knt", row.Field<string>("KTr_ZrdBaza"), attributeKnt.Field<string>("Knt_Kod"))).ID;
                                    targetKntAttribute.Wartosc = attributeKnt.Field<string>("TKA_WartoscTxt");
                                }
                            }

                            OptimaSaveWithoutSessionReset();
                        }
                    }

                    OptimaSaveWithoutSessionReset();
                    NewTwrId = targetGood.ID;
                    OptimaSave();

                    logFile.Write("Stworzono towar o kodzie docelowym: [" + row.Field<string>("KTr_Kod") + "], TwrId = [" + NewTwrId + "]");

                    return NewTwrId;
                }
                catch (Exception e)
                {
                    try
                    {
                        OptimaSaveWithoutSessionReset();
                        optima.GetGoodCollection().Delete(optima.GetGoodByID(targetGood.ID));
                        OptimaSave();
                    }
                    finally
                    {
                        optima.ForceSessionRenew();
                        throw new Exception(e.Message);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy tworzeniu obiektu towaru o docelowym kodzie [" + row.Field<string>("KTr_Kod") + "]: " + e.Message);
            }
        }
    }
}
