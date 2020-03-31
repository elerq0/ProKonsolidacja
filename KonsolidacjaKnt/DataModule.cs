using System;
using System.Data;
using PROLog;
using PROOptima;
using PROSql;

namespace KonsolidacjaKnt
{
    class DataModule
    {
        private readonly LogFile logFile;
        private readonly LogFile errLogFile;
        private readonly Optima optima;
        private readonly SQL sql;

        private readonly string OptimaTargetCompany = Properties.Settings.Default.OptimaCompany;
        private string CurrentCompany = string.Empty;

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
            CurrentCompany = string.Empty;
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

        public DataTable GetKntConsolidationList()
        {
            if (debug)
                return GetKntConsolidationListMock();

            DataTable dt;
            try
            {
                SqlConnect();

                dt = sql.Execute("exec CDN_KGL_KON.CDN.GetContractorsForConsolidation");

                logFile.Write("Stworzono listę kontrahentów do konsolidacji, " + dt.Rows.Count + " pozycji.");

            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy tworzeniu listy kontrahentów do konsolidacji: " + e.Message);
            }

            return dt;
        }

        private DataTable GetKntConsolidationListMock()
        {
            try
            {
                SqlConnect();
                DataTable dt = sql.Execute("select 1 as Kns_KnsId, 'CDN_Dotykacka' as Kns_ZrdBaza, 602 as Kns_ZrdKntId, 'Test ECP POLSKA' as Kns_Kod, Rab_Rabat, CDN.Kontrahenci.*, BNa_Numer, FPl_Nazwa, SprKat.Kat_KodSzczegol as SprKat_KodSzczegol, ZakKat.Kat_KodSzczegol as ZakKat_KodSzczegol from CDN.Kontrahenci join CDN.BnkNazwy on BNa_BNaId = Knt_BNaId left join CDN.FormyPlatnosci on FPl_FPlId = Knt_FplID left join CDN.Kategorie SprKat on SprKat.Kat_KatID = Knt_KatID left join CDN.Kategorie ZakKat on ZakKat.Kat_KatID = Knt_KatZakID join CDN.Rabaty on Rab_Typ = 2 and Rab_PodmiotTyp = 1 and Rab_PodmiotId = Knt_KntId where Knt_KntId = 602");

                return dt;
            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy tworzeniu listy kontrahentów do konsolidacji: " + e.Message);
            }
        }

        public void SetTargetKntId(int KnsId, int KnsKntId, string KnsKod)
        {
            if (debug)
                return;

            try
            {
                SqlConnect();
                sql.Execute("update CDN_KGL_KON.CDN.PROKonsolidacja set Kns_KntId = " + KnsKntId + " where Kns_KnsId = " + KnsId);
                logFile.Write("Zaktualizowano KntId dla kontrahenta o kodzie docelowym [" + KnsKod + "]");
            }
            catch (Exception e)
            {
                SqlDisconnect();
                throw new Exception("Błąd przy aktualizowaniu KntId kontrahenta o docelowym kodzie [" + KnsKod + "]: " + e.Message);
            }
        }

        private DataTable GetRepresentatives(string baza, int KntId, string KntCode)
        {
            DataTable dt;
            try
            {
                SqlConnect();

                dt = sql.Execute("select * from " + baza + ".CDN.KntOsoby where KnO_KntId = " + KntId);

                logFile.Write("Stworzono listę przedstawicieli dla kontrahenta o kodzie " + KntCode + " o ilości: " + dt.Rows.Count + " pozycji.");

            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy tworzeniu listy przedstawicieli dla kontrahenta o kodzie [" + KntCode + "] z bazy [" + baza + "], " + e.Message);
            }

            return dt;
        }

        private DataTable GetReceivers(string baza, int KntId, string KntCode)
        {
            DataTable dt;
            try
            {
                SqlConnect();

                dt = sql.Execute("select * " +
                           "from " + baza + ".CDN.KntOdbiorcy " +
                           "join " + baza + ".CDN.Kontrahenci on Knt_KntId = Odb_OdbKntID " +
                           "where Odb_KntOdbID = " + KntId);

                logFile.Write("Stworzono listę odbiorców dla kontrahenta o kodzie " + KntCode + " o ilości: " + dt.Rows.Count + " pozycji.");

            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy tworzeniu listy odbiorców dla kontrahenta o kodzie [" + KntCode + "] z bazy [" + baza + "], " + e.Message);
            }

            return dt;
        }

        private DataTable GetBankAccountNumbers(string baza, int KntId, string KntCode)
        {
            DataTable dt;
            try
            {
                SqlConnect();

                dt = sql.Execute("select * " +
                            "from " + baza + ".CDN.SchematPlatnosci " +
                            "where SPL_PodmiotTyp = 1 and SPL_PodmiotID = " + KntId);

                logFile.Write("Stworzono listę numerów rachunków bankowych dla kontrahenta o kodzie [" + KntCode + "] o ilości: " + dt.Rows.Count + " pozycji.");

            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy tworzeniu listy numerów rachunków bankowych dla kontrahenta o kodzie [" + KntCode + "] z bazy [" + baza + "], " + e.Message);
            }

            return dt;
        }

        private DataTable GetBankData(string baza, int BnaId)
        {
            DataTable dt;
            try
            {
                SqlConnect();

                dt = sql.Execute("select * " +
                            "from " + baza + ".CDN.BnkNazwy " +
                            "where BNa_BNaId = " + BnaId);

                logFile.Write("Pobrano dane banku dla BnaId " + BnaId);

            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy tworzeniu listy danych banku dla BnaId [" + BnaId + "] z bazy [" + baza + "], " + e.Message);
            }

            return dt;
        }

        private DataTable GetAtributesData(string baza, int KntId, string KntCode)
        {
            DataTable dt;
            try
            {
                SqlConnect();

                dt = sql.Execute("select DeA_Kod, KnA_WartoscTxt " +
                            "from " + baza + ".CDN.KntAtrybuty " +
                            "join " + baza + ".CDN.DefAtrybuty on DeA_DeAId = KnA_DeAId " +
                            "join " + baza + ".CDN.Kontrahenci on Knt_KntId = KnA_PodmiotId and KnA_PodmiotTyp = 1 " +
                            "where Knt_KntId = " + KntId);

                logFile.Write("Pobrano listę atrybutów dla kontrahenta o kodzie [" + KntCode + "] o ilości: " + dt.Rows.Count + " pozycji.");

            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy pobieraniu listy atrybutów dla kontrahenta o kodzie [" + KntCode + "] z bazy [" + baza + "], " + e.Message);
            }

            return dt;
        }

        private DataTable GetBankFormats(string baza, string akronim)
        {
            DataTable dt;
            try
            {
                SqlConnect();

                dt = sql.Execute("exec CDN.BnkGetFormatsForConsolidation '" + baza + "', '" + akronim + "'");

                logFile.Write("Pobrano formaty banku o akronimie [" + akronim + "], z bazy [" + baza + "]");

            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy pobieraniu formatów banku o akronimie [" + akronim + "], z bazy [" + baza + "]: " + e.Message);
            }

            return dt;
        }

        private DataTable GetAgreesFromSource(string baza, int Id, string Code, string type)
        {
            DataTable dt;
            try
            {
                SqlConnect();

                string cmd = "select RZT_Tytul, RZT_Tresc, Poz.RZZ_Nazwa as Poz_Nazwa, Wyc.RZZ_Nazwa as Wyc_Nazwa, * " +
                            "from " + baza + ".CDN.PodmiotRejestracjaZgod " +
                            "join " + baza + ".CDN.RejestracjaZgodTresc on PRZ_TrescZgodyId = RZT_RZTId " +
                            "left join " + baza + ".CDN.RejestracjaZgodZrodlo Poz on PRZ_ZrodloPozyskaniaId = Poz.RZZ_RZZId " +
                            "left join " + baza + ".CDN.RejestracjaZgodZrodlo Wyc on PRZ_ZrodloWycofaniaId = Wyc.RZZ_RZZId ";


                if (type == "Kontrahent")
                    cmd += " where PRZ_PodmiotId = " + Id + " and PRZ_PodmiotTyp = 1";
                else if (type == "Przedstawiciel")
                    cmd += " where PRZ_PrzedstawicielId = " + Id;

                dt = sql.Execute(cmd);

                logFile.Write("Pobrano listę zgód dla " + type + "a o kodzie [" + Code + "] o ilości: " + dt.Rows.Count + " pozycji.");

            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy pobieraniu listy zgód dla kontrahenta o kodzie [" + Code + "] z bazy [" + baza + "], " + e.Message);
            }

            return dt;
        }

        private DataTable GetAgreesSourceFromTarget(string Rzz_Nazwa)
        {
            DataTable dt;
            try
            {
                SqlConnect();

                dt = sql.Execute("select RZZ_RZZId " +
                            "from CDN.RejestracjaZgodZrodlo where RZZ_Nazwa = '" + Rzz_Nazwa + "'");

                if (dt.Rows.Count == 0)
                    throw new Exception();

            }
            catch (Exception)
            {
                throw new Exception("Nie znaleziono ZgodyŹródło o nazwie + [" + Rzz_Nazwa + "]");
            }

            return dt;
        }

        private DataTable GetAgreesContentFromTarget(string RZT_Tresc, string RZT_Tytul)
        {
            DataTable dt;
            try
            {
                SqlConnect();

                dt = sql.Execute("select RZT_RZTId " +
                            "from CDN.RejestracjaZgodTresc where RZT_Tytul = '" + RZT_Tytul + "' and RZT_Tresc = '" + RZT_Tresc + "'");

                if (dt.Rows.Count == 0)
                    throw new Exception();
            }
            catch (Exception)
            {
                throw new Exception("Nie znaleziono ZgodyTresci o tresci + [" + RZT_Tresc + "]  i tytule [" + RZT_Tytul + "]");
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

                Object output = sql.ExecuteFunction("select CDN_KGL_KON.CDN.PROConvertValue(@Typ, @NazwaZ, @BazaZ)", paramss);
                if (output == null || output == DBNull.Value)
                    throw new Exception("Nie znaleziono wartości!");

                return output.ToString();
            }
            catch (Exception e)
            {
                throw new Exception("Błąd przy pobieraniu skonwertowanego obiektu typu [" + type + "] o nazwie źródłowej [" + code + "] " + e.Message);
            }
        }

        public int CreateContractor(DataRow row)
        {
            try
            {
                OptimaConnect(OptimaTargetCompany);
                CDNHeal.Kontrahent targetContractor = optima.GetContractorCollection().AddNew();
                int NewKntId;

                try
                {

                    //--------------------------------------------------- Handlowe -----------------------------------------------------//
                    {
                        // Statusy
                        targetContractor.Finalny = row.Field<byte>("Knt_Finalny");
                        targetContractor.Export = row.Field<byte>("Knt_Export");
                        targetContractor.PodatekVAT = row.Field<byte>("Knt_PodatekVat"); ;
                        targetContractor.Medialny = row.Field<byte>("Knt_Medialny");
                        targetContractor.Rolnik = row.Field<byte>("Knt_Rolnik");
                        targetContractor.PowiazanyUoV = row.Field<byte>("Knt_PowiazanyUoV");
                        targetContractor.Chroniony = row.Field<byte>("Knt_Chroniony");
                        targetContractor.ZakazDokumentowHaMag = row.Field<byte>("Knt_ZakazDokumentowHaMag");
                        targetContractor.Nieaktywny = row.Field<byte>("Knt_Nieaktywny");
                        targetContractor.MetodaKasowa = row.Field<byte>("Knt_MetodaKasowa");

                        // Warunki handolowe
                        targetContractor.LimitFlag = row.Field<byte>("Knt_LimitFlag");
                        targetContractor.LimitKredytu = (double)row.Field<Decimal>("Knt_LimitKredytu");
                        targetContractor.LimitPrzeterKredytFlag = row.Field<byte>("Knt_LimitPrzeterKredytFlag");
                        targetContractor.LimitPrzeterKredytWartosc = (double)row.Field<Decimal>("Knt_LimitPrzeterKredytWartosc");
                        targetContractor.Ceny = 0;
                        targetContractor.Algorytm = row.Field<byte>("Knt_Algorytm");

                        // Opis
                        targetContractor.Opis = row.Field<string>("Knt_Opis");
                    }

                    //--------------------------------------------------- OGÓLNE -------------------------------------------------------//
                    {
                        // Dane ogólne
                        targetContractor.Akronim = row.Field<string>("Kns_Kod");
                        if (row.Field<string>("Knt_Grupa") != "")
                        {
                            string grupa = GetConvertedValue("GrK", row.Field<string>("Kns_ZrdBaza"), row.Field<string>("Knt_Grupa"));
                            if (grupa != "Nie_Przenosić")
                                targetContractor.Grupa = grupa;
                            else
                                logFile.Write("Uwaga! Nie znaleziono grupy kontrahenta [" + grupa + "]");
                        }

                        targetContractor.Rodzaj_Odbiorca = row.Field<byte>("Knt_Rodzaj_Odbiorca");
                        targetContractor.Rodzaj_Dostawca = row.Field<byte>("Knt_Rodzaj_Dostawca");
                        targetContractor.Rodzaj_Konkurencja = row.Field<byte>("Knt_Rodzaj_Konkurencja");
                        targetContractor.Rodzaj_Partner = row.Field<byte>("Knt_Rodzaj_Partner");
                        targetContractor.Rodzaj_Potencjalny = row.Field<byte>("Knt_Rodzaj_Potencjalny");
                        targetContractor.Nazwa1 = row.Field<string>("Knt_Nazwa1");
                        targetContractor.Nazwa2 = row.Field<string>("Knt_Nazwa2");
                        targetContractor.Nazwa3 = row.Field<string>("Knt_Nazwa3");
                        targetContractor.NumerNIP.NIPKraj = row.Field<string>("Knt_NipKraj");
                        targetContractor.NumerNIP.Nip = row.Field<string>("Knt_Nip");
                        targetContractor.Regon = row.Field<string>("Knt_Regon");
                        targetContractor.Pesel = row.Field<string>("Knt_Pesel");
                        targetContractor.DokumentTozsamosci = row.Field<string>("Knt_DokumentTozsamosci");

                        // Dane teleadresowe
                        targetContractor.Adres.Kraj = row.Field<string>("Knt_Kraj");
                        targetContractor.Adres.Miasto = row.Field<string>("Knt_Miasto");
                        targetContractor.Adres.Poczta = row.Field<string>("Knt_Poczta");
                        targetContractor.Adres.Ulica = row.Field<string>("Knt_Ulica");
                        targetContractor.KrajISO = row.Field<string>("Knt_KrajISO");
                        targetContractor.Adres.Wojewodztwo = row.Field<string>("Knt_Wojewodztwo");
                        targetContractor.Adres.Powiat = row.Field<string>("Knt_Powiat");
                        targetContractor.Adres.Gmina = row.Field<string>("Knt_Gmina");
                        targetContractor.Adres.KodPocztowy = row.Field<string>("Knt_KodPocztowy");
                        targetContractor.Adres.NrDomu = row.Field<string>("Knt_NrDomu");
                        targetContractor.Adres.NrLokalu = row.Field<string>("Knt_NrLokalu");
                        targetContractor.Adres2 = row.Field<string>("Knt_Adres2");
                        targetContractor.Telefon = row.Field<string>("Knt_Telefon1");
                        targetContractor.Telefon2 = row.Field<string>("Knt_Telefon2");
                        targetContractor.TelefonSms = row.Field<string>("Knt_TelefonSms");
                        targetContractor.Fax = row.Field<string>("Knt_Fax");
                        targetContractor.Email = row.Field<string>("Knt_Email");
                        targetContractor.URL = row.Field<string>("Knt_URL");

                        // Adres korespondacyjny
                        targetContractor.KorMiasto = row.Field<string>("Knt_KorMiasto");
                        targetContractor.KorPoczta = row.Field<string>("Knt_KorPoczta");
                        targetContractor.KorUlica = row.Field<string>("Knt_KorUlica");
                        targetContractor.KorKraj = row.Field<string>("Knt_KorKraj");
                        targetContractor.KorKodPocztowy = row.Field<string>("Knt_KorKodPocztowy");
                        targetContractor.KorNrDomu = row.Field<string>("Knt_KorNrDomu");
                        targetContractor.KorNrLokalu = row.Field<string>("Knt_KorNrLokalu");

                        // Inne
                        /* kategorie olewamy
                        if (!row.IsNull("SprKat_KodSzczegol"))
                            targetContractor.Kategoria = optima.GetCategoryByCode(GetConvertedValue("Kat", row.Field<string>("Kns_ZrdBaza"), row.Field<string>("SprKat_KodSzczegol")));
                        if (!row.IsNull("ZakKat_KodSzczegol"))
                            targetContractor.KategoriaZak = optima.GetCategoryByCode(GetConvertedValue("Kat", row.Field<string>("Kns_ZrdBaza"), row.Field<string>("ZakKat_KodSzczegol")));
                        */
                        targetContractor.Zezwolenie = row.Field<string>("Knt_Zezwolenie");
                        targetContractor.EAN = row.Field<string>("Knt_EAN");
                        targetContractor.GLN = row.Field<string>("Knt_GLN");
                    }

                    //--------------------------------------------------- Płatności ----------------------------------------------------//
                    {
                        // Płatności
                        targetContractor.FormaPlatnosci = optima.GetPaymentMethodbyName(GetConvertedValue("SPł", row.Field<string>("Kns_ZrdBaza"), row.Field<string>("FPl_Nazwa")));

                        if (row.Field<string>("Knt_Waluta") != "")
                            targetContractor.Waluta = optima.GetCurrencyBySymbol(row.Field<string>("Knt_Waluta"));
                        targetContractor.NieRozliczac = row.Field<byte>("Knt_NieRozliczac");
                        targetContractor.SplitPay = row.Field<byte>("Knt_SplitPay");
                        targetContractor.NieNaliczajOdsetek = row.Field<byte>("Knt_NieNaliczajOdsetek");
                        targetContractor.TerminPlat = row.Field<byte>("Knt_TerminPlat");
                        if (!row.IsNull("Knt_Termin"))
                            targetContractor.Termin = row.Field<Int16>("Knt_Termin");
                        if (!row.IsNull("Knt_MaxZwloka"))
                            targetContractor.MaxZwloka = row.Field<Int16>("Knt_MaxZwloka");

                        // Numery rachunków bankowych
                        DataTable bankAccountNumbers = GetBankAccountNumbers(row.Field<string>("Kns_ZrdBaza"), row.Field<int>("Kns_ZrdKntId"), row.Field<string>("Knt_Kod"));
                        foreach (DataRow bankAccountNumber in bankAccountNumbers.Rows)
                        {
                            CDNHeal.IBank bank;
                            if (!bankAccountNumber.IsNull("SPL_BnaId"))
                            {
                                DataRow bankData = GetBankData(row.Field<string>("Kns_ZrdBaza"), bankAccountNumber.Field<int>("SPL_BnaId")).Rows[0];

                                try
                                {
                                    bank = optima.GetBankByNumber(bankData.Field<string>("BNa_Numer"));
                                }
                                catch (Exception e)
                                {
                                    if (e.Message.StartsWith("Nie znaleziono banku"))
                                    {
                                        bank = optima.GetBankCollection().AddNew();

                                        if (!bankData.IsNull("BNa_Zagraniczny"))
                                            bank.Zagraniczny = bankData.Field<Int16>("BNa_Zagraniczny");

                                        bank.Akronim = bankData.Field<string>("BNa_Akronim");
                                        bank.Numer = bankData.Field<string>("BNa_Numer");
                                        bank.Nazwa1 = bankData.Field<string>("BNa_Nazwa1");
                                        bank.Nazwa2 = bankData.Field<string>("BNa_Nazwa2");
                                        bank.Swift = bankData.Field<string>("BNa_Swift");
                                        bank.Centrala = bankData.Field<string>("BNa_Centrala");

                                        bank.Adres.Kraj = bankData.Field<string>("BNa_Kraj");
                                        bank.Adres.Wojewodztwo = bankData.Field<string>("BNa_Wojewodztwo");
                                        bank.Adres.Powiat = bankData.Field<string>("BNa_Powiat");
                                        bank.Adres.Gmina = bankData.Field<string>("BNa_Gmina");
                                        bank.Adres.Miasto = bankData.Field<string>("BNa_Miasto");
                                        bank.Adres.Ulica = bankData.Field<string>("BNa_Ulica");
                                        bank.Adres.KodPocztowy = bankData.Field<string>("BNa_KodPocztowy");
                                        bank.Adres.NrDomu = bankData.Field<string>("BNa_NrDomu");
                                        bank.Adres.NrLokalu = bankData.Field<string>("BNa_NrLokalu");
                                        bank.Adres.Dodatkowe = bankData.Field<string>("BNa_Adres2");

                                        bank.Telefon = bankData.Field<string>("BNa_Telefon");
                                        bank.TelefonSms = bankData.Field<string>("BNa_TelefonSms");
                                        bank.Fax = bankData.Field<string>("BNa_Fax");
                                        bank.Email = bankData.Field<string>("BNa_Email");

                                        if (!bankData.IsNull("BNa_CzasRealizacji"))
                                            bank.CzasRealizacji = bankData.Field<Int16>("BNa_CzasRealizacji");
                                        bank.Modem = bankData.Field<string>("BNa_Modem");
                                        bank.UID = bankData.Field<string>("BNa_UID");
                                        bank.PWD = bankData.Field<string>("BNa_PWD");
                                        bank.URL = bankData.Field<string>("BNa_URL");


                                        if (!bankData.IsNull("BNa_IBAN"))
                                            bank.IBAN = bankData.Field<Int16>("BNa_IBAN");
                                        bank.NieRozliczac = bankData.Field<byte>("BNa_NieRozliczac");
                                        bank.Nieaktywny = bankData.Field<byte>("BNa_Nieaktywny");

                                        if (!bankData.IsNull("BNa_Format") ||
                                            !bankData.IsNull("BNa_FormatSplitPay") ||
                                            !bankData.IsNull("BNa_FormatUS") ||
                                            !bankData.IsNull("BNa_FormatZUS") ||
                                            !bankData.IsNull("BNa_FormatImp"))
                                        {
                                            DataTable bankFormats = GetBankFormats(row.Field<string>("Kns_ZrdBaza"), bank.Akronim);

                                            if (bankFormats.Rows.Count == 0)
                                            {
                                                logFile.Write("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiedników formatów");
                                                errLogFile.Write("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiedników formatów");
                                                Console.WriteLine("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiedników formatów");
                                            }
                                            else
                                            {
                                                DataRow bankFormat = bankFormats.Rows[0];
                                                if (!bankData.IsNull("BNa_Format"))
                                                {
                                                    if (!bankFormat.IsNull("Zwykle"))
                                                        bank.Format = bankFormat.Field<Int16>("Zwykle");
                                                    else
                                                    {
                                                        logFile.Write("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_Format");
                                                        errLogFile.Write("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_Format");
                                                        Console.WriteLine("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_Format");
                                                    }
                                                }

                                                if (!bankData.IsNull("BNa_FormatSplitPay"))
                                                {
                                                    if (!bankFormat.IsNull("SplitPay"))
                                                        bank.FormatSplitPay = bankFormat.Field<Int16>("SplitPay");
                                                    else
                                                    {
                                                        logFile.Write("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_FormatSplitPay");
                                                        errLogFile.Write("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_FormatSplitPay");
                                                        Console.WriteLine("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_FormatSplitPay");
                                                    }
                                                }

                                                if (!bankData.IsNull("BNa_FormatUS"))
                                                {
                                                    if (!bankFormat.IsNull("US"))
                                                        bank.FormatUS = bankFormat.Field<Int16>("US");
                                                    else
                                                    {
                                                        logFile.Write("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_FormatUS");
                                                        errLogFile.Write("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_FormatUS");
                                                        Console.WriteLine("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_FormatUS");
                                                    }
                                                }

                                                if (!bankData.IsNull("BNa_FormatZUS"))
                                                {
                                                    if (!bankFormat.IsNull("ZUS"))
                                                        bank.FormatZUS = bankFormat.Field<Int16>("ZUS");
                                                    else
                                                    {
                                                        logFile.Write("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_FormatZUS");
                                                        errLogFile.Write("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_FormatZUS");
                                                        Console.WriteLine("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_FormatZUS");
                                                    }
                                                }

                                                if (!bankData.IsNull("BNa_FormatImp"))
                                                {
                                                    if (!bankFormat.IsNull("IMP"))
                                                        bank.FormatImp = bankFormat.Field<Int16>("IMP");
                                                    else
                                                    {
                                                        logFile.Write("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_FormatImp");
                                                        errLogFile.Write("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_FormatImp");
                                                        Console.WriteLine("Błąd dla kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + ", banku o numerze [" + bankData.Field<string>("BNa_Numer") + "] nie znaleziono odpowiednika formatu BNa_FormatImp");
                                                    }
                                                }
                                            }
                                        }

                                        OptimaSaveWithoutSessionReset();
                                    }
                                    else
                                        throw new Exception(e.Message);
                                }

                                targetContractor.DodajRachunek(bank,
                                                        bankAccountNumber.Field<string>("SPL_RachunekNr"),
                                                        bankAccountNumber.Field<byte>("SPL_IBAN"),
                                                        bankAccountNumber.Field<string>("SPL_RachunekNr") == row.Field<string>("Knt_RachunekNr") ? 1 : 0,
                                                        bankAccountNumber.Field<string>("SPL_Opis"));

                            }
                            else
                            {

                                targetContractor.DodajRachunek(null,
                                                            bankAccountNumber.Field<string>("SPL_RachunekNr"),
                                                            bankAccountNumber.Field<byte>("SPL_IBAN"),
                                                            bankAccountNumber.Field<string>("SPL_RachunekNr") == row.Field<string>("Knt_RachunekNr") ? 1 : 0,
                                                            bankAccountNumber.Field<string>("SPL_Opis"));
                            }
                        }
                    }

                    //--------------------------------------------------- Dodatkowe ----------------------------------------------------//
                    {
                        // Przedstawiciele
                        DataTable reprezentatives = GetRepresentatives(row.Field<string>("Kns_ZrdBaza"), row.Field<int>("Kns_ZrdKntId"), row.Field<string>("Knt_Kod"));
                        DataTable reprezentativeAggres;
                        foreach (DataRow reprezentative in reprezentatives.Rows)
                        {
                            CDNHeal.Przedstawiciel targetReprezentative = targetContractor.Przedstawiciele.AddNew();

                            targetReprezentative.Nazwisko = reprezentative.Field<string>("KnO_Nazwisko");
                            targetReprezentative.Tytul = reprezentative.Field<string>("KnO_Tytul");
                            targetReprezentative.Kraj = reprezentative.Field<string>("KnO_Kraj");
                            targetReprezentative.Miasto = reprezentative.Field<string>("KnO_Miasto");
                            targetReprezentative.Poczta = reprezentative.Field<string>("KnO_Poczta");
                            targetReprezentative.Ulica = reprezentative.Field<string>("KnO_Ulica");
                            targetReprezentative.Adres2 = reprezentative.Field<string>("KnO_Adres2");
                            targetReprezentative.Telefon = reprezentative.Field<string>("KnO_Telefon");
                            targetReprezentative.GSM = reprezentative.Field<string>("KnO_GSM");
                            targetReprezentative.TelefonSms = reprezentative.Field<string>("KnO_TelefonSms");

                            targetReprezentative.Plec = reprezentative.Field<byte>("KnO_Plec");
                            targetReprezentative.Wojewodztwo = reprezentative.Field<string>("KnO_Wojewodztwo");
                            targetReprezentative.KodPocztowy = reprezentative.Field<string>("KnO_KodPocztowy");
                            targetReprezentative.NrDomu = reprezentative.Field<string>("KnO_NrDomu");
                            targetReprezentative.NrLokalu = reprezentative.Field<string>("KnO_NrLokalu");
                            targetReprezentative.Email = reprezentative.Field<string>("KnO_Email");
                            targetReprezentative.Mailing = reprezentative.Field<byte>("KnO_Mailing");
                            targetReprezentative.Informacje = reprezentative.Field<byte>("KnO_Informacje");

                            targetReprezentative.Nieaktywny = reprezentative.Field<byte>("KnO_Nieaktywny");

                            optima.SaveWithoutSessionRefresh();
                            if (!row.IsNull("Knt_WindykacjaOsobaId") && row.Field<int>("Knt_WindykacjaOsobaId") == reprezentative.Field<int>("KnO_KnOId"))
                            {
                                targetContractor.WindykacjaOsobaId = targetReprezentative.ID;
                                targetContractor.WindykacjaEMail = targetReprezentative.Email;
                                targetContractor.WindykacjaTelefonSms = targetReprezentative.TelefonSms;
                            }

                            reprezentativeAggres = GetAgreesFromSource(row.Field<string>("Kns_ZrdBaza"), reprezentative.Field<int>("KnO_KnOId"), reprezentative.Field<string>("KnO_Nazwisko"), "Przedstawiciel");
                            foreach (DataRow agree in reprezentativeAggres.Rows)
                            {
                                CDNHeal.IPodmiotRejestracjaZgod zgoda = targetReprezentative.Zgody.AddNew();
                                zgoda.TrescZgodyId = GetAgreesContentFromTarget(agree.Field<string>("RZT_Tresc"), agree.Field<string>("RZT_Tytul")).Rows[0].Field<int>("RZT_RZTId");

                                zgoda.ZrodloPozyskaniaId = GetAgreesSourceFromTarget(agree.Field<string>("Poz_Nazwa")).Rows[0].Field<int>("RZZ_RZZId");
                                zgoda.IPWyrazenia = agree.Field<string>("PRZ_IPWyrazenia");
                                zgoda.DataWyrazenia = agree.Field<DateTime>("PRZ_DataWyrazenia");

                                if (!agree.IsNull("Wyc_Nazwa"))
                                {
                                    zgoda.ZrodloWycofaniaId = GetAgreesSourceFromTarget(agree.Field<string>("Wyc_Nazwa")).Rows[0].Field<int>("RZZ_RZZId");
                                    zgoda.IPWycofania = agree.Field<string>("PRZ_IPWycofania");
                                    if (!agree.IsNull("PRZ_DataWycofania"))
                                        zgoda.DataWycofania = agree.Field<DateTime>("PRZ_DataWycofania");
                                    zgoda.PowodWycofania = agree.Field<string>("PRZ_PowodWycofania");
                                }


                            }

                        }
                        targetContractor.Domena = row.Field<string>("Knt_Domena");

                        // Operator
                        if (!row.IsNull("Knt_OpiekunTyp"))
                        {
                            targetContractor.OpiekunTyp = row.Field<Int16>("Knt_OpiekunTyp");
                            switch (row.Field<Int16>("Knt_OpiekunTyp"))
                            {
                                case 8: // Operator
                                    targetContractor.OpiekunID = row.Field<int>("Knt_OpiekunId");
                                    break;
                                case 3: // Pracownik
                                        //@@@@ KONWERSJA?
                                    break;
                            }
                        }
                        targetContractor.LoginComarchCloud = row.Field<string>("Knt_LoginComarchCloud");

                        // Odbiorcy
                        DataTable receivers = GetReceivers(row.Field<string>("Kns_ZrdBaza"), row.Field<int>("Kns_ZrdKntId"), row.Field<string>("Knt_Kod"));
                        foreach (DataRow receiver in receivers.Rows)
                        {
                            CDNHeal.Kontrahent targetReceiverKnt;

                            try
                            {
                                targetReceiverKnt = optima.GetContractorByName(receiver.Field<string>("Knt_Kod"));
                            }
                            catch (Exception e)
                            {
                                if (e.Message.StartsWith("Nie znaleziono kontrahenta"))
                                {
                                    targetReceiverKnt = optima.GetContractorCollection().AddNew();

                                    // Ogólne
                                    targetReceiverKnt.Akronim = receiver.Field<string>("Knt_Kod");
                                    targetReceiverKnt.Grupa = receiver.Field<string>("Knt_Grupa");
                                    targetReceiverKnt.Rodzaj_Odbiorca = receiver.Field<byte>("Knt_Rodzaj_Odbiorca");
                                    targetReceiverKnt.Rodzaj_Dostawca = receiver.Field<byte>("Knt_Rodzaj_Dostawca");
                                    targetReceiverKnt.Rodzaj_Konkurencja = receiver.Field<byte>("Knt_Rodzaj_Konkurencja");
                                    targetReceiverKnt.Rodzaj_Partner = receiver.Field<byte>("Knt_Rodzaj_Partner");
                                    targetReceiverKnt.Rodzaj_Potencjalny = receiver.Field<byte>("Knt_Rodzaj_Potencjalny");
                                    targetReceiverKnt.Nazwa1 = receiver.Field<string>("Knt_Nazwa1");
                                    targetReceiverKnt.Nazwa2 = receiver.Field<string>("Knt_Nazwa2");
                                    targetReceiverKnt.Nazwa3 = receiver.Field<string>("Knt_Nazwa3");
                                    targetReceiverKnt.NumerNIP.Nip = receiver.Field<string>("Knt_Nip");
                                    targetReceiverKnt.NumerNIP.NIPKraj = receiver.Field<string>("Knt_NipKraj");
                                    targetReceiverKnt.Regon = receiver.Field<string>("Knt_Regon");
                                    targetReceiverKnt.Pesel = receiver.Field<string>("Knt_Pesel");
                                    targetReceiverKnt.DokumentTozsamosci = receiver.Field<string>("Knt_DokumentTozsamosci");

                                    // Dane teleadresowe
                                    targetReceiverKnt.Adres.Kraj = receiver.Field<string>("Knt_Kraj");
                                    targetReceiverKnt.Adres.Miasto = receiver.Field<string>("Knt_Miasto");
                                    targetReceiverKnt.Adres.Poczta = receiver.Field<string>("Knt_Poczta");
                                    targetReceiverKnt.Adres.Ulica = receiver.Field<string>("Knt_Ulica");
                                    targetReceiverKnt.KrajISO = receiver.Field<string>("Knt_KrajISO");
                                    targetReceiverKnt.Adres.Wojewodztwo = receiver.Field<string>("Knt_Wojewodztwo");
                                    targetReceiverKnt.Adres.Powiat = receiver.Field<string>("Knt_Powiat");
                                    targetReceiverKnt.Adres.Gmina = receiver.Field<string>("Knt_Gmina");
                                    targetReceiverKnt.Adres.KodPocztowy = receiver.Field<string>("Knt_KodPocztowy");
                                    targetReceiverKnt.Adres.NrDomu = receiver.Field<string>("Knt_NrDomu");
                                    targetReceiverKnt.Adres.NrLokalu = receiver.Field<string>("Knt_NrLokalu");
                                    targetReceiverKnt.Adres2 = receiver.Field<string>("Knt_Adres2");
                                    targetReceiverKnt.Telefon = receiver.Field<string>("Knt_Telefon1");
                                    targetReceiverKnt.Telefon2 = receiver.Field<string>("Knt_Telefon2");
                                    targetReceiverKnt.TelefonSms = receiver.Field<string>("Knt_TelefonSms");
                                    targetReceiverKnt.Fax = receiver.Field<string>("Knt_Fax");
                                    targetReceiverKnt.Email = receiver.Field<string>("Knt_Email");
                                    targetReceiverKnt.URL = receiver.Field<string>("Knt_URL");

                                    // Adres korespondacyjny
                                    targetReceiverKnt.KorMiasto = receiver.Field<string>("Knt_KorMiasto");
                                    targetReceiverKnt.KorPoczta = receiver.Field<string>("Knt_KorPoczta");
                                    targetReceiverKnt.KorUlica = receiver.Field<string>("Knt_KorUlica");
                                    targetReceiverKnt.KorKraj = receiver.Field<string>("Knt_KorKraj");
                                    targetReceiverKnt.KorKodPocztowy = receiver.Field<string>("Knt_KorKodPocztowy");
                                    targetReceiverKnt.KorNrDomu = receiver.Field<string>("Knt_KorNrDomu");
                                    targetReceiverKnt.KorNrLokalu = receiver.Field<string>("Knt_KorNrLokalu");

                                    // Inne
                                    targetReceiverKnt.Zezwolenie = receiver.Field<string>("Knt_Zezwolenie");
                                    targetReceiverKnt.EAN = receiver.Field<string>("Knt_EAN");
                                    targetReceiverKnt.GLN = receiver.Field<string>("Knt_GLN");

                                    OptimaSaveWithoutSessionReset();
                                }
                                else
                                    throw new Exception(e.Message);
                            }

                            CDNHeal.KntOdbiorca targetReceiver = targetContractor.Odbiorcy.AddNew();

                            targetReceiver.OdbKntID = targetReceiverKnt.ID;
                        }
                    }

                    //--------------------------------------------------- Zgody --------------------------------------------------------//
                    {
                        DataTable agrees = GetAgreesFromSource(row.Field<string>("Kns_ZrdBaza"), row.Field<int>("Kns_ZrdKntId"), row.Field<string>("Knt_Kod"), "Kontrahent");
                        foreach (DataRow agree in agrees.Rows)
                        {
                            CDNHeal.IPodmiotRejestracjaZgod zgoda = targetContractor.Zgody.AddNew();
                            zgoda.TrescZgodyId = GetAgreesContentFromTarget(agree.Field<string>("RZT_Tresc"), agree.Field<string>("RZT_Tytul")).Rows[0].Field<int>("RZT_RZTId");

                            zgoda.ZrodloPozyskaniaId = GetAgreesSourceFromTarget(agree.Field<string>("Poz_Nazwa")).Rows[0].Field<int>("RZZ_RZZId");
                            zgoda.IPWyrazenia = agree.Field<string>("PRZ_IPWyrazenia");
                            zgoda.DataWyrazenia = agree.Field<DateTime>("PRZ_DataWyrazenia");

                            if (!agree.IsNull("Wyc_Nazwa"))
                            {
                                zgoda.ZrodloWycofaniaId = GetAgreesSourceFromTarget(agree.Field<string>("Wyc_Nazwa")).Rows[0].Field<int>("RZZ_RZZId");
                                zgoda.IPWycofania = agree.Field<string>("PRZ_IPWycofania");
                                if (!agree.IsNull("PRZ_DataWycofania"))
                                    zgoda.DataWycofania = agree.Field<DateTime>("PRZ_DataWycofania");
                                zgoda.PowodWycofania = agree.Field<string>("PRZ_PowodWycofania");
                            }
                        }

                    }

                    //--------------------------------------------------- Atrybuty -----------------------------------------------------//
                    {
                        DataTable attributes = GetAtributesData(row.Field<string>("Kns_ZrdBaza"), row.Field<int>("Kns_ZrdKntId"), row.Field<string>("Knt_Kod"));
                        foreach (DataRow attribute in attributes.Rows)
                        {
                            string value = GetConvertedValue("AtK", row.Field<string>("Kns_ZrdBaza"), attribute.Field<string>("DeA_Kod"));
                            if (value == "Nie_Przenosić")
                                continue;

                            CDNTwrb1.KntAtrybut targetAttribute = targetContractor.Atrybuty.AddNew();
                            targetAttribute.DeAID = optima.GetDefAtribute(value, 2).ID;
                            targetAttribute.Wartosc = attribute.Field<string>("KnA_WartoscTxt");
                        }

                    }

                    //--------------------------------------------------- Komornik -----------------------------------------------------//
                    {
                        targetContractor.Komornik = row.Field<byte>("Knt_Komornik");
                        if (!row.IsNull("Knt_KomornikOkreg"))
                            targetContractor.KomornikOkreg = row.Field<string>("Knt_KomornikOkreg");
                        if (!row.IsNull("Knt_KomornikMiasto"))
                            targetContractor.KomornikMiasto = row.Field<string>("Knt_KomornikMiasto");
                        if (!row.IsNull("Knt_KomornikRewir"))
                            targetContractor.KomornikRewir = row.Field<string>("Knt_KomornikRewir");
                    }

                    OptimaSaveWithoutSessionReset();

                    // Handlowe - warunki handlowe - rabat standardowy
                    if(!row.IsNull("Rab_Rabat"))
                    {
                        CDNTwrb1.Rabat rabat = optima.CreateRabat();
                        rabat.Typ = 2;
                        rabat.TypCenyNB = 1;
                        rabat.PodmiotTyp = 1;
                        rabat.PodmiotID = targetContractor.ID;
                        rabat.Wartosc = row.Field<Decimal>("Rab_Rabat");
                    }

                    NewKntId = targetContractor.ID;
                    OptimaSave();

                    logFile.Write("Stworzono kontrahenta o kodzie docelowym: [" + row.Field<string>("Kns_Kod") + "], KntId = [" + NewKntId + "]");

                    return NewKntId;
                }
                catch (Exception e)
                {
                    try
                    {
                        OptimaSaveWithoutSessionReset();
                        optima.GetContractorCollection().Delete(optima.GetContractorByID(targetContractor.ID));
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
                throw new Exception("Błąd przy tworzeniu obiektu kontrahenta o docelowym kodzie [" + row.Field<string>("Kns_Kod") + "]: " + e.Message);
            }
        }
    }
}
