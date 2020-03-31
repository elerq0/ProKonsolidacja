using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using PROLog;

namespace ProfessoftBaseFusion
{
    class DocCreator
    {
        private LogFile logFile;
        private DataModule dataModule;
        private DataTable dt;

        public int index;
        public readonly int limit;

        public bool running;
        public bool state;
        public bool error;

        public bool errorCheckAgainFlag;

        public string currentDocument;

        public DocCreator(LogFile logFile)
        {
            this.logFile = logFile;
            try
            {
                dataModule = new DataModule(logFile);

                dt = dataModule.GetSupplyDataTable();
                dataModule.CreateDefAtributes();

                index = 0;
                limit = dt.Rows.Count;

                running = false;
                state = false;
                error = false;

                errorCheckAgainFlag = true;

                currentDocument = string.Empty;
                dataModule.CreateDefAtributes();
            }
            catch(Exception e)
            {
                SetErrorOn();
                limit = 1;
                logFile.Write(e.Message);
            }

        }


        public void Run()
        {
            try
            {
                CDNHlmn.IDokumentHaMag doc = null;

                while(index < limit)
                {
                    if (!state)
                    {
                        running = false;
                        return;
                    }
                    else
                        running = true;

                    currentDocument = GetDocument(index);

                    if (GetStatus(index) == (int)SQLDocumentStates.COMPLETED || GetStatus(index) == (int)SQLDocumentStates.DROPPED)
                    {
                        index += 1;
                        continue;
                    }
                    else if (GetStatus(index) == (int)SQLDocumentStates.WORKING_WITH && !errorCheckAgainFlag)
                    {
                        dataModule.SetStatusForDocument(currentDocument, GetCompany(index), (int)SQLDocumentStates.DROPPED);
                        index += 1;
                        continue;
                    }

                    dataModule.SetStatusForDocument(currentDocument, GetCompany(index), (int)SQLDocumentStates.WORKING_WITH);
                    doc = dataModule.CreateDocument(GetCompany(index), GetWarehouse(index), currentDocument, GetContractor(index), GetDate(index), GetDocumentType(index));
                    do
                    {
                        dataModule.AddDocumentPosition(doc, GetGood(index), GetAmount(index), GetValue(index));
                        index++;
                    }
                    while (index < limit && GetWarehouse(index).Equals(GetWarehouse(index - 1)) && GetDocument(index).Equals(GetDocument(index - 1)) && GetCompany(index).Equals(GetCompany(index - 1)));

                    dataModule.OptimaSave();
                    dataModule.SetStatusForDocument(currentDocument, GetCompany(index), (int)SQLDocumentStates.COMPLETED);
                }
                running = false;
            }
            catch (Exception e)
            {
                SetErrorOn();
                logFile.Write(e.Message);
            }
        }

        public void Dispose()
        {
            dataModule.Dispose();
            logFile.Write("");
        }

        private string GetCompany(int i)
        {
            return dt.Rows[i].Field<string>(Extensions.SQLColumnNameCompany);
        }

        private string GetWarehouse(int i)
        {
            return dt.Rows[i].Field<string>(Extensions.SQLColumnNameWarehouse);
        }

        private string GetDocumentType(int i)
        {
            return dt.Rows[i].Field<string>(Extensions.SQLColumnNameDocumentType);
        }

        private string GetDocument(int i)
        {
            return dt.Rows[i].Field<string>(Extensions.SQLColumnNameDocument);
        }

        private string GetContractor(int i)
        {
            return dt.Rows[i].Field<string>(Extensions.SQLColumnNameContractor);
        }

        private int GetStatus(int i)
        {
            return dt.Rows[i].Field<int>(Extensions.SQLColumnNameStatus);
        }

        private DateTime GetDate(int i)
        {
            return dt.Rows[i].Field<DateTime>(Extensions.SQLColumnNameDate);
        }

        private string GetGood(int i)
        {
            return dt.Rows[i].Field<string>(Extensions.SQLColumnNameGood);
        }

        private decimal GetAmount(int i)
        {
            return dt.Rows[i].Field<decimal>(Extensions.SQLColumnNameAmount);
        }

        private decimal GetValue(int i)
        {
            return dt.Rows[i].Field<decimal>(Extensions.SQLColumnNameValue);
        }

        private void SetErrorOn()
        {            
            state = true;
            running = false;
            error = true;
        }

    }

}
