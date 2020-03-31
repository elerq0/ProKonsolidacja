using System;
using System.Timers;

namespace ProfessoftBaseFusion
{
    class Interface
    {
        private readonly int size = 70;
        private readonly string charracter = " ";
        private readonly string title = " Professoft Import BO App ";
        private readonly DateTime startDate;
        private UInt32 workSeconds;
        private Timer timer;
        private States state;

        private string background;

        public Interface()
        {
            Console.CursorVisible = false;
            Console.WriteLine("Loading ...");

            workSeconds = 0;
            startDate = DateTime.Now;

            timer = new Timer
            {
                Interval = 1000
            };
            timer.Elapsed += new ElapsedEventHandler(this.TimeIsRunning);

            background = GetLimiterString() + Environment.NewLine + 
                        GetTitleString() + Environment.NewLine +
                        Environment.NewLine +
                        GetLimiterString() + Environment.NewLine +
                        "_PROGRESS_BAR_" + Environment.NewLine +
                        Environment.NewLine +
                        GetLimiterString() + Environment.NewLine +
                        charracter + charracter + "Data uruchomienia: " + startDate + Environment.NewLine +
                        charracter + charracter + "Czas pracy: " + "_WORK_TIME_" + Environment.NewLine +
                        charracter + charracter + "Szacowany czas do zakończenia: " + "_REMAINING_TIME_" + Environment.NewLine +
                        charracter + charracter + "Aktualny stan: " + "_ACTUAL_STATE_" + Environment.NewLine +
                        charracter + charracter + "_CURRENT_DOCUMENT_INFO_" + Environment.NewLine +
                        Environment.NewLine;

        }

        public void Dispose()
        {
            Console.Clear();
            Console.WriteLine("Exiting ...");
            timer.Dispose();
        }

        public void Refresh(int index, int limit, string currentDocument, Actions action)
        {
            string newBackground = background
                                    .Replace("_PROGRESS_BAR_", GetProgressBar(index, limit))
                                    .Replace("_WORK_TIME_", GetWorkTime())
                                    .Replace("_REMAINING_TIME_", GetRemainingTime(index, limit))
                                    .Replace("_ACTUAL_STATE_", state.ToString())
                                    .Replace("_CURRENT_DOCUMENT_INFO_", GetCurrentDocumentInfo(currentDocument))
                                   ;
            newBackground += GetMenu(action) + Environment.NewLine;

            Console.Clear();
            Console.Write(newBackground);
        }

        private string GetMenu(Actions currentAction)
        {
            string str = string.Empty;
            string line = string.Empty;
            Actions[] actions = Extensions.GetActionsPerState(state);
            int length = 0;

            foreach(Actions action in actions)
            {
                line = string.Empty;
                length = (size - action.ToString().Length) / 2;

                if(action == currentAction)
                {
                    for (int i = 0; i < length - 1; i++)
                        line += " ";

                    line += ">" + action.ToString() + "<";

                    for (int i = line.Length; i < size - 1; i++)
                        line += " ";
                }
                else
                {
                    for (int i = 0; i < length; i++)
                        line += " ";

                    line += action.ToString();

                    for (int i = line.Length; i < size; i++)
                        line += " ";
                }
                str += line + Environment.NewLine;
            }

            return str;
        }

        public void SetState(States state)
        {
            this.state = state;
            switch(state)
            {
                case States.RUNNING:
                case States.STOPPING:
                    timer.Start();
                    break;
                default:
                    timer.Stop();
                    break;
            }
        }

        private string GetWorkTime()
        {
            int seconds = (int)(workSeconds % 60);
            int minutes = (int)(workSeconds / 60 % 60);
            int hours   = (int)(workSeconds / 3600);

            return GetZeroedString(hours, 2) + ":" + GetZeroedString(minutes, 2) + ":" + GetZeroedString(seconds, 2);
        }

        private string GetRemainingTime(int index, int limit)
        {

            string str = string.Empty;
            if (index == 0 || GetPercent(index, limit) <= 5)
                str = "NIEOKRESLONY";
            else
            {
                UInt32 remainingSeconds = (workSeconds  * (UInt32)(((double)limit - (double)index) / (double)(index) * 100)) / 100;

                int seconds = (int)(remainingSeconds % 60);
                int minutes = (int)(remainingSeconds / 60 % 60);
                int hours   = (int)(remainingSeconds / 3600);

                str = GetZeroedString(hours, 2) + ":" + GetZeroedString(minutes, 2) + ":" + GetZeroedString(seconds, 2);   
            }
            return str;
        }

        private string GetCurrentDocumentInfo(string document)
        {
            switch (state)
            {
                case States.RUNNING:
                case States.STOPPING:
                    {
                        return "Aktualnie przetwarzany dokument: [" + document + "]";
                    }
                case States.STOPPED:
                case States.COMPLETED:
                default:
                    {
                        switch (document)
                        {
                            case "":
                                return "";
                            default:
                                return "Ostatnio przetworzony dokument: [" + document + "]";
                        }
                    }
            }
        }

        private string GetLimiterString()
        {
            string str = string.Empty;
            for (int i = 0; i < size; i++)
                str += charracter;

            return str;
        }

        private string GetTitleString()
        {
            string str = String.Empty;
            int titleLength = (size - title.Length) / 2;

            if (titleLength > 0)
            {
                for (int i = 0; i < titleLength; i++)
                    str += charracter;

                str += title;

                for (int i = str.Length; i < size; i++)
                    str += charracter;
            }
            else
                str = title;

            return str;
        }

        private string GetProgressBar(int index, int limit)
        {
            string str = String.Empty;
            int progressLength = size - 12;

            if (progressLength > 0)
            {
                bool percentageAdded = false;
                str += charracter + charracter + "[";

                for (int i = 0; i < progressLength; i++)
                {
                    if (i == (progressLength / 2) && !percentageAdded)
                    {
                        str += "[" + GetZeroedString(GetPercent(index, limit), 3) + "%]";
                        percentageAdded = true;
                    }

                    if (i < ((decimal)index / (decimal)limit) * progressLength)
                    {
                        str += '#';
                    }
                    else
                    {
                        str += '-';
                    }
                }

                str += "]" + charracter + charracter;
            }
            else
                str = GetZeroedString((int)(((decimal)index / (decimal)limit) * 100), 3);

            return str;
        }

        private string GetZeroedString(int number, int ammount)
        {
            string str = number.ToString();
            while (str.Length < ammount)
            {
                str = "0" + str;
            }
            return str;
        }

        private int GetPercent(int complete, int total)
        {
            return (int)Math.Round((double)(100 * complete) / total);
        }

        private void TimeIsRunning(object sender, ElapsedEventArgs e)
        {
            workSeconds += 1;
        }

    }


  
}
