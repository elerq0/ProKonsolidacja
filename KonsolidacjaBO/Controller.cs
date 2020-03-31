using System;
using System.Threading;
using PROLog;

namespace ProfessoftBaseFusion
{
    class Controller
    {
        private readonly ConsoleKey ActionKey = ConsoleKey.Enter;

        private ConsoleKey selectedKey;
        private bool keyActionCompleted = true;

        private States state = States.START_WITH_ERROR_CHECK;
        private Actions action;

        public void Run()
        {
            Interface iface = new Interface();
            LogFile logFile = new LogFile(Properties.Settings.Default.LogFilePath);
            DocCreator docCreator = new DocCreator(logFile);
            Thread keyListener = new Thread(new ThreadStart(ReadKey));

            if (state == States.START_WITH_ERROR_CHECK)
                docCreator.errorCheckAgainFlag = true;
            else
                docCreator.errorCheckAgainFlag = false;

            keyListener.Start();
            while (true)
            {
                Thread.Sleep(300);
                iface.Refresh(docCreator.index, docCreator.limit, docCreator.currentDocument, action);

                // NIE CHCE ZEBY PRACOWAL I NIE PRACUJE // -- WYŁĄCZONY
                if (!docCreator.state && !docCreator.running)
                {
                    if(state == States.START_WITH_ERROR_BYPASS)
                    {
                        SetState(iface, States.START_WITH_ERROR_BYPASS);
                        // URUCHOMIENIE //
                        if (action == Actions.START && KeyPressed(ActionKey))
                        {
                            RunCreator(docCreator);
                        }
                        else if (action == Actions.SET_CHECK_AGAIN_OLD_ERRORS && KeyPressed(ActionKey))
                        {
                            SetRunAgainError(docCreator, true);
                        }
                        else if (action == Actions.EXIT && KeyPressed(ActionKey))
                        {
                            break;
                        }
                    }
                    else if (state == States.START_WITH_ERROR_CHECK)
                    {
                        SetState(iface, States.START_WITH_ERROR_CHECK);
                        // URUCHOMIENIE //
                        if (action == Actions.START && KeyPressed(ActionKey))
                        {
                            RunCreator(docCreator);
                        }
                        else if (action == Actions.SET_BYPASS_OLD_ERRORS && KeyPressed(ActionKey))
                        {
                            SetRunAgainError(docCreator, false);
                        }
                        else if (action == Actions.EXIT && KeyPressed(ActionKey))
                        {
                            break;
                        }
                    }
                    else
                    {
                        SetState(iface, States.STOPPED);
                        // URUCHOMIENIE //
                        if (action == Actions.CONTINUE && KeyPressed(ActionKey))
                        {
                            RunCreator(docCreator);
                        }
                        else if (action == Actions.EXIT && KeyPressed(ActionKey))
                        {
                            break;
                        }
                    }
                }
                // CHCE ZEBY PRACOWAŁ I PRACUJE // RUNNING - PRACA AKTYWNA
                else if (docCreator.state && docCreator.running)
                {
                    SetState(iface, States.RUNNING);
                    // WSTRZYMANIE //
                    if (action == Actions.STOP && KeyPressed(ActionKey))
                    {
                        PauseCreator(docCreator);
                    }
                }
                // NIE CHCE ZEBY PRACOWAL ALE PRACUJE // STOPPING - KONCZY DOKUMENT, WSTRZYMUJE PRACE
                else if (!docCreator.state && docCreator.running)
                {
                    SetState(iface, States.STOPPING);
                }
                // CHCE ZEBY PRACOWAL ALE NIE PRACUJE // COMPLETED - ZAKONCZYŁ PRACE
                else if (docCreator.state && !docCreator.running)
                {
                    // ZAKONCZYŁ PRACĘ POMYŚLNIE //
                    if(!docCreator.error)
                    {
                        SetState(iface, States.COMPLETED);
                        if (action == Actions.OPEN_LOG_FILE && KeyPressed(ActionKey))
                        {
                            OpenLogFile(logFile);
                        }
                        else if (action == Actions.EXIT && KeyPressed(ActionKey))
                        {
                            break;
                        }
                    }
                    // ZAKOŃCZYŁ PRACĘ Z BŁĘDEM //
                    else
                    {
                        SetState(iface, States.ERROR);
                        if (action == Actions.OPEN_LOG_FILE && KeyPressed(ActionKey))
                        {
                            OpenLogFile(logFile);
                        }
                        else if (action == Actions.EXIT && KeyPressed(ActionKey))
                        {
                            break;
                        }
                    } 
                }

                if (KeyPressed(ConsoleKey.UpArrow))
                {
                    PrevAction();
                }
                else if (KeyPressed(ConsoleKey.DownArrow))
                {
                    NextAction();
                }

                Console.WriteLine();
                Console.WriteLine("State: " + docCreator.state);
                Console.WriteLine("Running: " + docCreator.running);
                Console.WriteLine("Error: " + docCreator.error);
                Console.WriteLine("RunAgain: " + docCreator.errorCheckAgainFlag);
                Console.WriteLine("ACTION: " + action.ToString());
                Console.WriteLine("STATE: " + state.ToString());
                
            }
            iface.Dispose();
            keyListener.Abort();
            docCreator.Dispose();
        }

        private void ReadKey()
        {
            while (true)
            {
                selectedKey = Console.ReadKey().Key;
                keyActionCompleted = false;
            }
        }

        private bool KeyPressed(ConsoleKey key)
        {
            if (!keyActionCompleted && selectedKey == key)
            {
                keyActionCompleted = true;
                return true;
            }
            else
            {
                return false;
            }
        }

        private void NextAction()
        {
            do
            {
                action = action.Next();
            }
            while (!Extensions.ActionAllowedForState(action, state));
        }

        private void PrevAction()
        {
            do
            {
                action = action.Prev();
            }
            while (!Extensions.ActionAllowedForState(action, state));
        }

        private void SetState(Interface iface, States state)
        {
            if(!Extensions.ActionAllowedForState(action, state))
            {
                action = Extensions.GetActionsPerState(state)[0];
            }

            this.state = state;
            iface.SetState(state);
        }

        private void OpenLogFile(LogFile logFile)
        {
            System.Diagnostics.Process.Start(logFile.path);
        }

        private void RunCreator(DocCreator docCreator)
        {
            docCreator.state = true;

            new Thread(new ThreadStart(docCreator.Run)).Start();
        }

        private void PauseCreator(DocCreator docCreator)
        {
            docCreator.state = false;
        }
        
        private void SetRunAgainError(DocCreator docCreator, bool b)
        {
            docCreator.errorCheckAgainFlag = b;
            if (state == States.START_WITH_ERROR_BYPASS)
                state = States.START_WITH_ERROR_CHECK;
            else if (state == States.START_WITH_ERROR_CHECK)
                state = States.START_WITH_ERROR_BYPASS;
        }
    }
}
