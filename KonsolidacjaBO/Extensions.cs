using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProfessoftBaseFusion
{

    public enum States
    {
        START_WITH_ERROR_CHECK,
        START_WITH_ERROR_BYPASS,
        STOPPED,
        RUNNING,
        STOPPING,
        COMPLETED,
        ERROR
    }

    public enum Actions
    {
        START,
        CONTINUE,
        STOP,
        SET_CHECK_AGAIN_OLD_ERRORS,
        SET_BYPASS_OLD_ERRORS,
        OPEN_LOG_FILE,
        EXIT,
        WAITING,
    }

    public enum SQLDocumentStates
    {
        READY = 0,
        WORKING_WITH = 1,
        COMPLETED = 2,
        DROPPED = 3,
    }

    public static class Extensions
    {
        public static string SQLColumnNameCompany = "Firma";
        public static string SQLColumnNameWarehouse = "MagazynD";
        public static string SQLColumnNameGood = "TowarD";
        public static string SQLColumnNameDocumentType = "TypDokumentu";
        public static string SQLColumnNameDocument = "Dokument";
        public static string SQLColumnNameDate = "Data";
        public static string SQLColumnNameAmount = "Ilosc";
        public static string SQLColumnNameValue = "Wartosc";
        public static string SQLColumnNameStatus = "Stan";
        public static string SQLColumnNameContractor = "KontrahentD";

        public static T Next<T>(this T src) where T : struct
        {
            if (!typeof(T).IsEnum)
                throw new ArgumentException(String.Format("Argument {0} is not an Enum", typeof(T).FullName));

            T[] Arr = (T[])Enum.GetValues(src.GetType());
            int j = Array.IndexOf<T>(Arr, src) + 1;
            return (Arr.Length == j) ? Arr[0] : Arr[j];
        }

        public static T Prev<T>(this T src) where T : struct
        {
            if (!typeof(T).IsEnum)
                throw new ArgumentException(String.Format("Argument {0} is not an Enum", typeof(T).FullName));

            T[] Arr = (T[])Enum.GetValues(src.GetType());
            int j = Array.IndexOf<T>(Arr, src) - 1;
            return (j < 0) ? Arr[Arr.Length - 1] : Arr[j];
        }

        public static Actions[] GetActionsPerState(States state)
        {
            switch (state)
            {
                case States.STOPPED:
                    return new Actions[] { Actions.CONTINUE, Actions.EXIT };
                case States.RUNNING:
                    return new Actions[] { Actions.STOP };
                case States.STOPPING:
                    return new Actions[] { Actions.WAITING };
                case States.START_WITH_ERROR_BYPASS:
                    return new Actions[] { Actions.START, Actions.SET_CHECK_AGAIN_OLD_ERRORS, Actions.EXIT };
                case States.START_WITH_ERROR_CHECK:
                    return new Actions[] { Actions.START, Actions.SET_BYPASS_OLD_ERRORS, Actions.EXIT };
                default:
                    return new Actions[] { Actions.OPEN_LOG_FILE, Actions.EXIT };
            }

        }

        public static bool ActionAllowedForState(Actions selectedAction, States state)
        {
            Actions[] actions = Extensions.GetActionsPerState(state);
            bool isAllowed = false;

            foreach(Actions action in actions)
            {
                if(action == selectedAction)
                {
                    isAllowed = true;
                    break;
                }
            }

            return isAllowed;
        }


    }
}
