package cj;

/**
 * Created by cj on 3/3/16.
 */
public enum CommandCodes {
    LOOKUP,
    STORE,
    JOIN,
    UPDATESUCCESSOR,
    TRANSFER,
    LIST,
    INTERNALLOOKUP,
    INTERNALLOOKUPRESPONSE,
    ALIVE,
    UPDATENODEID,
    CLEAR,
    LOOKUPRESPONSE,
    NODELOOKUP,
    NODELOOKUPRESPONSE,
    NOTFOUND;

    public static CommandCodes intToCommand(int i) {
        switch(i) {
            case 0: return LOOKUP;
            case 1: return STORE;
            case 2: return JOIN;
            case 3: return UPDATESUCCESSOR;
            case 4: return TRANSFER;
            case 5: return LIST;
            case 6: return INTERNALLOOKUP;
            case 7: return INTERNALLOOKUPRESPONSE;
            case 8: return ALIVE;
            case 9: return UPDATENODEID;
            case 10: return CLEAR;
            case 11: return LOOKUPRESPONSE;
            case 12: return NODELOOKUP;
            case 13: return NODELOOKUPRESPONSE;
            default: return NOTFOUND;
        }
    }
}
