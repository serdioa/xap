package com.gigaspaces.logger.cef;

/*
 * * List of character leading by backslash :
 *  \
 *  |
 *  =
 *  \n
 */
public enum ESCAPE_SYMBOLS {
    BACKSLASH('\\'),
    PIPE('|'),
    EQUAL_SIGN('=');

    private static String NEW_LINE = "\n";
    private final char ch;

    ESCAPE_SYMBOLS(char ch) {
        this.ch = ch;
    }

    public char ch() {
        return ch;
    }

    public static String encodeSpecialSymbols(String value) {
        if (value == null) {
            return value;
        }
        for (ESCAPE_SYMBOLS sign : ESCAPE_SYMBOLS.values()) {
            value = value.replace(String.valueOf(sign.ch()), "\\" + sign.ch());
        }
        value = value.replace(NEW_LINE, "\\n");
        return value;
    }

}
