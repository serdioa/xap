package com.gigaspaces.logger.cef;

/*
 * * List of character leading by backslash :
 *  \
 *  |
 *  =
 */
public enum ESCAPE_SYMBOLS {
    BLACKSLASH('\\'),
    PIPE('|'),
    EQUAL_SIGN('=');
    private final char ch;

    ESCAPE_SYMBOLS(char ch) {
        this.ch = ch;
    }

    public char ch() {
        return ch;
    }

    public static String quoteSpaces(String value) {
        for (ESCAPE_SYMBOLS sign : ESCAPE_SYMBOLS.values()) {
            value = value.replace(String.valueOf(sign.ch()), "\\" + sign.ch());
        }
        value = value.replace("\n", "\\n");
        return "\"" + value.replace("\"", "\\\"") + "\"";
    }

}
