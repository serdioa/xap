/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        for (ESCAPE_SYMBOLS sign : ESCAPE_SYMBOLS.values()) {
            value = value.replace(String.valueOf(sign.ch()), "\\" + sign.ch());
        }
        value = value.replace(NEW_LINE, "\\n");
        return value;
    }

}
