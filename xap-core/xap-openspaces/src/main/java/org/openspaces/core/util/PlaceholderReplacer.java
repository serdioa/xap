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

package org.openspaces.core.util;

import org.springframework.util.PropertyPlaceholderHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * @author Dan Kilman
 */
public class PlaceholderReplacer {

    /**
     * Replaces each occurrence of ${SOME_VALUE} or $(SOME_VALUE) with the variable SOME_VALUE in 'variables' If any
     * error occurs during parsing, i.e: variable doesn't exist, bad syntax, etc... a {@link
     * PlaceholderResolutionException} is thrown, otherwise the new string after all replacements
     * have been made will be returned.
     *
     * @param variables The variables map to match placeholders against
     * @param value     the string, possibly containing placeholders
     * @return the value after placeholder replacement have been made
     * @throws PlaceholderResolutionException if an empty placeholder is found or a placeholder
     *                                        with no suitable value in 'variables'
     */
    public static String replacePlaceholders(final Map<String, String> variables, final String value) throws PlaceholderResolutionException {
        ArrayList<PropertyPlaceholderHelper> helpers = new ArrayList<>(
                Arrays.asList(
                        new PropertyPlaceholderHelper("${", "}"),
                        new PropertyPlaceholderHelper("$(", ")")));

        String result = value;
        for (PropertyPlaceholderHelper helper : helpers) {
            final String str = result;
            result = helper.replacePlaceholders(str, key -> {
                if (key.isEmpty()) {
                    throw new PlaceholderResolutionException("Placeholder in '" + str + "' has to have a length of at least 1");
                }

                String val = variables.get(key);
                if (val == null) {
                    throw new PlaceholderResolutionException("Missing value for placeholder: '" + key + "' in '" + str + "'");
                }

                return val;
            });
        }
        return result;
    }

    // this class extends RuntimeException so it can be thrown from PlaceholderResolver#resolvePlaceholder as done above
    public static class PlaceholderResolutionException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public PlaceholderResolutionException(String message) {
            super(message);
        }

    }

}
