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
package com.gigaspaces.security.encoding;

import com.gigaspaces.security.encoding.aes.AesContentEncoder;
import com.gigaspaces.security.encoding.md5.Md5PasswordEncoder;

public final class EncodingManager {

    private final PasswordEncoder passwordEncoder;
    private final ContentEncoder contentEncoder;

    /**
     * Constructs the encoders
     */
    public EncodingManager() {
        passwordEncoder = createPasswordEncoder();
        contentEncoder = createContentEncoder();
    }

    /**
     * @return a PasswordEncoder implementation {@link Md5PasswordEncoder}.
     */
    private PasswordEncoder createPasswordEncoder() throws EncodingException {
        try {
            Class<? extends PasswordEncoder> passwordEncoderClass = Class.forName(Md5PasswordEncoder.class.getName())
                    .asSubclass(PasswordEncoder.class);
            return passwordEncoderClass.newInstance();
        } catch (Exception e) {
            throw new EncodingException("Failed to create PasswordEncoder", e);
        }
    }

    /**
     * @return a ContentEncoder implementation {@link AesContentEncoder}
     */
    private ContentEncoder createContentEncoder() throws EncodingException {
        try {
            Class<? extends ContentEncoder> contentEncoderClass = Class.forName(AesContentEncoder.class.getName())
                    .asSubclass(ContentEncoder.class);
            return contentEncoderClass.newInstance();
        } catch (Exception e) {
            throw new EncodingException("Failed to create ContentEncoder", e);
        }
    }

    public PasswordEncoder getPasswordEncoder() {
        return passwordEncoder;
    }

    public ContentEncoder getContentEncoder() {
        return contentEncoder;
    }

}
