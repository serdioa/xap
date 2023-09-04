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

//Internal Doc
package com.gigaspaces.security.encoding.aes;

import com.gigaspaces.internal.utils.ByteUtils;
import com.gigaspaces.security.encoding.ContentEncoder;
import com.gigaspaces.security.encoding.EncodingException;
import com.gigaspaces.security.encoding.KeyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.util.Arrays;

/**
 * A {@link ContentEncoder} using AES as the cryptographic algorithm.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class AesContentEncoder extends AesEncrypter implements ContentEncoder {

    private static final Logger logger = LoggerFactory.getLogger(AesContentEncoder.class.getPackage().getName() + "." + AesContentEncoder.class.getName());

    //see FileEncodingManager which instantiates this class by name
    public AesContentEncoder() {
        this(loadSecretKey());
    }

    public AesContentEncoder(SecretKey secretKey) {
        super(secretKey);
    }

    /**
     * Attempts to load a custom secret key. If not found, uses default private secret key.
     *
     * @return a secret key.
     */
    private static SecretKey loadSecretKey() {
        /** The custom private key filename to lookup when constructing a secret key */
        final String PRIVATE_KEY_FILENAME = "gs-private.key";

        SecretKey secretKey = KeyFactory.loadKey(PRIVATE_KEY_FILENAME);
        if (secretKey == null) {
            secretKey = KeyFactory.generateKey(new byte[]{-83, -117, -82, -28, 100, -16, 18, 18, -105, -124, -22,
                    86, 102, -34, 107, 123}, "AES");
        }
        return secretKey;
    }

    /*
     * @see com.gigaspaces.security.encoding.ContentEncoder#decode(byte[])
     */
    public synchronized Object decode(byte[] bytes) throws EncodingException {
        try {
            logger.info("Decode bytes: " + Arrays.toString(bytes));
            byte[] decrypted = dcipher.doFinal(bytes);
            logger.info("Decrypted bytes: " + Arrays.toString(decrypted));
            Object obj = ByteUtils.bytesToObject(decrypted);
            logger.info("Decoded object: " + obj);
            return obj;
        } catch (Exception e) {
            throw new EncodingException("Failed to decode byte array.", e);
        }
    }

    /*
     * @see com.gigaspaces.security.encoding.ContentEncoder#encode(java.lang.Object)
     */
    public synchronized byte[] encode(Object obj) throws EncodingException {
        try {
            logger.info("Object encode: " + obj);
            byte[] objectToBytes = ByteUtils.objectToBytes(obj);
            logger.info("Encoded bytes: " + Arrays.toString(objectToBytes));
            byte[] encrypted = ecipher.doFinal(objectToBytes);
            logger.info("Encrypted bytes: " + Arrays.toString(encrypted));
            return encrypted;
        } catch (Exception e) {
            throw new EncodingException("Failed to encode object.", e);
        }
    }
}
