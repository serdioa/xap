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
