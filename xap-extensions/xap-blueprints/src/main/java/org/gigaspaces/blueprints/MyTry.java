package org.gigaspaces.blueprints;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;

public class MyTry {

    private static final String VALUES_PATH = "/home/meshir/xap/xap-extensions/xap-blueprints/src/main/java/org/gigaspaces/blueprints/pipeline.json";
    private static final MustacheFactory mf = new DefaultMustacheFactory();


    public static void main(String[] args) throws IOException {

        Mustache m = mf.compile(VALUES_PATH);
        HashMap<String, Object> scopes = new HashMap<>();
        scopes.put("name", "Mustache");
        scopes.put("feture", "lll");
        OutputStream       outputStream       = new FileOutputStream("/home/meshir/xap/xap-extensions/xap-blueprints/src/main/java/org/gigaspaces/blueprints/pipeline2.json");

        Writer writer = new OutputStreamWriter(outputStream);

        Writer execute = m.execute(writer, scopes);
        System.out.println(writer);
        writer.flush();

    }

}
