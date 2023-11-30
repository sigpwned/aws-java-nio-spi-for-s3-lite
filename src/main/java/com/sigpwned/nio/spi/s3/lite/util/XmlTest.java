package com.sigpwned.nio.spi.s3.lite.util;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;

public class XmlTest {
  public static void main(String[] args) throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    Document doc =
        dbf.newDocumentBuilder()
            .parse(new ByteArrayInputStream(new StringBuilder()
                .append("<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n").append("<foo>\n")
                .append("  <alpha>1</alpha>\n").append("  <bravo>hello</bravo>\n").append("</foo>")
                .toString().getBytes(StandardCharsets.UTF_8)));

    System.out.println(doc.getDocumentElement());

    TransformerFactory tf = TransformerFactory.newInstance();
    Transformer transformer = tf.newTransformer();
    StringWriter writer = new StringWriter();
    transformer.transform(new DOMSource(doc), new StreamResult(writer));
    String output = writer.toString();

    System.out.println(output);
  }
}
