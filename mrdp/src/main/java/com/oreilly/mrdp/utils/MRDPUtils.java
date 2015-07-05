package com.oreilly.mrdp.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

public class MRDPUtils {

  public static final String[] REDIS_INSTANCES = { "p0", "p1", "p2", "p3",
      "p4", "p6" };
  private static DocumentBuilder bldr;

  static {
    try {
      // Create the XML document builder to parse the XML string
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      bldr = factory.newDocumentBuilder();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This helper function parses the StackOverflow record, storing the attribute
   * names and values into a Map
   * 
   * Returns an empty map if a parsing exception occurred
   * 
   * @param xml
   *          The XML to parse
   * @return The map containing the attribute names and values, or an empty map
   *         if a parsing exception occurred
   */
  public static Map<String, String> transformXmlToMap(String xml) {

    Map<String, String> map = new HashMap<String, String>();

    Document doc;
    try {
      doc = bldr.parse(new ByteArrayInputStream(xml.getBytes()));
    } catch (SAXException | IOException e) {
      // return the (empty) map on exception
      return map;
    }

    // Get the attribute map from the document element
    NamedNodeMap attributes = doc.getDocumentElement().getAttributes();

    // For each attribute
    for (int i = 0; i < attributes.getLength(); ++i) {

      // Get the node and validate it is an attribute node
      Node node = attributes.item(i);
      if (node.getNodeType() == Node.ATTRIBUTE_NODE) {
        // Add the attribute name and value to the map
        map.put(node.getNodeName(), node.getNodeValue());
      }
    }
    return map;
  }
}
