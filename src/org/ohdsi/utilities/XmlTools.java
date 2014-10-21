/*******************************************************************************
 * Copyright 2014 Observational Health Data Sciences and Informatics
 * 
 * This file is part of MedlineXmlToDatabase
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * @author Observational Health Data Sciences and Informatics
 * @author Martijn Schuemie
 ******************************************************************************/
package org.ohdsi.utilities;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XmlTools {
	public static Node getChildByName(Node node, String name){
		NodeList childNodes = node.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++){
			Node childNode = childNodes.item(i);
			if (childNode.getNodeName().equals(name))
				return childNode;
		}
		return null;
	}

	public static String getChildByNameValue(Node node, String name){
		Node childNode = getChildByName(node, name);
		if (childNode == null)
			return null;
		else 
			return getValue(childNode);
	}

	public static String getValue(Node node){
		String value = node.getNodeValue();
		if (value == null){
			node = getChildByName(node, "#text");
			if (node != null)
				value = node.getNodeValue();
		}
		return value;
	}

	public static List<Node> getChildrenByName(Node node, String name){
		List<Node> result = new ArrayList<Node>();
		NodeList childNodes = node.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); i++){
			Node childNode = childNodes.item(i);
			if (childNode.getNodeName().equals(name))
				result.add(childNode);
		}
		return result;
	}

	public static String getAttributeValue(Node node, String attributeName){
		Node attributeNode = node.getAttributes().getNamedItem(attributeName);
		if (attributeNode == null)
			return null;
		else
			return attributeNode.getNodeValue();		
	}


	public static boolean hasAttributeWithValue(Node node, String attributeName, String attributeValue){
		Node attributeNode = node.getAttributes().getNamedItem(attributeName);
		return (attributeNode != null && attributeNode.getNodeValue().equals(attributeValue));		
	}
}
