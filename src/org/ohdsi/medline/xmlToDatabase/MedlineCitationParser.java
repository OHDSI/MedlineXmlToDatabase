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
package org.ohdsi.medline.xmlToDatabase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ohdsi.databases.DbType;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.collections.OneToManySet;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MedlineCitationParser {

	private OneToManySet<String, String>	tables2Fields	= new OneToManySet<String, String>();
	private String							pmid;
	private String							pmid_version;
	private Connection						connection;
	private DbType							dbType;

	public MedlineCitationParser(Connection connection, String schema, DbType dbType) {
		this.connection = connection;
		this.dbType = dbType;
		try {
			Set<String> tables = new HashSet<String>();
			Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			if (dbType.equals(DbType.MSSQL))
				statement.execute("SHOW TABLES;");
			else if (dbType.equals(DbType.MSSQL))
				statement.execute("SELECT name FROM " + schema + ".sys.tables ");
			if (dbType.equals(DbType.POSTGRESQL))
				statement.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema.toLowerCase() + "'");
			ResultSet resultSet = statement.getResultSet();
			while (resultSet.next()) {
				String name = Abbreviator.unAbbreviate(resultSet.getString(1));
				if (name.toLowerCase().startsWith("medlinecitation"))
					tables.add(name);
			}

			for (String table : tables) {
				if (dbType.equals(DbType.MSSQL))
					statement.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + Abbreviator.abbreviate(table) + "';");
				if (dbType.equals(DbType.POSTGRESQL))
					statement.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + Abbreviator.abbreviate(table) + "';");
				resultSet = statement.getResultSet();
				while (resultSet.next()) {
					String field = resultSet.getString(1);
					tables2Fields.put(table, field);
				}
			}
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void parseAndInjectIntoDB(Node citation) {
		findPmidAndVersion(citation);

		List<String> sqls = new ArrayList<String>();
		deleteAllForPmidAndVersion(sqls);
		Map<String, String> keys = new HashMap<String, String>();
		keys.put("PMID", pmid);
		keys.put("PMID_Version", pmid_version);
		parseNode(citation, "", "MedlineCitation", new HashMap<String, String>(), true, keys, sqls);

		execute(sqls);
	}

	private void execute(List<String> sqls) {
		try {
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();
			for (String sql : sqls)
				statement.addBatch(sql);
			statement.executeBatch();
			statement.close();
			connection.setAutoCommit(true);
		} catch (SQLException e) {
			System.err.println(StringUtilities.join(sqls, "\n"));
			e.printStackTrace();
		}
	}

	/**
	 * Record could be an update of a previous entry. Just in case, all previous data must be removed
	 * 
	 * @param sqls
	 */
	private void deleteAllForPmidAndVersion(List<String> sqls) {
		for (String table : tables2Fields.keySet()) {
			String sql = "DELETE FROM " + Abbreviator.abbreviate(table) + " WHERE pmid = " + pmid + " AND pmid_version = " + pmid_version;
			sqls.add(sql);
		}

	}

	private void insertIntoDB(String table, Map<String, String> field2Value, List<String> sqls) {
		removeFieldsNotInDb(table, field2Value);
		StringBuilder sql = new StringBuilder();
		sql.append("INSERT INTO ");
		sql.append(Abbreviator.abbreviate(table));
		sql.append(" (");
		boolean first = true;
		for (String field : field2Value.keySet()) {
			if (first)
				first = false;
			else
				sql.append(",");
			sql.append(Abbreviator.abbreviate(field));
		}

		if (dbType.equals(DbType.MYSQL)) { // MySQL uses double quotes, escape using backslash
			sql.append(") VALUES (\"");
			first = true;
			for (String field : field2Value.keySet()) {
				if (first)
					first = false;
				else
					sql.append("\",\"");
				sql.append(field2Value.get(field).replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\""));
			}
			sql.append("\");");
		} else if (dbType.equals(DbType.MYSQL) || dbType.equals(DbType.POSTGRESQL) ) { // MSSQL uses single quotes, escape by doubling
			sql.append(") VALUES ('");
			first = true;
			for (String field : field2Value.keySet()) {
				if (first)
					first = false;
				else
					sql.append("','");
				sql.append(field2Value.get(field).replaceAll("'", "''"));
			}
			sql.append("');");
		}
		sqls.add(sql.toString());
	}

	private void removeFieldsNotInDb(String table, Map<String, String> field2Value) {
		Set<String> fieldsInDb = tables2Fields.get(table.toLowerCase());
		Iterator<Map.Entry<String, String>> iterator = field2Value.entrySet().iterator();
		while (iterator.hasNext()) {
			String field = iterator.next().getKey();
			if (!fieldsInDb.contains(Abbreviator.abbreviate(field.toLowerCase()))) {
				System.err.println("Ignoring field " + field + " in table " + table
						+ " because field is not in DB (meaning it wasn't encountered in the XML files before now)");
				iterator.remove();
			}
		}
	}

	private boolean findPmidAndVersion(Node node) {
		NodeList children = node.getChildNodes();
		for (int j = 0; j < children.getLength(); j++) {
			Node child = children.item(j);
			if (child.getNodeName().equals("PMID")) {
				pmid = child.getFirstChild().getNodeValue();
				pmid_version = child.getAttributes().getNamedItem("Version").getNodeValue();
				return true;
			}
			if (findPmidAndVersion(child))
				return true;
		}
		return false;
	}

	private void parseNode(Node node, String name, String tableName, HashMap<String, String> field2Value, boolean tableRoot, Map<String, String> keys,
			List<String> sqls) {
		// Add this value:
		if (node.getNodeValue() != null && node.getNodeValue().trim().length() != 0)
			field2Value.put(name.length() == 0 ? "Value" : name, node.getNodeValue());

		// Add attributes:
		NamedNodeMap attributes = node.getAttributes();
		if (attributes != null)
			for (int i = 0; i < attributes.getLength(); i++) {
				Node attribute = attributes.item(i);
				String attributeName = concatenate(name, attribute.getNodeName());
				field2Value.put(attributeName, attribute.getNodeValue());
			}

		// Add children
		NodeList children = node.getChildNodes();
		int subCount = 1;
		for (int i = 0; i < children.getLength(); i++) {
			Node child = children.item(i);
			String childName = name;
			if (!child.getNodeName().equals("#text")) {
				childName = concatenate(childName, child.getNodeName());
			}
			String potentialNewTableName = concatenate(tableName, childName);
			if (tables2Fields.keySet().contains(potentialNewTableName.toLowerCase())) {// Its a sub table
				Map<String, String> newKeys = new HashMap<String, String>(keys);
				newKeys.put(potentialNewTableName + "_Order", Integer.toString(subCount++));
				parseNode(child, "", potentialNewTableName, new HashMap<String, String>(), true, newKeys, sqls);
			} else {
				parseNode(child, childName, tableName, field2Value, false, keys, sqls);
			}
		}

		if (tableRoot) { // Bottom level completed: write values to database
			field2Value.putAll(keys);
			insertIntoDB(tableName, field2Value, sqls);
		}
	}

	private String concatenate(String pre, String post) {
		if (pre.length() != 0)
			return pre + "_" + post;
		else
			return post;
	}

	public void delete(Node node) {
		List<String> sqls = new ArrayList<String>();
		NodeList children = node.getChildNodes();
		for (int j = 0; j < children.getLength(); j++) {
			Node child = children.item(j);
			if (child.getNodeName().equals("PMID")) {
				pmid = child.getFirstChild().getNodeValue();
				pmid_version = child.getAttributes().getNamedItem("Version").getNodeValue();
				deleteAllForPmidAndVersion(sqls);
			}
		}
		execute(sqls);
	}
}
