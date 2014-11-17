/*******************************************************************************
 * Copyright 2014 Observational Health Data Sciences and Informatics
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.medline.xmlToDatabase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ohdsi.databases.DBConnector;
import org.ohdsi.databases.DbType;
import org.ohdsi.medline.xmlToDatabase.MedlineCitationAnalyser.VariableType;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.Row;

/**
 * Wrapper around java.sql.connection to handle any database work that is platform-specific.
 * 
 * @author MSCHUEMI
 * 
 */
public class ConnectionWrapper {
	private Connection	connection;
	private DbType		dbType;
	private boolean		batchMode	= false;
	private Statement	statement;

	public ConnectionWrapper(String server, String domain, String user, String password, DbType dbType) {
		this.connection = DBConnector.connect(server, domain, user, password, dbType);
		this.dbType = dbType;
	}

	public void setBatchMode(boolean batchMode) {
		try {
			if (this.batchMode && !batchMode) { // turn off batchmode
				this.batchMode = false;
				statement.executeBatch();
				connection.setAutoCommit(true);
			} else {
				this.batchMode = true;
				connection.setAutoCommit(false);
				statement = connection.createStatement();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Switch the database to use.
	 * 
	 * @param database
	 */
	public void use(String database) {
		if (database == null)
			return;
		if (dbType.equals(DbType.ORACLE))
			execute("ALTER SESSION SET current_schema = " + database);
		else if (dbType.equals(DbType.POSTGRESQL))
			execute("SET search_path TO " + database);
		else
			execute("USE " + database);
	}

	/**
	 * Execute the given SQL statement.
	 * 
	 * @param sql
	 */
	public void execute(String sql) {
		try {
			if (sql.length() == 0)
				return;
			if (batchMode)
				statement.addBatch(sql);
			else {
				Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				statement.execute(sql);
				statement.close();
			}
		} catch (SQLException e) {
			System.err.println(sql);
			e.printStackTrace();
			e = e.getNextException();
			if (e != null) {
				System.err.println("Error: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	public void insertIntoTable(String table, Map<String, String> field2Value) {
		List<String> fields = new ArrayList<String>(field2Value.keySet());

		StringBuilder sql = new StringBuilder();
		sql.append("INSERT INTO ");
		sql.append(Abbreviator.abbreviate(table));
		sql.append(" (");
		boolean first = true;
		for (String field : fields) {
			if (first)
				first = false;
			else
				sql.append(",");
			sql.append(Abbreviator.abbreviate(field));
		}

		if (dbType.equals(DbType.MYSQL)) { // MySQL uses double quotes, escape using backslash
			sql.append(") VALUES (\"");
			first = true;
			for (String field : fields) {
				if (first)
					first = false;
				else
					sql.append("\",\"");
				sql.append(field2Value.get(field).replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\""));
			}
			sql.append("\");");
		} else if (dbType.equals(DbType.MSSQL) || dbType.equals(DbType.POSTGRESQL)) { // MSSQL uses single quotes, escape by doubling
			sql.append(") VALUES ('");
			first = true;
			for (String field : fields) {
				if (first)
					first = false;
				else
					sql.append("','");
				sql.append(field2Value.get(field).replaceAll("'", "''"));
			}
			sql.append("')");
		}
		execute(sql.toString());
	}

	public void createTable(String table, List<String> fields, List<String> types, List<String> primaryKey) {
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE " + table + " (\n");
		for (int i = 0; i < fields.size(); i++) {
			sql.append("  " + fields.get(i) + " " + types.get(i) + ",\n");
		}
		sql.append("  PRIMARY KEY (" + StringUtilities.join(primaryKey, ",") + ")\n");
		sql.append(");\n\n");
		execute(Abbreviator.abbreviate(sql.toString()));
	}

	public void createTableUsingVariableTypes(String table, List<String> fields, List<VariableType> variableTypes, List<String> primaryKey) {
		List<String> types = new ArrayList<String>(variableTypes.size());
		for (VariableType variableType : variableTypes) {
			if (dbType.equals(DbType.MYSQL)) {
				if (variableType.isNumeric)
					types.add("INT");
				else if (variableType.maxLength > 255)
					types.add("TEXT");
				else
					types.add("VARCHAR(255)");
			} else if (dbType.equals(DbType.MSSQL)) {
				if (variableType.isNumeric) {
					if (variableType.maxLength < 10)
						types.add("INT");
					else
						types.add("BIGINT");
				} else if (variableType.maxLength > 255)
					types.add("VARCHAR(MAX)");
				else
					types.add("VARCHAR(255)");
			} else if (dbType.equals(DbType.POSTGRESQL)) {
				if (variableType.isNumeric) {
					if (variableType.maxLength < 10)
						types.add("INT");
					else
						types.add("BIGINT");
				} else if (variableType.maxLength > 255)
					types.add("TEXT");
				else
					types.add("VARCHAR(255)");
			} else
				throw new RuntimeException("Unknown datasource type " + dbType);
		}

		createTable(table, fields, types, primaryKey);
	}

	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public class QueryResult implements Iterable<Row> {
		private String				sql;

		private List<DBRowIterator>	iterators	= new ArrayList<DBRowIterator>();

		public QueryResult(String sql) {
			this.sql = sql;
		}

		@Override
		public Iterator<Row> iterator() {
			DBRowIterator iterator = new DBRowIterator(sql);
			iterators.add(iterator);
			return iterator;
		}

		public void close() {
			for (DBRowIterator iterator : iterators) {
				iterator.close();
			}
		}
	}

	public List<String> getTableNames(String database) {
		List<String> names = new ArrayList<String>();
		String query = null;
		if (dbType.equals(DbType.MYSQL)) {
			if (database == null)
				query = "SHOW TABLES";
			else
				query = "SHOW TABLES IN " + database;
		} else if (dbType.equals(DbType.MSSQL)) {
			query = "SELECT name FROM " + database + ".sys.tables ";
		} else if (dbType.equals(DbType.ORACLE)) {
			query = "SELECT table_name FROM all_tables WHERE owner='" + database.toUpperCase() + "'";
		} else if (dbType.equals(DbType.POSTGRESQL)) {
			query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + database + "'";
		}

		for (Row row : query(query))
			names.add(row.get(row.getFieldNames().get(0)));
		return names;
	}

	public List<String> getFieldNames(String table) {
		List<String> names = new ArrayList<String>();
		if (dbType.equals(DbType.MSSQL)) {
			for (Row row : query("SELECT name FROM syscolumns WHERE id=OBJECT_ID('" + table + "')"))
				names.add(row.get("name"));
		} else if (dbType.equals(DbType.MYSQL))
			for (Row row : query("SHOW COLUMNS FROM " + table))
				names.add(row.get("COLUMN_NAME"));
		else if (dbType.equals(DbType.POSTGRESQL))
			for (Row row : query("SELECT column_name FROM information_schema.columns WHERE table_name='" + table.toLowerCase() + "'"))
				names.add(row.get("column_name"));
		else
			throw new RuntimeException("DB type not supported");

		return names;
	}

	private QueryResult query(String sql) {
		return new QueryResult(sql);
	}

	private class DBRowIterator implements Iterator<Row> {

		private ResultSet	resultSet;

		private boolean		hasNext;

		private Set<String>	columnNames	= new HashSet<String>();

		public DBRowIterator(String sql) {
			try {
				sql.trim();
				if (sql.endsWith(";"))
					sql = sql.substring(0, sql.length() - 1);
				Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				resultSet = statement.executeQuery(sql.toString());
				hasNext = resultSet.next();
			} catch (SQLException e) {
				System.err.println(sql.toString());
				System.err.println(e.getMessage());
				throw new RuntimeException(e);
			}
		}

		public void close() {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				resultSet = null;
				hasNext = false;
			}
		}

		@Override
		public boolean hasNext() {
			return hasNext;
		}

		@Override
		public Row next() {
			try {
				Row row = new Row();
				ResultSetMetaData metaData;
				metaData = resultSet.getMetaData();
				columnNames.clear();

				for (int i = 1; i < metaData.getColumnCount() + 1; i++) {
					String columnName = metaData.getColumnName(i);
					if (columnNames.add(columnName)) {
						String value = resultSet.getString(i);
						if (value == null)
							value = "";

						row.add(columnName, value.replace(" 00:00:00", ""));
					}
				}
				hasNext = resultSet.next();
				if (!hasNext) {
					resultSet.close();
					resultSet = null;
				}
				return row;
			} catch (SQLException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		@Override
		public void remove() {
		}
	}

	public void setDateFormat() {
		try {
			Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			if (dbType.equals(DbType.POSTGRESQL))
				statement.execute("SET datestyle = \"ISO, MDY\"");
			else
				statement.execute("SET DateFormat MDY;");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
