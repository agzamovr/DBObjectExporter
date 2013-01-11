package com.bas.export;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.xml.sax.InputSource;

public class GenerateDDL extends Task {
	private Connection conn;
	private final String ddlSql;
	private final String synonymsSql;
	private final String aleterDdlSql;
	private final String[] dbObjects = { "SEQUENCE", "TABLE", "INDEX", "VIEW",
			"MATERIALIZED_VIEW", "PROCEDURE", "FUNCTION", "TRIGGER", "PACKAGE" };
	private final String lineSep = System.getProperty("line.separator");
	private static final Map<String, String> sqlList = new HashMap<String, String>();
	private String ddlFileEncoding = "Cp1251";
	private String projectCode;
	private String dbLink;
	private String outDir;
	private boolean isSingleOutput;
	private String jdbcURL;
	private String dbUser;
	private String dbPassword;
	private String action;

	@Override
	public void execute() throws BuildException {
		try {
			if ("downDB".equalsIgnoreCase(action))
				genProjectDdl();
			else if ("db.diff".equalsIgnoreCase(action))
				genDbDiff();
		} catch (Exception e) {
			throw new BuildException(e);
		}
	}

	public GenerateDDL() throws IOException {
		ddlSql = getSql("get_ddl");
		aleterDdlSql = getSql("alter_ddl");
		synonymsSql = getSql("get_synonym");
	}

	private Set<String> getDependantProjectCodes() {
		String depProj = getProject().getProperty(projectCode + ".db.depends");
		Set<String> projectCodes = new HashSet<String>();
		projectCodes.add(projectCode);
		if (depProj != null && !depProj.isEmpty()) {
			projectCodes.addAll(Arrays.asList(depProj.split(",")));
		}
		return projectCodes;
	}

	private void genDbDiff() throws IOException, SQLException,
			ClassNotFoundException {
		String rootDefaultschema = getProject().getProperty(
				projectCode + ".default.schema");
		File out = new File(outDir);
		if (!out.isDirectory())
			out.mkdir();
		Set<String> projectCodes = getDependantProjectCodes();
		for (String proj : projectCodes) {
			out = new File(outDir + File.separator + proj);
			if (!out.isDirectory())
				out.mkdir();
			String defaultschema = getProject().getProperty(
					proj + ".default.schema");
			if (defaultschema == null || defaultschema.isEmpty())
				defaultschema = rootDefaultschema;

			String schema = getProject().getProperty(proj + ".table.schema");
			if (schema == null)
				schema = defaultschema;
			if (schema == null)
				return;
			String nList = getProject().getProperty(proj + ".table.name");
			if (nList == null)
				return;
			nList = nList.toUpperCase();
			schema = schema.toUpperCase();
			Set<String> names = new HashSet<String>(Arrays.asList(nList
					.split(",")));
			try {
				for (String name : names) {
					File f = new File(out.getPath() + File.separator + name
							+ ".alter.sql");
					Writer sw = new OutputStreamWriter(new FileOutputStream(f),
							ddlFileEncoding);
					boolean hasAlter = false;
					try {
						hasAlter = genDiffDdl(getConn(), sw, "TABLE", name,
								schema);
					} finally {
						sw.close();
					}
					if (!hasAlter)
						f.delete();
				}
			} finally {
				if (getConn() != null)
					getConn().close();
			}
		}
	}

	private void genProjectDdl() throws SQLException, IOException,
			ClassNotFoundException {
		String rootDefaultschema = getProject().getProperty(
				projectCode + ".default.schema");
		Set<String> projectCodes = getDependantProjectCodes();
		File out = new File(outDir);
		if (!out.isDirectory())
			out.mkdir();

		try {
			for (String proj : projectCodes) {
				for (String obj : dbObjects) {
					String defaultschema = getProject().getProperty(
							proj + ".default.schema");
					if (defaultschema == null || defaultschema.isEmpty())
						defaultschema = rootDefaultschema;
					String schema = getProject().getProperty(
							proj + "." + obj.toLowerCase() + ".schema");
					if (schema == null)
						schema = defaultschema;
					if (schema == null)
						continue;
					String nList = getProject().getProperty(
							proj + "." + obj.toLowerCase() + ".name");
					if (nList == null)
						continue;
					nList = nList.toUpperCase();
					schema = schema.toUpperCase();
					Set<String> names = new HashSet<String>(Arrays.asList(nList
							.split(",")));
					for (String name : names) {
						String ext = ".sql";
						if ("PACKAGE".equalsIgnoreCase(obj))
							ext = ".pck";
						else if ("TRIGGER".equalsIgnoreCase(obj))
							ext = ".trg";
						else if ("FUNCTION".equalsIgnoreCase(obj))
							ext = ".fnc";
						else if ("PROCEDURE".equalsIgnoreCase(obj))
							ext = ".prc";
						File f = new File(out.getPath() + File.separator + name
								+ ext);
						Writer sw = new OutputStreamWriter(
								new FileOutputStream(f), ddlFileEncoding);
						try {
							genObjectDdl(getConn(), sw, obj, name, schema);
							if (!"SYNONYM".equals(obj)) {
								Map<String, String> synonyms = getSynonyms(
										getConn(), name, schema);
								for (Map.Entry<String, String> entry : synonyms
										.entrySet()) {
									genObjectDdl(getConn(), sw, "SYNONYM",
											entry.getKey(), entry.getValue());
								}
							}
						} finally {
							sw.close();
						}
						System.out.println(name + " " + obj + " ddl generated");
					}
				}
			}
		} finally {
			if (getConn() != null)
				getConn().close();
		}
	}

	private void genObjectDdl(Connection dbConnection, Writer w,
			String objectType, String objectName, String schema)
			throws SQLException, IOException {
		if ("PACKAGE".equals(objectType)) {
			genObjectDdl(dbConnection, w, "PACKAGE_SPEC", objectName, schema);
			genObjectDdl(dbConnection, w, "PACKAGE_BODY", objectName, schema);
			return;
		}
		CallableStatement call = dbConnection.prepareCall(ddlSql);
		try {
			call.registerOutParameter(1, Types.CLOB);
			call.setString(2, objectType);
			call.setString(3, objectName);
			call.setString(4, schema);
			call.execute();
			Clob res = call.getClob(1);
			String buff = null;
			BufferedReader br = new BufferedReader(res.getCharacterStream());

			while ((buff = br.readLine()) != null) {
				if (buff.trim().startsWith("ALTER")) {
					w.append(lineSep);
					w.write("/");
					w.append(lineSep);
				}
				w.write(buff);
				w.append(lineSep);
			}
			w.append('/');
			w.append(lineSep);
			w.flush();
		} finally {
			call.close();
		}
	}

	private boolean genDiffDdl(Connection dbConnection, Writer w,
			String objectType, String objectName, String schema)
			throws SQLException, IOException {
		boolean hasAlter = false;
		CallableStatement call = dbConnection.prepareCall(aleterDdlSql);
		try {
			call.registerOutParameter(1, Types.CLOB);
			call.setString(2, objectType);
			call.setString(3, objectName);
			call.setString(4, objectName);
			call.setString(5, schema);
			call.setString(6, schema);
			call.setString(7, dbLink);
			call.execute();
			Clob res = call.getClob(1);
			String buff = null;
			BufferedReader br = new BufferedReader(res.getCharacterStream());
			while ((buff = br.readLine()) != null) {
				hasAlter = true;
				buff = buff.trim();
				w.write(buff);
				w.append(lineSep);
				w.write("/");
				w.append(lineSep);
			}
		} finally {
			call.close();
		}
		return hasAlter;
	}

	private Map<String, String> getSynonyms(Connection dbConnection,
			String objectName, String schema) throws SQLException {
		Map<String, String> res = new HashMap<String, String>();
		PreparedStatement ps = dbConnection.prepareStatement(synonymsSql);
		try {
			ps.setString(1, schema);
			ps.setString(2, objectName);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				res.put(rs.getString(2), rs.getString(1));
			}
			rs.close();
		} finally {
			ps.close();
		}
		return res;
	}

	private String getSql(String sqlName) {
		if (sqlList.containsKey(sqlName))
			return sqlList.get(sqlName);
		else
			return getSqlFromFile(sqlName);
	}

	private String getSqlFromFile(String sqlName) {
		InputStream is = GenerateDDL.class
				.getResourceAsStream("/com/bas/export/ddl.xml");
		XPath xPath = XPathFactory.newInstance().newXPath();
		XPathExpression xPathExpression = null;
		try {
			xPathExpression = xPath.compile(String.format("/sql/%s/text()",
					sqlName));
			InputSource inputSource = new InputSource(is);
			String sql = xPathExpression.evaluate(inputSource);
			sqlList.put(sqlName, sql);
			return sql;
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		}
		return "";
	}

	public String getDbUser() {
		return dbUser;
	}

	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	public String getProjectCode() {
		return projectCode;
	}

	public void setProjectCode(String projectCode) {
		this.projectCode = projectCode;
	}

	public String getDbLink() {
		return dbLink;
	}

	public void setDbLink(String dbLink) {
		this.dbLink = dbLink;
	}

	public String getOutDir() {
		return outDir;
	}

	public void setOutDir(String outDir) {
		this.outDir = outDir;
	}

	public boolean isSingleOutput() {
		return isSingleOutput;
	}

	public void setSingleOutput(String isSingleOutput) {
		this.isSingleOutput = "Y".equalsIgnoreCase(isSingleOutput);
	}

	public void setJdbcURL(String jdbcURL) {
		this.jdbcURL = jdbcURL;
	}

	public String getJdbcURL() {
		return jdbcURL;
	}

	public void setDdlFileEncoding(String ddlFileEncoding) {
		this.ddlFileEncoding = ddlFileEncoding;
	}

	public String getDdlFileEncoding() {
		return ddlFileEncoding;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public Connection getConn() throws SQLException, ClassNotFoundException {
		if (conn != null && !conn.isClosed())
			return conn;
		Class.forName("oracle.jdbc.driver.OracleDriver");
		conn = DriverManager.getConnection(jdbcURL, dbUser, dbPassword);
		return conn;
	}
}
