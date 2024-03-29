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
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.xml.sax.InputSource;

public class GenerateDDL extends Task {
	private Connection conn;
	private volatile static int threadCount = 0;
	private ExecutorService executor;
	private final BlockingQueue<DBObj> tasks = new LinkedBlockingQueue<DBObj>();
	private final String ddlSql;
	private final String synonymsSql;
	private final String indexesSql;
	private final String aleterDdlSql;
	private final MessageFormat objectExistsSql;
	private final String[] dbObjects = { "SEQUENCE", "TABLE", "VIEW",
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
	private int maxThreadCount = 5;

	public static void main(String[] args) throws IOException, SQLException,
			ClassNotFoundException {
		GenerateDDL gen = new GenerateDDL();
		gen.dbLink = "TEST.SAMRUK.KZ";
		gen.dbPassword = "apps";
		gen.dbUser = "apps";
		gen.jdbcURL = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=192.168.4.52)(PORT=1522))(CONNECT_DATA=(SERVICE_NAME=DEV)))";
		gen.objectExists(gen.getConn(), "", "XXSK", true);
	}

	private class DBObj {
		public final String type;
		public final String schema;
		public final String name;
		public final String ext;
		public final String outDir;

		public DBObj(String outDir, String type, String schema, String name) {
			this.outDir = outDir;
			this.type = type;
			this.schema = schema;
			this.name = name;
			if ("PACKAGE".equalsIgnoreCase(type))
				ext = ".pck";
			else if ("TRIGGER".equalsIgnoreCase(type))
				ext = ".trg";
			else if ("FUNCTION".equalsIgnoreCase(type))
				ext = ".fnc";
			else if ("PROCEDURE".equalsIgnoreCase(type))
				ext = ".prc";
			else
				this.ext = ".sql";
		}
	}

	private class ExecGenDiffDDL implements Callable<DBObj> {
		private Connection conn;

		private Connection getConn() throws SQLException {
			if (conn != null && !conn.isClosed())
				return conn;
			conn = DriverManager.getConnection(jdbcURL, dbUser, dbPassword);
			conn.setAutoCommit(false);
			return conn;
		}

		@Override
		public DBObj call() throws Exception {
			final int currentThreadIdx = ++threadCount;
			DBObj dbObject = tasks.poll();
			if (dbObject == null
					|| !objectExists(
							getConn(),
							dbObject.name,
							dbObject.schema,
							false || !objectExists(getConn(), dbObject.name,
									dbObject.schema, true)))
				return null;
			Writer sw = null;
			try {
				File f = new File(dbObject.outDir + File.separator
						+ dbObject.name + ".alter.sql");
				sw = new OutputStreamWriter(new FileOutputStream(f),
						ddlFileEncoding);
				boolean hasAlter = false;
				try {
					hasAlter = genDiffDdl(getConn(), sw, "TABLE",
							dbObject.name, dbObject.schema);
				} finally {
					sw.close();
				}
				if (!hasAlter)
					f.delete();
				System.out.println(dbObject.name + " "
						+ dbObject.type.toLowerCase()
						+ " ddl generated. Thread number: " + currentThreadIdx);
			} finally {
				try {
					sw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				getConn().close();
			}
			return dbObject;
		}

	};

	private class ExecGenDDL implements Callable<DBObj> {
		private Connection conn;

		private Connection getConn() throws SQLException {
			if (conn != null && !conn.isClosed())
				return conn;
			conn = DriverManager.getConnection(jdbcURL, dbUser, dbPassword);
			conn.setAutoCommit(false);
			return conn;
		}

		@Override
		public DBObj call() throws Exception {
			final int currentThreadIdx = ++threadCount;
			DBObj dbObject = tasks.poll();
			if (dbObject == null
					|| !objectExists(getConn(), dbObject.name, dbObject.schema,
							false))
				return null;
			File f = new File(dbObject.outDir + File.separator + dbObject.name
					+ dbObject.ext);
			Writer sw = null;
			try {
				sw = new OutputStreamWriter(new FileOutputStream(f),
						ddlFileEncoding);

				genObjectDdl(getConn(), sw, dbObject.type, dbObject.name,
						dbObject.schema);
				if ("TABLE".equals(dbObject.type)) {
					Map<String, String> indexes = getTableIndexes(getConn(),
							dbObject.name, dbObject.schema);
					for (Map.Entry<String, String> entry : indexes.entrySet()) {
						genObjectDdl(getConn(), sw, "INDEX", entry.getKey(),
								entry.getValue());
					}
				}
				if (!"SYNONYM".equals(dbObject.type)) {
					Map<String, String> synonyms = getSynonyms(getConn(),
							dbObject.name, dbObject.schema);
					for (Map.Entry<String, String> entry : synonyms.entrySet()) {
						genObjectDdl(getConn(), sw, "SYNONYM", entry.getKey(),
								entry.getValue());
					}
				}
				System.out.println(dbObject.name + " "
						+ dbObject.type.toLowerCase()
						+ " ddl generated. Thread number: " + currentThreadIdx);
			} finally {
				try {
					sw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				getConn().close();
			}
			return dbObject;
		}

	};

	@Override
	public void execute() throws BuildException {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
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
		indexesSql = getSql("get_table_index");
		objectExistsSql = new MessageFormat(getSql("object_exists"));
	}

	private Set<String> getDependantProjectCodes(Set<String> projectCodes,
			String projectCode) {
		String depProj = getProject().getProperty(projectCode + ".db.depends");
		if (projectCodes == null)
			projectCodes = new HashSet<String>();
		projectCodes.add(projectCode);
		if (depProj != null && !depProj.isEmpty()) {
			String[] projects = depProj.split(",");
			for (String proj : projects)
				if (!projectCodes.contains(proj))
					getDependantProjectCodes(projectCodes, proj);
		}
		return projectCodes;
	}

	private void genDbDiff() throws IOException, SQLException,
			ClassNotFoundException, InterruptedException, ExecutionException {
		String rootDefaultschema = getProject().getProperty(
				projectCode + ".default.schema");
		File out = new File(outDir);
		if (!out.isDirectory())
			out.mkdir();
		if (maxThreadCount < 1)
			maxThreadCount = 1;
		executor = Executors.newFixedThreadPool(maxThreadCount);
		Queue<Future<DBObj>> futures = new LinkedList<Future<DBObj>>();
		Set<String> projectCodes = getDependantProjectCodes(null, projectCode);
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
					DBObj db = new DBObj(out.getPath(), "TABLE", schema, name);
					tasks.put(db);
					futures.add(executor.submit(new ExecGenDiffDDL()));
				}
			} finally {
				if (getConn() != null)
					getConn().close();
			}
		}
		while (!futures.isEmpty())
			futures.poll().get();
	}

	private void genProjectDdl() throws SQLException, IOException,
			ClassNotFoundException, InterruptedException, ExecutionException {
		String rootDefaultschema = getProject().getProperty(
				projectCode + ".default.schema");
		Set<String> projectCodes = getDependantProjectCodes(null, projectCode);
		Queue<Future<DBObj>> futures = new LinkedList<Future<DBObj>>();

		File out = new File(outDir);
		if (!out.isDirectory())
			out.mkdir();
		if (maxThreadCount < 1)
			maxThreadCount = 1;
		executor = Executors.newFixedThreadPool(maxThreadCount);
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
					DBObj db = new DBObj(out.getPath(), obj, schema, name);
					tasks.put(db);
					futures.add(executor.submit(new ExecGenDDL()));
				}
			}
		}
		while (!futures.isEmpty())
			futures.poll().get();

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
			BufferedReader br = new BufferedReader(res.getCharacterStream());
			String buff = null;

			while ((buff = br.readLine().trim()).isEmpty()) {
			}

			do {
				if (buff.trim().startsWith("ALTER")) {
					w.append(lineSep);
					w.write("/");
					w.append(lineSep);
				}
				w.write(buff);
				w.append(lineSep);
			} while ((buff = br.readLine()) != null);
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

	private Map<String, String> getTableIndexes(Connection dbConnection,
			String objectName, String schema) throws SQLException {
		Map<String, String> res = new HashMap<String, String>();
		PreparedStatement ps = dbConnection.prepareStatement(indexesSql);
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

	private boolean objectExists(Connection dbConnection, String objectName,
			String schema, boolean forDblink) throws SQLException {
		boolean res = false;
		String sql;
		Object[] param;
		if (forDblink)
			param = new Object[] { "@\"" + dbLink + "\"" };
		else
			param = new Object[] { "" };

		sql = objectExistsSql.format(param);
		PreparedStatement ps = dbConnection.prepareStatement(sql);
		try {
			ps.setString(1, schema);
			ps.setString(2, objectName);
			ResultSet rs = ps.executeQuery();
			res = rs.next();
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
		conn = DriverManager.getConnection(jdbcURL, dbUser, dbPassword);
		conn.setAutoCommit(false);
		return conn;
	}

	public int getMaxThreadCount() {
		return maxThreadCount;
	}

	public void setMaxThreadCount(int maxThreadCount) {
		this.maxThreadCount = maxThreadCount;
	}
}
