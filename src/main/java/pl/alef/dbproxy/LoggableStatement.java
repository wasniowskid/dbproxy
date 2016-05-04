package pl.alef.dbproxy;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggableStatement implements PreparedStatement {

	private static final Logger LOG = LoggerFactory.getLogger(LoggableStatement.class);

	private static long counterSequence = 0;
	private static long templateSequence = 0;
	private static final String templatePrefix = new SimpleDateFormat("yyMMdd_HHmm_").format(new java.util.Date());

	public final static String PROP_THRESHOLD_WARN = "thresholds.warn";
	public final static String PROP_THRESHOLD_INFO = "thresholds.info";
	public final static String PROP_MAX_VALUE_SIZE = "thresholds.max_value";

	private Properties props;

	private String connType;

	private long counter;

	protected static HashMap<Long, LoggableStatement> loggableStatements = new HashMap<Long, LoggableStatement>(200);
	protected static Map<String, SQLTemplatesInfo> sqlTemplates = new HashMap<String, SQLTemplatesInfo>(1000);

	// <Integer, KVParam>
	protected HashMap<Integer, KVParam> parameterValues;

	// the query string with question marks as
	// parameter placeholders
	private final String sqlTemplate;

	// a statement created from a real database
	// connection
	protected PreparedStatement wrappedStatement;

	private long startTime;
	private int expectedMaxTime = 0;

	private long templateId;

	private String threadName;

	private int getWarnThreshold() {
		final int WARN_THRESHOLD = 10000;
		int value = WARN_THRESHOLD;

		if (props != null) {
			String str = props.getProperty(PROP_THRESHOLD_WARN);

			try {
				value = Integer.parseInt(str);
			} catch (NumberFormatException e) {
				LOG.warn("Unable to parse warn threshold value {}. Setting default {}", str, WARN_THRESHOLD);
			}
		}
		return value;
	}

	private int getMaxValueSize() {
		final int MAX_VALUE_SIZE = 0;
		int value = MAX_VALUE_SIZE;

		if (props != null) {
			String str = props.getProperty(PROP_MAX_VALUE_SIZE);

			try {
				value = Integer.parseInt(str);
			} catch (NumberFormatException e) {
				LOG.warn("Unable to parse max value size {}. Setting default {}", str, MAX_VALUE_SIZE);
			}
		}
		return value;
	}

	private int getInfoThreshold() {
		final int INFO_THRESHOLD = 1000;
		int value = INFO_THRESHOLD;

		if (props != null) {
			String str = props.getProperty(PROP_THRESHOLD_INFO);

			try {
				value = Integer.parseInt(str);
			} catch (NumberFormatException e) {
				LOG.warn("Unable to parse warn threshold value {}. Setting default {}", str, INFO_THRESHOLD);
			}
		}
		return value;
	}

	public int getExpectedMaxTime() {
		return expectedMaxTime;
	}

	public void setExpectedMaxTime(int expectedMaxTime) {
		this.expectedMaxTime = expectedMaxTime;
	}

	protected void logStart() {
		startTime = System.currentTimeMillis();
	}

	protected String getSQLTypeDesc(int sqlType) {
		switch (sqlType) {
		case -7:
			return "BIT";
		case -6:
			return "TINYINT";
		case 5:
			return "SMALLINT";
		case 4:
			return "INTEGER";
		case -5:
			return "BIGINT";
		case 6:
			return "FLOAT";
		case 7:
			return "REAL";
		case 8:
			return "DOUBLE";
		case 2:
			return "NUMERIC";
		case 3:
			return "DECIMAL";
		case 1:
			return "CHAR";
		case 12:
			return "VARCHAR(8000)";
		case -1:
			return "LONGVARCHAR";
		case 91:
			return "DATE";
		case 92:
			return "TIME";
		case 93:
			return "TIMESTAMP";
		case -2:
			return "BINARY";
		case -3:
			return "VARBINARY";
		case -4:
			return "LONGVARBINARY";
		case 1111:
			return "OTHER";
		case 2004:
			return "BLOB";
		case 2005:
			return "CLOB";
		case 16:
			return "BOOLEAN";
		default:
			return "UNKNOWN";
		}
	}

	protected String getSQLScript() {
		ArrayList<String> sqlQuery = new ArrayList<>();

		String sqlTemplateRest = sqlTemplate;
		while (true) {
			int index = sqlTemplateRest.indexOf("?");
			if (index > 0) {
				sqlQuery.add(sqlTemplateRest.substring(0, index));
				sqlTemplateRest = sqlTemplateRest.substring(index + 1);
			} else {
				sqlQuery.add(sqlTemplateRest);
				break;
			}
		}
		String queryBody = sqlQuery.get(0);
		String paramLegend = "";
		String paramLegendTypes = "";

		for (int i = 1; i < sqlQuery.size(); i++) {
			KVParam param = parameterValues.get(i);
			if (param != null) {
				queryBody += "@P" + i + sqlQuery.get(i);
				paramLegend += " SET " + "@P" + i + "=";

				String valueToAdd = "";

				if (param.getValue() != null) {
					switch (param.getSqlType()) {
					case Types.BIT:
					case Types.BOOLEAN:
						valueToAdd = (param.getValue() instanceof Boolean
								? (((Boolean) param.getValue()).equals(true) ? "1" : "0") : "" + param.getValue());
						break;
					case Types.DATE:
					case Types.TIME:
					case Types.TIMESTAMP:
						valueToAdd = (param.getValue() instanceof Date ? "CONVERT(DATETIME, '"
								+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format((Date) param.getValue())
								+ "', 120)" : "" + param.getValue());
						break;
					case Types.VARCHAR:
						valueToAdd = "'" + param.getValue() + "'";
						break;
					default:
						valueToAdd = param.getValue().toString();
					}
				} else {
					valueToAdd = "null";

				}
				int maxSize = getMaxValueSize();

				if (maxSize > 0 && valueToAdd.length() > maxSize) {
					paramLegend += valueToAdd.substring(0, maxSize) + "(...)";
				} else {
					paramLegend += valueToAdd;
				}

				paramLegendTypes += (paramLegendTypes.length() > 0 ? ", " : "") + "@P" + i + " "
						+ getSQLTypeDesc(param.getSqlType());
			} else {
				queryBody += "?" + sqlQuery.get(i);
			}
		}
		return "\n--paramLegendTypes:\nDECLARE " + paramLegendTypes + "\n--paramLegend:\n" + paramLegend + "\n--sql:\n"
				+ queryBody + "\n";
	}

	protected void logEnd() {
		logEnd(null);
	}

	protected void logEnd(SQLException e) {
		if (startTime == 0) {
			return;
		}
		long now = System.currentTimeMillis();
		long allTime = now - startTime;
		try {
			if (LOG.isWarnEnabled() && (e != null || allTime >= getWarnThreshold())) {
				String sqlExceptionStr = null;
				if (e != null) {
					sqlExceptionStr = String.format("SQLException errorCode=%s, SQLState=%s, message=%s :", e.getErrorCode(), e.getSQLState(), e.getMessage());
				}
				LOG.warn((sqlExceptionStr != null ? sqlExceptionStr : "SQL OK:") + " connType=" + getConnTypeStr()
						+ ", templateId=" + getTemplateId()
						+ ", time=" + allTime + "ms, threadName=" + threadName + ", " + getSQLScript());
			} else if (LOG.isInfoEnabled() && allTime >= getInfoThreshold()) {
				LOG.info("SQL OK: connType={}, time={} ms, sql ={}", getConnTypeStr(), allTime, getSQLScript());
			}
		} catch (Exception ex) {
			LOG.error("Error logging SQL. SQL Time=" + allTime + "ms", ex);
		}
		startTime = 0;
	}

	public LoggableStatement(Connection connection, String sql, int autoGeneratedKeys, PreparedStatement ps)
			throws SQLException {
		if (wrappedStatement != null) {
			wrappedStatement = ps;
		} else {
			wrappedStatement = connection.prepareStatement(sql, autoGeneratedKeys);
		}
		sqlTemplate = sql;
		parameterValues = new HashMap<Integer, KVParam>();

		threadName = Thread.currentThread().getName();

		registerLoggableStatement();
	}

	public LoggableStatement(Connection connection, String sql, int autoGeneratedKeys) throws SQLException {
		this(connection, sql, autoGeneratedKeys, null);
	}

	protected class SQLTemplatesInfo {
		public long templateId;
		public int templateUsages;
		public float avgTime;
		public float maxTime;
		String connType;
	}

	private void registerLoggableStatement() {
		synchronized (loggableStatements) {
			counter = ++counterSequence;
			loggableStatements.put(counter, this);
			SQLTemplatesInfo templateInfo = sqlTemplates.get(getConnTypeStr() + sqlTemplate);
			if (templateInfo != null) {
				this.templateId = templateInfo.templateId;
			} else {
				SQLTemplatesInfo ti = new SQLTemplatesInfo();
				ti.templateId = ++templateSequence;
				this.templateId = ti.templateId;
				LOG.info("New SQL Template id=" + getTemplateId() + ", sqlTemplate=" + sqlTemplate);
				sqlTemplates.put(getConnTypeStr() + sqlTemplate, ti);
			}
		}
	}

	public LoggableStatement(Connection connection, String sql, PreparedStatement ps) throws SQLException {
		if (ps != null) {
			wrappedStatement = ps;
		} else {
			wrappedStatement = connection.prepareStatement(sql);
		}
		sqlTemplate = sql;
		parameterValues = new HashMap<Integer, KVParam>();
		threadName = Thread.currentThread().getName();

		registerLoggableStatement();

	}

	public LoggableStatement(Connection connection, String sql) throws SQLException {
		this(connection, sql, null);
	}

	@Override
	public void addBatch() throws SQLException {
		wrappedStatement.addBatch();
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		wrappedStatement.addBatch(sql);
	}

	@Override
	public void cancel() throws SQLException {
		wrappedStatement.cancel();
	}

	@Override
	public void clearBatch() throws SQLException {
		wrappedStatement.clearBatch();
	}

	@Override
	public void clearParameters() throws SQLException {
		wrappedStatement.clearParameters();
	}

	@Override
	public void clearWarnings() throws SQLException {
		wrappedStatement.clearWarnings();
	}

	@Override
	public void close() throws SQLException {
		synchronized (loggableStatements) {
			loggableStatements.remove(counter);
		}

		wrappedStatement.close();
	}

	@Override
	public boolean execute() throws SQLException {
		try {
			logStart();
			return wrappedStatement.execute();
		} catch (SQLException e) {
			logEnd(e);
			throw e;
		} finally {
			logEnd();
		}
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		try {
			logStart();
			return wrappedStatement.execute(sql, autoGeneratedKeys);
		} catch (SQLException e) {
			logEnd(e);
			throw e;
		} finally {
			logEnd();
		}

	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		try {
			logStart();
			return wrappedStatement.execute(sql, columnIndexes);
		} catch (SQLException e) {
			logEnd(e);
			throw e;
		} finally {
			logEnd();
		}

	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		try {
			logStart();
			return wrappedStatement.execute(sql, columnNames);
		} catch (SQLException e) {
			logEnd(e);
			throw e;
		} finally {
			logEnd();
		}

	}

	@Override
	public boolean execute(String sql) throws SQLException {
		try {
			logStart();
			return wrappedStatement.execute(sql);
		} catch (SQLException e) {
			logEnd(e);
			throw e;
		} finally {
			logEnd();
		}

	}

	@Override
	public int[] executeBatch() throws SQLException {
		try {
			logStart();
			return wrappedStatement.executeBatch();
		} catch (SQLException e) {
			logEnd(e);
			throw e;
		} finally {
			logEnd();
		}

	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		try {
			logStart();
			return wrappedStatement.executeQuery();
		} catch (SQLException e) {
			logEnd(e);
			throw e;
		} finally {
			logEnd();
		}

	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		try {
			logStart();
			return wrappedStatement.executeQuery(sql);
		} catch (SQLException e) {
			logEnd(e);
			throw e;
		} finally {
			logEnd();
		}

	}

	@Override
	public int executeUpdate() throws SQLException {
		try {
			logStart();
			return wrappedStatement.executeUpdate();
		} catch (SQLException e) {
			logEnd(e);
			throw e;
		} finally {
			logEnd();
		}

	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		try {
			logStart();
			return wrappedStatement.executeUpdate(sql, autoGeneratedKeys);
		} catch (SQLException e) {
			logEnd(e);
			throw e;
		} finally {
			logEnd();
		}

	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		try {
			logStart();
			return wrappedStatement.executeUpdate(sql, columnIndexes);
		} catch (SQLException e) {
			logEnd(e);
			throw e;
		} finally {
			logEnd();
		}

	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		try {
			logStart();
			return wrappedStatement.executeUpdate(sql, columnNames);
		} catch (SQLException e) {
			logEnd(e);
			throw e;
		} finally {
			logEnd();
		}

	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		try {
			logStart();
			return wrappedStatement.executeUpdate(sql);
		} catch (SQLException e) {
			logEnd(e);
			throw e;
		} finally {
			logEnd();
		}

	}

	@Override
	public Connection getConnection() throws SQLException {
		return wrappedStatement.getConnection();
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return wrappedStatement.getFetchDirection();
	}

	@Override
	public int getFetchSize() throws SQLException {
		return wrappedStatement.getFetchSize();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return wrappedStatement.getGeneratedKeys();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		return wrappedStatement.getMaxFieldSize();
	}

	@Override
	public int getMaxRows() throws SQLException {
		return wrappedStatement.getMaxRows();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return wrappedStatement.getMetaData();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return wrappedStatement.getMoreResults();
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		return wrappedStatement.getMoreResults(current);
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return wrappedStatement.getParameterMetaData();
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return wrappedStatement.getQueryTimeout();
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return wrappedStatement.getResultSet();
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return wrappedStatement.getResultSetConcurrency();
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return wrappedStatement.getResultSetHoldability();
	}

	@Override
	public int getResultSetType() throws SQLException {
		return wrappedStatement.getResultSetType();
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return wrappedStatement.getUpdateCount();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return wrappedStatement.getWarnings();
	}

	@Override
	public void setArray(int i, Array x) throws SQLException {
		parameterValues.put(i, new KVParam(x, Types.ARRAY));
		wrappedStatement.setArray(i, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.LONGVARCHAR));
		wrappedStatement.setAsciiStream(parameterIndex, x, length);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.NUMERIC));
		wrappedStatement.setBigDecimal(parameterIndex, x);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.LONGVARBINARY));
		wrappedStatement.setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void setBlob(int i, Blob x) throws SQLException {
		parameterValues.put(i, new KVParam(x, Types.BLOB));
		wrappedStatement.setBlob(i, x);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.BIT));
		wrappedStatement.setBoolean(parameterIndex, x);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.TINYINT));
		wrappedStatement.setByte(parameterIndex, x);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.VARBINARY));
		wrappedStatement.setBytes(parameterIndex, x);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(parameterIndex, Types.LONGVARCHAR));
		wrappedStatement.setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setClob(int i, Clob x) throws SQLException {
		parameterValues.put(i, new KVParam(x, Types.CLOB));
		wrappedStatement.setClob(i, x);
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		wrappedStatement.setCursorName(name);
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.DATE));
		wrappedStatement.setDate(parameterIndex, x, cal);
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.DATE));
		wrappedStatement.setDate(parameterIndex, x);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.DOUBLE));
		wrappedStatement.setDouble(parameterIndex, x);
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		wrappedStatement.setEscapeProcessing(enable);
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		wrappedStatement.setFetchDirection(direction);
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		wrappedStatement.setFetchSize(rows);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.FLOAT));
		wrappedStatement.setFloat(parameterIndex, x);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.INTEGER));
		wrappedStatement.setInt(parameterIndex, x);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.BIGINT));
		wrappedStatement.setLong(parameterIndex, x);
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		wrappedStatement.setMaxFieldSize(max);
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		wrappedStatement.setMaxRows(max);
	}

	@Override
	public void setNull(int paramIndex, int sqlType, String typeName) throws SQLException {
		parameterValues.put(paramIndex, new KVParam(null, sqlType));
		wrappedStatement.setNull(paramIndex, sqlType, typeName);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(null, sqlType));
		wrappedStatement.setNull(parameterIndex, sqlType);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scale) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, targetSqlType));
		wrappedStatement.setObject(parameterIndex, x, targetSqlType, scale);
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(null, targetSqlType));
		wrappedStatement.setObject(parameterIndex, x, targetSqlType);
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.NUMERIC));
		wrappedStatement.setObject(parameterIndex, x);
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		wrappedStatement.setQueryTimeout(seconds);
	}

	@Override
	public void setRef(int i, Ref x) throws SQLException {
		parameterValues.put(i, new KVParam(x, Types.REF));
		wrappedStatement.setRef(i, x);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.SMALLINT));
		wrappedStatement.setShort(parameterIndex, x);
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.VARCHAR));
		wrappedStatement.setString(parameterIndex, x);
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.TIME));
		wrappedStatement.setTime(parameterIndex, x, cal);
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(null, Types.TIME));
		wrappedStatement.setTime(parameterIndex, x);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(null, Types.TIMESTAMP));
		wrappedStatement.setTimestamp(parameterIndex, x, cal);
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(null, Types.TIMESTAMP));
		wrappedStatement.setTimestamp(parameterIndex, x);
	}

	@Override
	@Deprecated
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.LONGVARCHAR));
		wrappedStatement.setUnicodeStream(parameterIndex, x, length);
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.DATALINK));
		wrappedStatement.setURL(parameterIndex, x);
	}

	protected class KVParam {
		private Object value;
		private int sqlType;

		public int getSqlType() {
			return sqlType;
		}

		public void setSqlType(int sqlType) {
			this.sqlType = sqlType;
		}

		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public KVParam(Object value, int sqlType) {
			super();
			this.value = value;
			this.sqlType = sqlType;
		}

	}

	public static String list() {
		String str = "PreparedStatements: ";
		synchronized (loggableStatements) {
			for (LoggableStatement ls : loggableStatements.values()) {
				str += ls + "\n";

			}
		}
		return str;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	@Override
	public String toString() {
		return "expectedMaxTime=" + getExpectedMaxTime() + ", threadName=" + threadName + ", timeElapsed="
				+ (getStartTime() > 0 ? (System.currentTimeMillis() - getStartTime()) + "ms" : "N/A") + ", "
				+ getSQLScript();

	}

	static {
		Thread th = new Thread() {
			private final static long SLEEP_TIME = 300000;
			private final static long QUERY_TIME = 30000;

			@Override
			public void run() {
				while (true) {
					synchronized (loggableStatements) {
						LOG.info("Monitoring LoggableStatements... loggableStatements.size()={}",
								loggableStatements.size());
						long now = System.currentTimeMillis();

						for (LoggableStatement ls : loggableStatements.values()) {
							if (ls.getStartTime() > 0) {
								if ((now - ls.getStartTime()) > QUERY_TIME) {
									LOG.warn("Long lasting query!!! {}", ls);
								}
							}
						}
					}
					try {
						LOG.info("Sleeping monitoring for {}", SLEEP_TIME);
						Thread.sleep(SLEEP_TIME);
					} catch (InterruptedException e) {
						return;
					}
				}
			}
		};
		th.setDaemon(true);
		th.start();
	}

	public String getThreadName() {
		return threadName;
	}

	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}

	public String getTemplateId() {
		return templatePrefix + templateId;
	}

	public void setTemplateId(long templateId) {
		this.templateId = templateId;
	}

	public static String listTemplates() {
		String templatesStr = "";
		DecimalFormat dformat = new DecimalFormat("#.00");
		for (Map.Entry<String, SQLTemplatesInfo> e : sqlTemplates.entrySet()) {
			SQLTemplatesInfo el = e.getValue();
			String t = e.getKey();

			templatesStr += el.templateId + ". connType=" + el.connType + "templateUsages=" + el.templateUsages
					+ ", maxTime=" + ("" + dformat.format(el.maxTime)).replace('.', ',') + ", avgTime="
					+ ("" + dformat.format(el.avgTime)).replace('.', ',') + "\n\nsql=" + t + "\n\n";
		}
		return templatesStr;
	}

	public String getConnTypeStr() {
		if (connType == null) {
			return "<unknown>";
		} else {
			return connType;
		}
	}

	public void setConnType(String connType) {
		this.connType = connType;
	}

	public Properties getProps() {
		return props;
	}

	public void setProps(Properties props) {
		this.props = props;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return wrappedStatement.isClosed();
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		wrappedStatement.setPoolable(poolable);
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return wrappedStatement.isPoolable();
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		wrappedStatement.closeOnCompletion();
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return wrappedStatement.isCloseOnCompletion();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return wrappedStatement.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return wrappedStatement.isWrapperFor(iface);
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.LONGVARCHAR));
		wrappedStatement.setRowId(parameterIndex, x);
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(value, Types.NVARCHAR));
		wrappedStatement.setNString(parameterIndex, value);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(reader, Types.NCHAR));
		wrappedStatement.setNCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(value, Types.NCLOB));
		wrappedStatement.setNClob(parameterIndex, value);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(reader, Types.CLOB));
		wrappedStatement.setClob(parameterIndex, reader, length);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(inputStream, Types.BLOB));
		wrappedStatement.setBlob(parameterIndex, inputStream, length);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(reader, Types.NCLOB));
		wrappedStatement.setNClob(parameterIndex, reader, length);
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(xmlObject, Types.SQLXML));
		wrappedStatement.setSQLXML(parameterIndex, xmlObject);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.LONGVARCHAR));
		wrappedStatement.setAsciiStream(parameterIndex, x, length);
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.BINARY));
		wrappedStatement.setBinaryStream(parameterIndex, x, length);
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(reader, Types.LONGVARCHAR));
		wrappedStatement.setCharacterStream(parameterIndex, reader, length);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.LONGVARCHAR));
		wrappedStatement.setAsciiStream(parameterIndex, x);

	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.BINARY));
		wrappedStatement.setBinaryStream(parameterIndex, x);

	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(reader, Types.LONGVARCHAR));
		wrappedStatement.setCharacterStream(parameterIndex, reader);

	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(reader, Types.LONGNVARCHAR));
		wrappedStatement.setNCharacterStream(parameterIndex, reader);
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(reader, Types.CLOB));
		wrappedStatement.setClob(parameterIndex, reader);
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(inputStream, Types.BLOB));
		wrappedStatement.setBlob(parameterIndex, inputStream);
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(reader, Types.NCLOB));
		wrappedStatement.setNClob(parameterIndex, reader);
	}

}
