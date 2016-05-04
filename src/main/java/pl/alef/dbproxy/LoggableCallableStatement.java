package pl.alef.dbproxy;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Map;

public class LoggableCallableStatement extends LoggableStatement implements CallableStatement {

	protected CallableStatement callableStatement;

	public LoggableCallableStatement(Connection connection, String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		super(connection, sql, null);
		if (connection instanceof LoggableConnection) {
			LoggableConnection loggableConnection = (LoggableConnection) connection;
			this.callableStatement = loggableConnection.wrappedConnection.prepareCall(sql, resultSetType,
					resultSetConcurrency);
		} else {
			this.callableStatement = connection.prepareCall(sql, resultSetType, resultSetConcurrency);
		}
		super.wrappedStatement = this.callableStatement;
	}

	public LoggableCallableStatement(Connection connection, String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		super(connection, sql, null);
		if (connection instanceof LoggableConnection) {
			this.callableStatement = ((LoggableConnection) connection).wrappedConnection.prepareCall(sql, resultSetType,
					resultSetConcurrency, resultSetHoldability);
		} else {
			this.callableStatement = connection.prepareCall(sql, resultSetType, resultSetConcurrency,
					resultSetHoldability);
		}
		super.wrappedStatement = this.callableStatement;
	}

	public LoggableCallableStatement(Connection connection, String sql) throws SQLException {
		super(connection, sql, null);
		if (connection instanceof LoggableConnection) {
			this.callableStatement = ((LoggableConnection) connection).prepareCall(sql);
		} else {
			this.callableStatement = connection.prepareCall(sql);
		}
		super.wrappedStatement = this.callableStatement;

	}

	@Override
	public Array getArray(int i) throws SQLException {
		return callableStatement.getArray(i);
	}

	@Override
	public Array getArray(String parameterName) throws SQLException {
		return callableStatement.getArray(parameterName);
	}

	@Override
	@Deprecated
	public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
		return callableStatement.getBigDecimal(parameterIndex, scale);
	}

	@Override
	public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
		return callableStatement.getBigDecimal(parameterIndex);
	}

	@Override
	public BigDecimal getBigDecimal(String parameterName) throws SQLException {
		return callableStatement.getBigDecimal(parameterName);
	}

	@Override
	public Blob getBlob(int i) throws SQLException {
		return callableStatement.getBlob(i);
	}

	@Override
	public Blob getBlob(String parameterName) throws SQLException {
		return callableStatement.getBlob(parameterName);
	}

	@Override
	public boolean getBoolean(int parameterIndex) throws SQLException {
		return callableStatement.getBoolean(parameterIndex);
	}

	@Override
	public boolean getBoolean(String parameterName) throws SQLException {
		return callableStatement.getBoolean(parameterName);
	}

	@Override
	public byte getByte(int parameterIndex) throws SQLException {
		return callableStatement.getByte(parameterIndex);
	}

	@Override
	public byte getByte(String parameterName) throws SQLException {
		return callableStatement.getByte(parameterName);
	}

	@Override
	public byte[] getBytes(int parameterIndex) throws SQLException {
		return callableStatement.getBytes(parameterIndex);
	}

	@Override
	public byte[] getBytes(String parameterName) throws SQLException {
		return callableStatement.getBytes(parameterName);
	}

	@Override
	public Clob getClob(int i) throws SQLException {
		return callableStatement.getClob(i);
	}

	@Override
	public Clob getClob(String parameterName) throws SQLException {
		return callableStatement.getClob(parameterName);
	}

	@Override
	public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
		return callableStatement.getDate(parameterIndex, cal);
	}

	@Override
	public Date getDate(int parameterIndex) throws SQLException {
		return callableStatement.getDate(parameterIndex);
	}

	@Override
	public Date getDate(String parameterName, Calendar cal) throws SQLException {
		return callableStatement.getDate(parameterName, cal);
	}

	@Override
	public Date getDate(String parameterName) throws SQLException {
		return callableStatement.getDate(parameterName);
	}

	@Override
	public double getDouble(int parameterIndex) throws SQLException {
		return callableStatement.getDouble(parameterIndex);
	}

	@Override
	public double getDouble(String parameterName) throws SQLException {
		return callableStatement.getDouble(parameterName);
	}

	@Override
	public float getFloat(int parameterIndex) throws SQLException {
		return callableStatement.getFloat(parameterIndex);
	}

	@Override
	public float getFloat(String parameterName) throws SQLException {
		return callableStatement.getFloat(parameterName);
	}

	@Override
	public int getInt(int parameterIndex) throws SQLException {
		return callableStatement.getInt(parameterIndex);
	}

	@Override
	public int getInt(String parameterName) throws SQLException {
		return callableStatement.getInt(parameterName);
	}

	@Override
	public long getLong(int parameterIndex) throws SQLException {
		return callableStatement.getLong(parameterIndex);
	}

	@Override
	public long getLong(String parameterName) throws SQLException {
		return callableStatement.getLong(parameterName);
	}

	@Override
	public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
		return callableStatement.getObject(i, map);
	}

	@Override
	public Object getObject(int parameterIndex) throws SQLException {
		return callableStatement.getObject(parameterIndex);
	}

	@Override
	public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
		return callableStatement.getObject(parameterName, map);
	}

	@Override
	public Object getObject(String parameterName) throws SQLException {
		return callableStatement.getObject(parameterName);
	}

	@Override
	public Ref getRef(int i) throws SQLException {
		return callableStatement.getRef(i);
	}

	@Override
	public Ref getRef(String parameterName) throws SQLException {
		return callableStatement.getRef(parameterName);
	}

	@Override
	public short getShort(int parameterIndex) throws SQLException {
		return callableStatement.getShort(parameterIndex);
	}

	@Override
	public short getShort(String parameterName) throws SQLException {
		return callableStatement.getShort(parameterName);
	}

	@Override
	public String getString(int parameterIndex) throws SQLException {
		return callableStatement.getString(parameterIndex);
	}

	@Override
	public String getString(String parameterName) throws SQLException {
		return callableStatement.getString(parameterName);
	}

	@Override
	public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
		return callableStatement.getTime(parameterIndex, cal);
	}

	@Override
	public Time getTime(int parameterIndex) throws SQLException {
		return callableStatement.getTime(parameterIndex);
	}

	@Override
	public Time getTime(String parameterName, Calendar cal) throws SQLException {
		return callableStatement.getTime(parameterName, cal);
	}

	@Override
	public Time getTime(String parameterName) throws SQLException {
		return callableStatement.getTime(parameterName);
	}

	@Override
	public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
		return callableStatement.getTimestamp(parameterIndex, cal);
	}

	@Override
	public Timestamp getTimestamp(int parameterIndex) throws SQLException {
		return callableStatement.getTimestamp(parameterIndex);
	}

	@Override
	public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
		return callableStatement.getTimestamp(parameterName, cal);
	}

	@Override
	public Timestamp getTimestamp(String parameterName) throws SQLException {
		return callableStatement.getTimestamp(parameterName);
	}

	@Override
	public URL getURL(int parameterIndex) throws SQLException {
		return callableStatement.getURL(parameterIndex);
	}

	@Override
	public URL getURL(String parameterName) throws SQLException {
		return callableStatement.getURL(parameterName);
	}

	@Override
	public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
		callableStatement.registerOutParameter(parameterIndex, sqlType, scale);
	}

	@Override
	public void registerOutParameter(int paramIndex, int sqlType, String typeName) throws SQLException {
		callableStatement.registerOutParameter(paramIndex, sqlType, typeName);
	}

	@Override
	public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
		callableStatement.registerOutParameter(parameterIndex, sqlType);
	}

	@Override
	public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
		callableStatement.registerOutParameter(parameterName, sqlType, scale);
	}

	@Override
	public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
		callableStatement.registerOutParameter(parameterName, sqlType, typeName);
	}

	@Override
	public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
		callableStatement.registerOutParameter(parameterName, sqlType);
	}

	@Override
	public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
		// parameterValues.put(parameterIndex, new KVParam(x, Types.NUMERIC));
		callableStatement.setAsciiStream(parameterName, x, length);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
		parameterValues.put(parameterIndex, new KVParam(x, Types.NUMERIC));
		callableStatement.setBigDecimal(parameterIndex, x);
	}

	@Override
	public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
		callableStatement.setBigDecimal(parameterName, x);
	}

	@Override
	public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
		callableStatement.setBinaryStream(parameterName, x, length);
	}

	@Override
	public void setBoolean(String parameterName, boolean x) throws SQLException {
		callableStatement.setBoolean(parameterName, x);
	}

	@Override
	public void setByte(String parameterName, byte x) throws SQLException {
		callableStatement.setByte(parameterName, x);
	}

	@Override
	public void setBytes(String parameterName, byte[] x) throws SQLException {
		callableStatement.setBytes(parameterName, x);
	}

	@Override
	public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
		callableStatement.setCharacterStream(parameterName, reader, length);
	}

	@Override
	public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
		callableStatement.setDate(parameterName, x, cal);
	}

	@Override
	public void setDate(String parameterName, Date x) throws SQLException {
		callableStatement.setDate(parameterName, x);
	}

	@Override
	public void setDouble(String parameterName, double x) throws SQLException {
		callableStatement.setDouble(parameterName, x);
	}

	@Override
	public void setFloat(String parameterName, float x) throws SQLException {
		callableStatement.setFloat(parameterName, x);
	}

	@Override
	public void setInt(String parameterName, int x) throws SQLException {
		callableStatement.setInt(parameterName, x);
	}

	@Override
	public void setLong(String parameterName, long x) throws SQLException {
		callableStatement.setLong(parameterName, x);
	}

	@Override
	public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
		callableStatement.setNull(parameterName, sqlType, typeName);
	}

	@Override
	public void setNull(String parameterName, int sqlType) throws SQLException {
		callableStatement.setNull(parameterName, sqlType);
	}

	@Override
	public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
		callableStatement.setObject(parameterName, x, targetSqlType, scale);
	}

	@Override
	public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
		callableStatement.setObject(parameterName, x, targetSqlType);
	}

	@Override
	public void setObject(String parameterName, Object x) throws SQLException {
		callableStatement.setObject(parameterName, x);
	}

	@Override
	public void setShort(String parameterName, short x) throws SQLException {
		callableStatement.setShort(parameterName, x);
	}

	@Override
	public void setString(String parameterName, String x) throws SQLException {
		callableStatement.setString(parameterName, x);
	}

	@Override
	public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
		callableStatement.setTime(parameterName, x, cal);
	}

	@Override
	public void setTime(String parameterName, Time x) throws SQLException {
		callableStatement.setTime(parameterName, x);
	}

	@Override
	public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
		callableStatement.setTimestamp(parameterName, x, cal);
	}

	@Override
	public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
		callableStatement.setTimestamp(parameterName, x);
	}

	@Override
	public void setURL(String parameterName, URL val) throws SQLException {
		callableStatement.setURL(parameterName, val);
	}

	@Override
	public boolean wasNull() throws SQLException {
		return callableStatement.wasNull();
	}

	@Override
	public RowId getRowId(int parameterIndex) throws SQLException {
		return callableStatement.getRowId(parameterIndex);
	}

	@Override
	public RowId getRowId(String parameterName) throws SQLException {
		return callableStatement.getRowId(parameterName);
	}

	@Override
	public void setRowId(String parameterName, RowId x) throws SQLException {
		callableStatement.setRowId(parameterName, x);
	}

	@Override
	public void setNString(String parameterName, String value) throws SQLException {
		callableStatement.setNString(parameterName, value);
	}

	@Override
	public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
		callableStatement.setNCharacterStream(parameterName, value, length);
	}

	@Override
	public void setNClob(String parameterName, NClob value) throws SQLException {
		callableStatement.setNClob(parameterName, value);
	}

	@Override
	public void setClob(String parameterName, Reader reader, long length) throws SQLException {
		callableStatement.setClob(parameterName, reader, length);
	}

	@Override
	public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
		callableStatement.setBlob(parameterName, inputStream, length);
	}

	@Override
	public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
		callableStatement.setNClob(parameterName, reader, length);
	}

	@Override
	public NClob getNClob(int parameterIndex) throws SQLException {
		return callableStatement.getNClob(parameterIndex);
	}

	@Override
	public NClob getNClob(String parameterName) throws SQLException {
		return callableStatement.getNClob(parameterName);
	}

	@Override
	public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
		callableStatement.setSQLXML(parameterName, xmlObject);
	}

	@Override
	public SQLXML getSQLXML(int parameterIndex) throws SQLException {
		return callableStatement.getSQLXML(parameterIndex);
	}

	@Override
	public SQLXML getSQLXML(String parameterName) throws SQLException {
		return callableStatement.getSQLXML(parameterName);
	}

	@Override
	public String getNString(int parameterIndex) throws SQLException {
		return callableStatement.getNString(parameterIndex);
	}

	@Override
	public String getNString(String parameterName) throws SQLException {
		return callableStatement.getNString(parameterName);
	}

	@Override
	public Reader getNCharacterStream(int parameterIndex) throws SQLException {
		return callableStatement.getNCharacterStream(parameterIndex);
	}

	@Override
	public Reader getNCharacterStream(String parameterName) throws SQLException {
		return callableStatement.getNCharacterStream(parameterName);
	}

	@Override
	public Reader getCharacterStream(int parameterIndex) throws SQLException {
		return callableStatement.getCharacterStream(parameterIndex);
	}

	@Override
	public Reader getCharacterStream(String parameterName) throws SQLException {
		return callableStatement.getCharacterStream(parameterName);
	}

	@Override
	public void setBlob(String parameterName, Blob x) throws SQLException {
		callableStatement.setBlob(parameterName, x);
	}

	@Override
	public void setClob(String parameterName, Clob x) throws SQLException {
		callableStatement.setClob(parameterName, x);
	}

	@Override
	public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
		callableStatement.setAsciiStream(parameterName, x, length);
	}

	@Override
	public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
		callableStatement.setBinaryStream(parameterName, x, length);
	}

	@Override
	public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
		callableStatement.setCharacterStream(parameterName, reader, length);
	}

	@Override
	public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
		callableStatement.setAsciiStream(parameterName, x);
	}

	@Override
	public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
		callableStatement.setBinaryStream(parameterName, x);
	}

	@Override
	public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
		callableStatement.setCharacterStream(parameterName, reader);
	}

	@Override
	public void setNCharacterStream(String parameterName, Reader reader) throws SQLException {
		callableStatement.setNCharacterStream(parameterName, reader);
	}

	@Override
	public void setClob(String parameterName, Reader reader) throws SQLException {
		callableStatement.setClob(parameterName, reader);
	}

	@Override
	public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
		callableStatement.setBlob(parameterName, inputStream);
	}

	@Override
	public void setNClob(String parameterName, Reader reader) throws SQLException {
		callableStatement.setNClob(parameterName, reader);
	}

	@Override
	public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
		return callableStatement.getObject(parameterIndex, type);
	}

	@Override
	public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
		return callableStatement.getObject(parameterName, type);
	}

}
