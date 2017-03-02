package io.taskforcex.tool;

import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Created by dgt on 3/1/17.
 */
public class JdbcSource implements ISource {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    private String url;
    private String user;
    private String password;
    private Connection connection;

    public JdbcSource(String url, String user, String password) throws SQLException {
        this.url = url;
        this.user = user;
        this.password = password;
        init();
    }

    public static JsonObject resultToJson(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        JsonObject resultJson = new JsonObject();

        for (int i = 0; i < resultSetMetaData.getColumnCount(); i ++) {
            String columnName = resultSetMetaData.getColumnName(i + 1);
            int columnType = resultSetMetaData.getColumnType(i + 1);

            switch (columnType) {
                case Types.TINYINT:
                    resultJson.addProperty(columnName, resultSet.getByte(columnName));
                    break;

                case Types.SMALLINT:
                    resultJson.addProperty(columnName, resultSet.getShort(columnName));
                    break;

                case Types.INTEGER:
                    resultJson.addProperty(columnName, resultSet.getInt(columnName));
                    break;

                case Types.BIGINT:
                    resultJson.addProperty(columnName, resultSet.getLong(columnName));
                    break;

                case Types.FLOAT:
                case Types.REAL:
                    resultJson.addProperty(columnName, resultSet.getFloat(columnName));
                    break;

                case Types.DOUBLE:
                    resultJson.addProperty(columnName, resultSet.getDouble(columnName));
                    break;

                case Types.NUMERIC:
                case Types.DECIMAL:
                    resultJson.addProperty(columnName, resultSet.getBigDecimal(columnName));
                    break;

                case Types.BIT:
                case Types.BOOLEAN:
                    resultJson.addProperty(columnName, resultSet.getBoolean(columnName));
                    break;

                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    resultJson.addProperty(columnName, resultSet.getNString(columnName));
                    break;

                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                    resultJson.addProperty(columnName, resultSet.getString(columnName));
                    break;

                default:
                    resultJson.addProperty(columnName, resultSet.getString(columnName));
                    break;
            }
        }

        return resultJson;
    }

    @Override
    public void init() {
        try {
            this.connection = DriverManager.getConnection(this.url, this.user, this.password);
            LOG.info("Successfully connected.");
        } catch (SQLException e) {
            LOG.error("Failed to connect: " + e.getMessage());
        }

    }

    @Override
    public void close() {
        try {
            this.connection.close();
            LOG.info("Successfully closed connection.");
        } catch (SQLException e) {
            LOG.error("Failed to close connection.");
        }
    }

    public Connection getConnection() {
        return this.connection;
    }
}
