package dev.langchain4j.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class AlloyDBTestUtils {

    public static void verifyColumns(Connection connection, String tableName, Set<String> expectedColumns) throws SQLException {
        Set<String> actualNames = new HashSet<>();

        ResultSet resultSet = connection.createStatement().executeQuery("SELECT * FROM " + tableName);
        ResultSetMetaData rsMeta = resultSet.getMetaData();
        int columnCount = rsMeta.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            actualNames.add(rsMeta.getColumnName(i));

        }
        assertThat(actualNames).isEqualTo(expectedColumns);

    }

    public static void verifyIndex(Connection connection, String tableName, String type, String expected) throws SQLException {
        ResultSet indexes = connection.createStatement().executeQuery(String.format("SELECT indexdef FROM pg_indexes WHERE tablename = '%s' AND indexname = '%s_%s_index'", tableName.toLowerCase(), tableName.toLowerCase(), type));
        while (indexes.next()) {
            assertThat(indexes.getString("indexdef")).contains(expected);
        }
    }
}
