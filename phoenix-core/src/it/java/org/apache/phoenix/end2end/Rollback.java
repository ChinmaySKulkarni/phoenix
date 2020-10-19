package org.apache.phoenix.end2end;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.UpgradeUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Rollback {

    private static final Logger LOGGER = LoggerFactory.getLogger(Rollback.class);

    @Test
    public void repopulateParentChildLinks() {
        final String zkQuorum = "jdbc:phoenix:localhost";
        final String sysCat4_14 = "SYSTEM.CATALOG_4_14";
        try (Connection conn = DriverManager.getConnection(zkQuorum)) {
            // Includes parent->child links that were moved to SYSTEM.CHILD_LINK when doing the
            // metadata upgrade from 4.14.3 to 4.16, as well as parent->child links for tenant views
            // that were created after the upgrade.
            final int expectedChildLinksCount = querySysChildLink(conn);
            UpgradeUtil.addParentToChildLinks(conn.unwrap(PhoenixConnection.class), sysCat4_14);
            assertChildLinksInSysCat4_14(conn, sysCat4_14, expectedChildLinksCount);
        } catch (Exception ex) {
            LOGGER.error(String.format("Failed with exception %s", ex));
        }
    }

    private int querySysChildLink(Connection conn) throws SQLException  {
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM SYSTEM.CHILD_LINK");
        StringBuilder result = new StringBuilder();
        int num = 0;
        while(rs.next()) {
            result.append(rs.getString(1)).append("\t").append(rs.getString(2)).append("\t")
                    .append(rs.getString(3)).append("\t").append(rs.getString(4)).append("\t")
                    .append(rs.getString(5)).append("\t").append(rs.getInt(6)).append("\n");
            num++;
        }
        LOGGER.info(String.format("Queried SYSTEM.CHILD_LINK and got : %d records. "
                + "The table looks like: %s",num, result.toString()));
        return num;
    }

    private void assertChildLinksInSysCat4_14(Connection conn, String sysCatName,
            int expectedChildLinksCount) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(String.format(
                "SELECT COUNT(*) FROM %s WHERE LINK_TYPE=%d", sysCatName,
                PTable.LinkType.CHILD_TABLE.getSerializedValue()));
        assertTrue(rs.next());
        assertEquals("Found an unexpected number of child_links", expectedChildLinksCount,
                rs.getInt(1));
        assertFalse(rs.next());
    }
}
