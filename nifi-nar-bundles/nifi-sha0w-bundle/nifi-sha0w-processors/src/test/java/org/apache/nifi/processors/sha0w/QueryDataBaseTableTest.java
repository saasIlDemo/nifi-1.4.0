package org.apache.nifi.processors.sha0w;

import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPConnectionPool;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class QueryDataBaseTableTest {
    @Test
    public void QDB() throws InitializationException {
        TestRunner testRunner = TestRunners.newTestRunner(QueryDatabaseTable.class);
        final AbstractControllerService cacheService = new DBCPConnectionPool();
        //DBCP init
        testRunner.addControllerService("Database Connection Pooling Service", cacheService);
        testRunner.setProperty(cacheService, DBCPConnectionPool.DATABASE_URL, "jdbc:mysql://127.0.0.1:3306/orcl_cnic?serverTimezone=UTC&characterEncoding=utf8&useSSL=false");
        testRunner.setProperty(cacheService, DBCPConnectionPool.DB_DRIVER_LOCATION, "C:\\Program Files (x86)\\mysql-connector-java-5.1.42\\mysql-connector-java-5.1.42-bin.jar");
        testRunner.setProperty(cacheService, DBCPConnectionPool.DB_DRIVERNAME, "com.mysql.jdbc.Driver");
        testRunner.setProperty(cacheService, DBCPConnectionPool.DB_USER, "root");
        testRunner.setProperty(cacheService, DBCPConnectionPool.DB_PASSWORD, "1234");
        testRunner.setProperty(cacheService, DBCPConnectionPool.MAX_TOTAL_CONNECTIONS, "10");
        testRunner.enableControllerService(cacheService);
        //QueryDBT init
        testRunner.setIncomingConnection(false);
        testRunner.setProperty(QueryDatabaseTable.DBCP_SERVICE, "Database Connection Pooling Service");
        testRunner.setProperty(QueryDatabaseTable.TABLE_NAME,"export_is_pub1_award_full_old");
        testRunner.setProperty(QueryDatabaseTable.FETCH_SIZE,"1000");
        testRunner.setProperty(QueryDatabaseTable.MAX_FRAGMENTS,"0");
        testRunner.setProperty(QueryDatabaseTable.MAX_ROWS_PER_FLOW_FILE, "1000");
        //on schedule
        testRunner.setRunSchedule(0);
        //run
        testRunner.run();
        System.out.println(testRunner.getQueueSize());
//        testRunner.setProperty(QueryDatabaseTable.COLUMN_NAMES,"");
    }
}
