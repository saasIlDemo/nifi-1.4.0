package org.apache.nifi.processors.sha0w;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPConnectionPool;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;

import static org.apache.nifi.dbcp.DBCPConnectionPool.DATABASE_URL;

public class ExcuteSQKMTest {
    @Test
    public void testExcuteSQL() throws InitializationException {
        TestRunner testRunner = TestRunners.newTestRunner(ExecuteSQLM.class);
        TestRunner testRunner_w = TestRunners.newTestRunner(WriteToLocalFile.class);

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
        //other stuff
        testRunner.setProperty(ExecuteSQLM.DBCP_SERVICE, "Database Connection Pooling Service");
        testRunner.setProperty(ExecuteSQLM.OFFSET_QUERY,"10000");
        testRunner.setProperty(ExecuteSQLM.SQL_SELECT_QUERY, "select * from export_is_pub1_award_full_old ");
        //Writer init
        testRunner_w.setProperty(WriteToLocalFile.FILE_NAME, "log.txt");
        testRunner_w.setProperty(WriteToLocalFile.FILE_PATH, "G:\\nifiLog");
        testRunner_w.addConnection(ExecuteSQLM.REL_SUCCESS);
        testRunner_w.setIncomingConnection(true);
//        testRunner.
        testRunner.setRunSchedule(500);
        testRunner.setThreadCount(1);
        testRunner.setIncomingConnection(false);
        //run
        testRunner.run(6,true);
//        testRunner_w.run();
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(ExecuteSQLM.REL_SUCCESS);
        ProcessSession processSession = testRunner.getProcessSessionFactory().createSession();
        for (FlowFile f : flowFiles){
            testRunner_w.enqueue(f);
        }
        testRunner_w.run();
    }

}
