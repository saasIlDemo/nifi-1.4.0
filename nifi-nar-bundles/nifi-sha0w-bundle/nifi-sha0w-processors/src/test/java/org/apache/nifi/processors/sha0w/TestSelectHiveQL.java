package org.apache.nifi.processors.sha0w;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.hive.HiveConnectionPool;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processors.standard.PutFile;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class TestSelectHiveQL {
    @Test
    public void testSHQ() throws InitializationException {
        final String path = "D:\\nifi\\nifi-nar-bundles\\nifi-sha0w-bundle\\nifi-sha0w-processors\\src\\test\\java\\org\\apache\\nifi\\processors\\sha0w\\HiveConf\\";
        final String hivesit = path + "hive-site.xml";
        final String hivelog4j = path + "hive-log4j.properties";
        final String hiveexeclog4j = path + "hive-exec-log4j.properties";
        final String hiveenv = path + "hive-env.sh";

        TestRunner testRunner = TestRunners.newTestRunner(new SelectHiveQL());
        TestRunner testRunner_w = TestRunners.newTestRunner(new PutFile());
        TestRunner testRunner_A = TestRunners.newTestRunner(new DemoAvroProcessor());
        final AbstractControllerService cacheService = new HiveConnectionPool();
        //HiveDBCP init
        testRunner.addControllerService("Database Connection Pooling Service", cacheService);
//        testRunner.setProperty(cacheService, HiveConnectionPool.DATABASE_URL,"jdbc:hive2://10.0.82.168:2181,10.0.82.169:2181,10.0.82.170:2181,10.0.82.173:2181,10.0.82.172:2181,10.0.82.171:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2");
        testRunner.setProperty(cacheService, HiveConnectionPool.DATABASE_URL,"jdbc:hive2://10.0.82.170:10000/default");
        testRunner.setProperty(cacheService, HiveConnectionPool.DB_PASSWORD,"hive");
        testRunner.setProperty(cacheService, HiveConnectionPool.DB_USER,"hive");
        testRunner.setProperty(cacheService, HiveConnectionPool.HIVE_CONFIGURATION_RESOURCES,
                hivesit );
        testRunner.setProperty(cacheService, HiveConnectionPool.MAX_TOTAL_CONNECTIONS,"8");
        testRunner.setProperty(cacheService, HiveConnectionPool.MAX_WAIT_TIME,"30000 millis");
        testRunner.setProperty(cacheService, HiveConnectionPool.VALIDATION_QUERY,"select 1 from cal_hr_322");

        testRunner.enableControllerService(cacheService);

        testRunner.setProperty(SelectHiveQL.HIVE_DBCP_SERVICE, "Database Connection Pooling Service");
        testRunner.setProperty(SelectHiveQL.HIVEQL_SELECT_QUERY, "select * from city");
        testRunner.setProperty(SelectHiveQL.HIVEQL_OUTPUT_FORMAT, SelectHiveQL.AVRO);
        testRunner.setIncomingConnection(false);
        testRunner.run();

        //Writer init
        testRunner_w.setProperty(PutFile.DIRECTORY, "G:\\nifiLog\\out");
        testRunner_w.addConnection(SelectHiveQL.REL_SUCCESS);
        testRunner_w.setIncomingConnection(true);

        testRunner_A.addConnection(SelectHiveQL.REL_SUCCESS);
        testRunner_A.setIncomingConnection(true);

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(SelectHiveQL.REL_SUCCESS);
        for (FlowFile f : flowFiles){
            testRunner_A.enqueue(f);
        }
        testRunner_A.run();
        List<MockFlowFile> flowFiles1 = testRunner_A.getFlowFilesForRelationship(SelectHiveQL.REL_SUCCESS);
        for (FlowFile f : flowFiles1){
            System.out.println(f);
        }
    }
}
