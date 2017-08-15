package org.apache.nifi.processors.sha0w;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;


public class DemoTestProcessorTest {
    @Test
    public void writeTest() throws IOException {
//        InputStream content = new ByteArrayInputStream("{\"hello\":\"nifi rocks\"}".getBytes());
//        TestRunner testRunner = TestRunners.newTestRunner(new DemoTestProcessor());
//        testRunner.addConnection(DemoTestProcessor.SUCCESS);
//        testRunner.run();
//        testRunner.assertQueueEmpty();
//        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(WriteToLocalFile.SUCCESS);
//        for (MockFlowFile result : results) {
//            System.out.println("Match: " + IOUtils.toString(result.toByteArray()));
//        }
    }
}
