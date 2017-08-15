package org.apache.nifi.processors.sha0w;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.apache.nifi.processors.sha0w.WriteToLocalFile.APPEND_RESOLUTION;

/**
 * Created by coco1 on 2017/7/20.
 */
public class WriteToLocalFileTest {
    @Test
    public void writeTest() throws IOException {
        InputStream content = new ByteArrayInputStream("{\"hello\":\"nifi rocks\"}".getBytes());
        TestRunner testRunner = TestRunners.newTestRunner(new WriteToLocalFile());
        testRunner.setProperty(WriteToLocalFile.FILE_NAME, "test.out");
        testRunner.setProperty(WriteToLocalFile.FILE_PATH, "G:\\User\\Sha0w\\文档\\Nifi-Project\\nifiOut");
        testRunner.setProperty(WriteToLocalFile.CREATE_ABLE, "true");
        testRunner.setProperty(WriteToLocalFile.CONFLICT_RESOLUTION, APPEND_RESOLUTION);
        System.out.println(content.toString());
        testRunner.enqueue(content);
        testRunner.run();
        testRunner.assertQueueEmpty();

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(WriteToLocalFile.SUCCESS);
        for (MockFlowFile result : results) {
            System.out.println("Match: " + IOUtils.toString(result.toByteArray()));
        }
    }
}
