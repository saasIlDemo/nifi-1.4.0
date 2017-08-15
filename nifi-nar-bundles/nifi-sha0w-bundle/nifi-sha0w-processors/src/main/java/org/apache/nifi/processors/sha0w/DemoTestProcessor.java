package org.apache.nifi.processors.sha0w;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class DemoTestProcessor extends AbstractProcessor {
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private static AtomicLong staticStuff = new AtomicLong(0);

    public static final PropertyDescriptor PROPERTY = new PropertyDescriptor.Builder()
            .defaultValue("10")
            .name("DEMO_PROPERTY")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure relationship")
            .build();

    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(PROPERTY);
        this.properties = Collections.unmodifiableList(pds);
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
//        防止多线程ADD
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.create();
        session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(staticStuff.toString().getBytes());
            }
        });
        session.transfer(flowFile);
        session.commit();
        FlowFile flowFile_after = session.create();
        session.write(flowFile_after, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(staticStuff.toString().getBytes());
            }
        });
    }
//    @OnScheduled
//    public void setup(ProcessContext context) {
//        staticStuff.set(context.getProperty("DEMO_PROPERTY").asLong());
//        System.out.print("after set up" + staticStuff);
//    }
}
