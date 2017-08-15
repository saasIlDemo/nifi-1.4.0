package org.apache.nifi.processors.sha0w;

import org.apache.avro.Schema;
import org.apache.avro.file.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


public class DemoAvroProcessor extends AbstractProcessor {
    private final DataFileWriter<GenericRecord> failureAvroWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());
    private final DataFileWriter<GenericRecord> successAvroWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>());
    private final Logger logger = LoggerFactory.getLogger(DemoAvroProcessor.class);
    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from HiveQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("fail to created FlowFile from HiveQL query result set.")
            .build();
    //PropertiesDescriptor
    private static final PropertyDescriptor REQUEST_COLUMN = new PropertyDescriptor.Builder()
            .name("request")
            .description("A STRING SPLIT BY COMMA, EACH ONE WILL BUILD THE RETURN AVRO")
            .required(true)
            .defaultValue("*")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(REQUEST_COLUMN);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);
        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final List<FlowFile> retFlowFile = new ArrayList<>();
        final List<GenericRecord> genericRecords = new ArrayList<>();
        //read flowfile
        session.read(flowFile, in -> {
            final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>());
            while (reader.hasNext()) {
                genericRecords.add(reader.next());
            }
        });
        //edit flowfile
        FlowFile childFlowFile = session.create(flowFile);
        session.write(childFlowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream rawOut) throws IOException {
                OutputStream bfo = new BufferedOutputStream(rawOut);
                DatumWriter<GenericRecord> writter = new GenericDatumWriter<>();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bfo, null);
                long processTimes = processRecord(genericRecords, genericRecords.get(0).getSchema(), encoder, splitString(context));
            }
        });
        //submit
        session.transfer(childFlowFile,REL_SUCCESS);
    }

    private long processRecord(List<GenericRecord> genericRecords, Schema schema,Encoder encoder, String... request) throws IOException {
        long count = 0;
        List<GenericRecord> retGr = new ArrayList<>();
        List<Schema.Field> fields = mkField(schema, request);
        Schema newSchema = Schema.createRecord(fields);
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>();
        GenericRecord ret = null;
        for (GenericRecord gr : genericRecords) {
            for (Schema.Field field : fields) {
                ret = new GenericData.Record(schema);
                ret.put(field.name(), gr.get(field.name()));
            }
            writer.write(ret, encoder);
            count ++;
        }
        encoder.flush();
        return count;
    }
    private String[] splitString(ProcessContext context) {
        return context.getProperty("request").getValue().split(",");
    }

    private List<Schema.Field> mkField(Schema schema, String... req) {
        List<Schema.Field> fields = new ArrayList<>();
        for (String r : req) {
            fields.add(schema.getField(r));
        }
        return fields;
    }
}
