package org.apache.nifi.processors.sha0w;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.AbstractDatabaseFetchProcessor;
import org.apache.nifi.processors.standard.SplitText;
import org.apache.nifi.processors.standard.db.DatabaseAdapter;
import org.apache.nifi.stream.io.NullOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.stream.io.util.TextLineDemarcator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.util.*;
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "select", "jdbc", "query", "database"})
public class TimeDataWash extends AbstractProcessor {
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    public static final PropertyDescriptor YEAR_FIELD = new PropertyDescriptor.Builder()
            .name("YEAR FIELD")
            .defaultValue("year")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .description("the field you choose to get year data")
            .build();

    public static final PropertyDescriptor MONTH_FIELD = new PropertyDescriptor.Builder()
            .name("MONTH FIELD")
            .defaultValue("month")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .description("the field you choose to get month data")
            .build();

    public static final PropertyDescriptor DAY_FIELD = new PropertyDescriptor.Builder()
            .name("DAY FIELD")
            .defaultValue("day")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .description("the field you choose to get day data")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Succes relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure relationship")
            .build();
    @Override
    public void init(final ProcessorInitializationContext context ) {
        List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(YEAR_FIELD);
        pds.add(MONTH_FIELD);
        pds.add(DAY_FIELD);
        this.properties = Collections.unmodifiableList(pds);
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
//        防止多线程ADD
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        final ComponentLog logger = getLogger();
        final String year_field = processContext.getProperty(YEAR_FIELD).getValue();
        final String day_field = processContext.getProperty(DAY_FIELD).getValue();
        final String month_field = processContext.getProperty(MONTH_FIELD).getValue();
        //get the flow file
        FlowFile flowFile = processSession.get();
        //valid
        if (flowFile == null) return;
        String avroValue = "";
        //process
        processSession.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {

            }
        });
        logger.debug("process this flowfile and return the date");

    }

}
