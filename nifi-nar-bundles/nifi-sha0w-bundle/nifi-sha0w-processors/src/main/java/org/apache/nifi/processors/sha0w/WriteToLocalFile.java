package org.apache.nifi.processors.sha0w;


import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by coco1 on 2017/7/20.
 *
 */
@Tags({ "write", "local", "file", "sha0w", "DIY" })
@CapabilityDescription("Provides a mechanism to write insert byte code in FlowFile to local file")
public class WriteToLocalFile extends AbstractProcessor {
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;


    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String FAIL_RESOLUTION = "fail";
    public static final String APPEND_RESOLUTION = "append";


    public static final PropertyDescriptor FILE_PATH = new PropertyDescriptor.Builder()
            .name("FILE PATH")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FILE_NAME = new PropertyDescriptor.Builder()
            .name("FILE NAME")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CREATE_ABLE = new PropertyDescriptor.Builder()
            .name("CAN CREATE NEW FILE")
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHAR_SET = new PropertyDescriptor.Builder()
            .name("CHAR SET")
            .defaultValue("utf-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(REPLACE_RESOLUTION)
            .allowableValues(REPLACE_RESOLUTION, IGNORE_RESOLUTION, FAIL_RESOLUTION, APPEND_RESOLUTION)
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
        ArrayList<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(FILE_PATH);
        properties.add(FILE_NAME);
        properties.add(CHAR_SET);
        properties.add(CREATE_ABLE);
        properties.add(CONFLICT_RESOLUTION);
//        防止多线程ADD
        this.properties = Collections.unmodifiableList(properties);
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
//        防止多线程ADD
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final String conflictResponse = context.getProperty(CONFLICT_RESOLUTION).getValue();
        FlowFile flowFile = session.get();
        if (flowFile == null) return;
        if (flowFile.isPenalized()) return;
        final Path configuredRootDirPath = Paths.get(context.getProperty(FILE_PATH).getValue());
        final String fileName = context.getProperty(FILE_NAME).getValue();
        Path writeFilePath = null;
        try {
            if (!Files.exists(configuredRootDirPath)) {
                if (context.getProperty(CREATE_ABLE).asBoolean()) {
                    Files.createFile(configuredRootDirPath);
                } else {
                // penalize make you cannot operate the flow file further more
                    session.transfer(flowFile, FAILURE);
                    return;
                }
            }
            writeFilePath = configuredRootDirPath.resolve(fileName);
            if (Files.exists(writeFilePath)) {
                switch (conflictResponse) {
                    case REPLACE_RESOLUTION:
                        Files.delete(writeFilePath);
                        Files.createFile(writeFilePath);
                        break;
                    case IGNORE_RESOLUTION:
                        session.transfer(flowFile, SUCCESS);
                        return;
                    case FAIL_RESOLUTION:
                        flowFile = session.penalize(flowFile);
                        session.transfer(flowFile, FAILURE);
                        return;
                    default:
                        break;
                }
            }
            session.exportTo(flowFile, new FileOutputStream(writeFilePath.toFile(), true));

            session.transfer(flowFile, SUCCESS);

        }catch (IOException e) {
            e.printStackTrace();
        }

    }

    protected String stringPermissions(String perms) {
        String permissions = "";
        final Pattern rwxPattern = Pattern.compile("^[rwx-]{9}$");
        final Pattern numPattern = Pattern.compile("\\d+");
        if (rwxPattern.matcher(perms).matches()) {
            permissions = perms;
        } else if (numPattern.matcher(perms).matches()) {
            try {
                int number = Integer.parseInt(perms, 8);
                StringBuilder permBuilder = new StringBuilder();
                if ((number & 0x100) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x80) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x40) > 0) {
                    permBuilder.append('x');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x20) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x10) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x8) > 0) {
                    permBuilder.append('x');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x4) > 0) {
                    permBuilder.append('r');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x2) > 0) {
                    permBuilder.append('w');
                } else {
                    permBuilder.append('-');
                }
                if ((number & 0x8) > 0) {
                    permBuilder.append('x');
                } else {
                    permBuilder.append('-');
                }
                permissions = permBuilder.toString();
            } catch (NumberFormatException ignore) {
            }
        }
        return permissions;
    }


    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }
}
