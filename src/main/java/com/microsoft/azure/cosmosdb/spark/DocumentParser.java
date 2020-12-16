package com.microsoft.azure.cosmosdb.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.documentdb.Undefined;
import com.microsoft.azure.documentdb.bulkexecutor.internal.DocumentAnalyzer;
import com.microsoft.azure.documentdb.bulkexecutor.internal.ExceptionUtils;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DocumentParser {
    private final static ObjectMapper objectMapper = new ObjectMapper();
    private final static Logger LOGGER = LoggerFactory.getLogger(DocumentAnalyzer.class);
    private final static char SEGMENT_SEPARATOR = '/';
    private final static String ERROR_MESSAGE_FORMAT = "Invalid path \"%s\", failed at %d";

    public String test() {
        LOGGER.info("hello");
        return "hello";
    }


    public String extractPartitionKeyValue(String documentAsString,
                                                                List<String> partitionKeyPath)  {

        if (partitionKeyPath == null || partitionKeyPath.size() == 0) {
            return null;
//            return PartitionKeyInternal.getEmpty();
        }

        return DocumentParser.extractPartitionKeyValueInternal(documentAsString, partitionKeyPath);
    }

    private static String extractPartitionKeyValueInternal(String documentAsString, List<String> partitionKeyPath) {
        JsonNode root;
        try {
            Collection<String> paths = partitionKeyPath;

            root = objectMapper.readTree(documentAsString);

            List<Object> partitionKeyValues = new ArrayList<Object>();

            for (String path: paths)
            {
                Iterator<String> parts = DocumentParser.getPathParts(path).iterator();

                JsonNode node =  root;

                while(parts.hasNext() && node != null) {
                    node = node.path(parts.next());
                }

                Object partitionKeyValue = null;

                if (node != null) {

                    switch (node.getNodeType()) {
                        case BOOLEAN:
                            partitionKeyValue = node.booleanValue();
                            break;
                        case MISSING:
                            partitionKeyValue = Undefined.Value();
                            break;
                        case NULL:
                            partitionKeyValue = JSONObject.NULL;
                            break;
                        case NUMBER:
                            partitionKeyValue = node.numberValue();
                            break;
                        case STRING:
                            partitionKeyValue = node.textValue();
                            break;
                        default:
                            throw new RuntimeException(String.format("undefined json type %s", node.getNodeType()));
                    }
                } else {
                    partitionKeyValue = Undefined.Value();
                }

                partitionKeyValues.add(partitionKeyValue);
            }

            if (partitionKeyValues.size() > 0) {
                return partitionKeyValues.get(0).toString();
            } else {
                return null;
            }


//            return PartitionKeyInternal.fromObjectArray(partitionKeyValues, false);

        } catch (Exception e) {
            LOGGER.error("Failed to extract partition key value from document {}", documentAsString, e);
            throw ExceptionUtils.toRuntimeException(e);
        }
    }

    public static List<String> getPathParts(String path)
    {
        List<String> tokens = new ArrayList<String>();
        AtomicInteger currentIndex = new AtomicInteger();

        while (currentIndex.get() < path.length())
        {
            char currentChar = path.charAt(currentIndex.get());
            if (currentChar != SEGMENT_SEPARATOR)
            {
                throw new IllegalArgumentException(
                        String.format(ERROR_MESSAGE_FORMAT, path, currentIndex.get()));
            }

            if (currentIndex.incrementAndGet() == path.length())
            {
                break;
            }

            currentChar = path.charAt(currentIndex.get());
            if (currentChar == '\"' || currentChar == '\'')
            {
                // Handles the partial path given in quotes such as "'abc/def'"
                tokens.add(getEscapedToken(path, currentIndex));
            }
            else
            {
                tokens.add(getToken(path, currentIndex));
            }
        }

        return tokens;
    }

    private static String getEscapedToken(String path, AtomicInteger currentIndex)
    {
        char quote = path.charAt(currentIndex.get());
        int newIndex = currentIndex.incrementAndGet();

        while (true)
        {
            newIndex = path.indexOf(quote, newIndex);
            if (newIndex == -1)
            {
                throw new IllegalArgumentException(
                        String.format(ERROR_MESSAGE_FORMAT, path, currentIndex.get()));
            }

            // Ignore escaped quote in the partial path we look at such as "'abc/def \'12\'/ghi'"
            if (path.charAt(newIndex - 1) != '\\')
            {
                break;
            }

            ++newIndex;
        }

        String token = path.substring(currentIndex.get(), newIndex);
        currentIndex.set(newIndex + 1);

        return token;
    }

    private static String getToken(String path, AtomicInteger currentIndex)
    {
        int newIndex = path.indexOf(SEGMENT_SEPARATOR, currentIndex.get());
        String token = null;
        if (newIndex == -1)
        {
            token = path.substring(currentIndex.get());
            currentIndex.set(path.length());
        }
        else
        {
            token = path.substring(currentIndex.get(), newIndex);
            currentIndex.set(newIndex);
        }

        token = token.trim();
        return token;
    }


}
