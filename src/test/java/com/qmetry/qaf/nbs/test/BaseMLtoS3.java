package com.qmetry.qaf.nbs.test;


import java.io.*;
import java.util.*;
import java.lang.reflect.Type;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import com.amazonaws.util.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.*;
import com.marklogic.client.document.JSONDocumentManager;

import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.query.*;
import org.json.JSONArray;
import com.qmetry.qaf.automation.core.ConfigurationManager;


public class BaseMLtoS3 {
    static DatabaseClient mlclient;
    static S3Object s3client = null;
    static String s3ObjectContent = null;
    private String fileName = null;


    public DatabaseClient connectML() {
        System.out.println("Connecting ML");
        return DatabaseClientFactory.newClient(
                ConfigurationManager.getBundle().getString("ml.db.host"), ConfigurationManager.getBundle().getInt("ml.db.port"), ConfigurationManager.getBundle().getString("ml.db.database"),
                new DatabaseClientFactory.DigestAuthContext(ConfigurationManager.getBundle().getString("ml.db.user"), ConfigurationManager.getBundle().getString("ml.db.pass")));
    }


    public List<String> mlDocumentList() {
        List<String> uriList = new ArrayList<>();
        final DataMovementManager manager = mlclient.newDataMovementManager();
        final StructuredQueryBuilder qb = new StructuredQueryBuilder();
        StructuredQueryDefinition query = qb.directory(1, ConfigurationManager.getBundle().getString("ml.uri"));
        ExportListener exportListener = new ExportListener().withConsistentSnapshot().onDocumentReady(doc -> {
            String[] uriParts = doc.getUri().split("/");
            try {
                fileName = uriParts[uriParts.length - 1];
                uriList.add(fileName);

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        final QueryBatcher batch = manager.newQueryBatcher(query).withBatchSize(100).withThreadCount(2)
                .withConsistentSnapshot().onUrisReady(exportListener).onQueryFailure(Throwable::printStackTrace);
        JobTicket ticket = manager.startJob(batch);
        batch.awaitCompletion();
        manager.stopJob(ticket);
        return uriList;
    }

    public Integer getCountFromML() {
        final DataMovementManager manager = mlclient.newDataMovementManager();
        final StructuredQueryBuilder qb = new StructuredQueryBuilder();
        StructuredQueryDefinition query = qb.directory(1, ConfigurationManager.getBundle().getString("ml.uri"));
        ExportListener exportListener = new ExportListener().withConsistentSnapshot().onDocumentReady(doc -> {
        });
        final QueryBatcher batch = manager.newQueryBatcher(query).withBatchSize(100).withThreadCount(2)
                .withConsistentSnapshot().onUrisReady(exportListener).onQueryFailure(Throwable::printStackTrace);
        JobTicket ticket = manager.startJob(batch);
        batch.awaitCompletion();
        manager.stopJob(ticket);
        JobReport report = manager.getJobReport(ticket);
        System.out.println("Total count from ML: " + report.getSuccessEventsCount());
        return Math.toIntExact(report.getSuccessEventsCount());
    }

    public Map<Object, Object> readDoc(String objectURI) {
        JSONDocumentManager docMgr = mlclient.newJSONDocumentManager();
        String docId = ConfigurationManager.getBundle().getString("ml.uri") + objectURI;
        JacksonHandle handle = new JacksonHandle();
        docMgr.read(docId, handle);
        JsonNode node = handle.get();
        return stringToMap(node.toString());
    }

    private static String getAsString(InputStream is) throws IOException {
        if (is == null)
            return "";
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is, StringUtils.UTF8));
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } finally {
            is.close();
        }
        return sb.toString();
    }

    public static Map<Object, Object> stringToMap(String payload) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return new TreeMap<Object, Object>(mapper.readValue(payload, Map.class));
        } catch (IOException e) {
            return null;
        }
    }

    public static List<Map<String, Object>> stringToList(String payload) {
        Gson gson = new Gson();
        Type resultType = new TypeToken<List<Map<String, Object>>>() {
        }.getType();
        return gson.fromJson(payload, resultType);
    }

    public S3Object connectS3() throws IOException {
        String bucket = ConfigurationManager.getBundle().getString("ml.s3rawbucket");
        String key = ConfigurationManager.getBundle().getString("ml.s3rawdata");
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        S3Object s3_object = s3Client.getObject(new GetObjectRequest(bucket, key));
        System.out.println("Connecting S3");
        s3ObjectContent = getAsString(s3_object.getObjectContent());
        return s3_object;
    }

    public Integer getCountFromS3Raw() {
        System.out.println("Total count from S3: " + stringToList(s3ObjectContent).size());
        return stringToList(s3ObjectContent).size();
    }

    public JSONArray getUriFromS3Raw() {
        return new JSONArray(s3ObjectContent);
    }

    public void createFile() {
        try {
            File myObj = new File("test-results/logs.txt");
            if (myObj.createNewFile()) {
                System.out.println("File created: " + myObj.getName());
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    public FileWriter writeFile() {
        try {
            createFile();
            return new FileWriter("test-results/logs.txt", false);
        } catch (IOException e) {
            System.out.println("An error occurred while creating the FileWriter object.");
            e.printStackTrace();
            return null;
        }
    }

    public List<String> s3ObjectList(JSONArray test) {
        List<String> uriS3List = new ArrayList<>();
        for (int i = 0; i <= test.length() - 1; i++) {
            uriS3List.add(test.getJSONObject(i).get("objectId").toString());
        }
        return uriS3List;
    }

    public List<String> commonURI(List<String> listOne, List<String> listTwo) {

        List<String> common = new ArrayList<>(listOne);
        common.retainAll(listTwo);
        System.out.println(common);
        return common;
    }

    public List<String> differentURI(List<String> listOne, List<String> listTwo) {
        List<String> differences = new ArrayList<>(listOne);
        differences.removeAll(listTwo);
        System.out.println("difference: " + differences);
        return differences;
    }
}
