package com.qmetry.qaf.nbs.test;


import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.json.JSONArray;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class MLtoS3Test extends BaseMLtoS3 {
    @BeforeSuite
    public void startUp() throws IOException {
        mlclient = connectML();
        s3client = connectS3();
    }

    @Test
    public void tc_count() {
        Integer marklogic_count = getCountFromML();
        Integer s3_count = getCountFromS3Raw();
        Assert.assertEquals(s3_count, marklogic_count);
    }

    //    @Test
    public void tc_comparison() throws IOException {
        // Create a file for Error logging
        FileWriter errorLog = writeFile();
        // S3
        JSONArray listURI = getUriFromS3Raw();
        // ML
//
        List<String> ml_uri = pageListUri();
        List<String> s3_uri = s3ObjectList(listURI);
//        System.out.println(s3_uri.size());
//        System.out.println(ml_uri.size());
        List<String> common_uri = commonURI(ml_uri, s3_uri);
        List<String> missing_objects = differentURI(ml_uri, s3_uri);
        for (int i = 0; i < common_uri.size() - 1; i++) {
            System.out.println(common_uri.get(i));
            Map<Object, Object> ml_document = readDoc(common_uri.get(i));
            for (int y = 0; y <= listURI.length(); y++) {
                if (listURI.getJSONObject(y).get("objectId").toString().equals(common_uri.get(i))) {
                    Map<Object, Object> s3_document = stringToMap(listURI.getJSONObject(y).toString());
                    MapDifference<Object, Object> diff = Maps.difference(ml_document, s3_document);
                    if (diff.entriesDiffering().size() != 0) {
                        System.out.println("MisMatched ObjectID: " + ml_document.get("objectId"));
                        errorLog.write("MisMatched ObjectID: " + ml_document.get("objectId"));
                        errorLog.append(System.getProperty("line.separator"));
                        errorLog.write("ML: " + ml_document);
                        errorLog.append(System.getProperty("line.separator"));
                        errorLog.write("S3: " + s3_document);
                        errorLog.append(System.getProperty("line.separator"));
                        errorLog.write("Difference: " + diff.entriesDiffering());
                        errorLog.append(System.getProperty("line.separator"));
                        errorLog.append(System.getProperty("line.separator"));
                    }
                    break;
                }
            }
        }
        if (missing_objects != null) {
            errorLog.write(missing_objects.size() + " ObjectIDs not found in S3: " + missing_objects);
        }
        errorLog.close();
    }

    @AfterSuite
    public void tearDown() {
        mlclient.release();
    }
}
