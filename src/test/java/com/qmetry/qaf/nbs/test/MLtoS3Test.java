package com.qmetry.qaf.nbs.test;


import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.marklogic.client.DatabaseClient;
import org.json.JSONArray;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class MLtoS3Test extends BaseMLtoS3 {
    @BeforeSuite
    public void startUp() throws SQLException {
        client = connectML();
    }

        @Test
    public void tc_count() throws IOException {
        getCountFromML(client);
        getCountFromS3Raw();

    }

//    @Test
    public void tc_comparison() throws IOException {
        // Create a file for Error logging
        FileWriter errorLog = writeFile();
        // S3
        JSONArray listURI = getUriFromS3Raw();
        // ML
//
        List<String> MLURI = pageListUri(client);
        List<String> S3URI = MLObjectIDlist();
//        System.out.println(S3URI.size());
//        System.out.println(MLURI.size());
        List<String> testcomp = commonURI(MLURI, S3URI);
        List<String> missing_objects = differentURI(MLURI, S3URI);
        for (int i = 0; i < testcomp.size() - 1; i++) {
            System.out.println(testcomp.get(i));
            Map<Object, Object> ML = readDoc(client, testcomp.get(i));
            for (int y = 0; y <= listURI.length(); y++) {
                if (listURI.getJSONObject(y).get("objectId").toString().equals(testcomp.get(i))) {
                    Map<Object, Object> S3 = stringToMap(listURI.getJSONObject(y).toString());
                    MapDifference<Object, Object> diff = Maps.difference(ML, S3);
                    if (diff.entriesDiffering().size() != 0) {
                        System.out.println("MisMatched ObjectID: " + ML.get("objectId"));
                        errorLog.write("MisMatched ObjectID: " + ML.get("objectId"));
                        errorLog.append(System.getProperty("line.separator"));
                        errorLog.write("ML: " + ML);
                        errorLog.append(System.getProperty("line.separator"));
                        errorLog.write("S3: " + S3);
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
    public void tearDown(){
        client.release();
    }
}
