package com.qmetry.qaf.nbs.test;

import com.qmetry.qaf.automation.testng.dataprovider.QAFDataProvider;
import org.testng.annotations.Test;

import com.qmetry.qaf.automation.ui.WebDriverTestCase;

import java.util.Map;

public class SampleTest extends WebDriverTestCase {
	@QAFDataProvider(sqlQuery = "select object_type from public.extraction_job")
	@Test
	public void testGoogleSearch(Map<String, String> data) {
		System.out.println(data.get("object_type"));
	}
}
