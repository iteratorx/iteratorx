package io.iteratorx.samples;

import java.io.File;
import java.util.Collection;

import com.alibaba.fastjson.JSONObject;

import io.iteratorx.reader.FileReader;

public class FileReaderUsage {

	public static void main(final String[] args) throws Exception {

		// create file reader
		final FileReader fileReader = new FileReader();

		// fetch by iterable
		for (final JSONObject item : fileReader.read(new File("data.json"), "utf-8")) {
			System.err.println(item);
		}

		// fetch all into one collection
		final Collection<JSONObject> items = fileReader.readAll(new File("data.json"), "utf-8");
		for (final JSONObject item : items) {
			System.err.println(item);
		}

	}
}
