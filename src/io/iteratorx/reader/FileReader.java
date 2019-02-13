package io.iteratorx.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.alibaba.fastjson.JSONObject;

import io.iteratorx.AutoCloseableIterator;
import io.iteratorx.Function;

public class FileReader {

	public <T> List<T> readAll(final File file, final String charset, final Function<String, T> function) {
		final List<T> results = new ArrayList<>();
		for (final T one : read(file, charset, function)) {
			results.add(one);
		}
		return results;
	}

	/**
	 * Read each file content line into JSONObject iteratively.
	 * 
	 * NOTICE: thread-unsafe: do not use result Iterable in multi-threads!
	 * 
	 */
	public <T> Iterable<T> read(final File file, final String charset, final Function<String, T> function) {

		// open buffered reader
		final BufferedReader bufferedReader;
		{
			try {
				bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
			} catch (final FileNotFoundException | UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}

		// return iterator
		return new Iterable<T>() {

			@Override
			public Iterator<T> iterator() {
				return new AutoCloseableIterator<T>() {
					private static final long serialVersionUID = 5244479534484878112L;

					String nextData = doNext();

					@Override
					public boolean hasNext() {
						return nextData != null;
					}

					@Override
					public T next() {
						final String result = nextData;
						if (nextData != null) {
							nextData = doNext();
						}
						try {
							return function.apply(result);
						} catch (final Exception e) {
							return null;
						}
					}

					private String doNext() {
						try {
							final String lineContent = bufferedReader.readLine();
							if (lineContent == null) {
								close();
							}

							return lineContent;

						} catch (final IOException e) {
							close();

							throw new RuntimeException(e);
						}
					}

					@Override
					public void close() {
						IOUtils.close(bufferedReader);
						nextData = null;
					}
				};
			}
		};
	}

	public List<JSONObject> readAll(final File file, final String charset) {
		final List<JSONObject> results = new ArrayList<>();
		for (final JSONObject one : read(file, charset)) {
			results.add(one);
		}
		return results;
	}

	/**
	 * Read each file content line into JSONObject iteratively.
	 * 
	 * NOTICE: thread-unsafe: do not use result Iterable in multi-threads!
	 * 
	 */
	public Iterable<JSONObject> read(final File file, final String charset) {
		return read(file, charset, (s) -> {
			return JSONObject.parseObject(s);
		});
	}
}