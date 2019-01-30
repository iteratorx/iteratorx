package io.iteratorx.parallels;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.alibaba.fastjson.JSONObject;

import io.iteratorx.Consumer;

public class Threads implements Parallels {
	private static final Logger logger = Logger.getLogger(Threads.class.getName());

	public static <T> long forEach(final int parallelism, final Iterator<T> iterator, final Consumer<T> onNext)
			throws Exception {

		// for back pressure
		final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(parallelism * 3);
		final ExecutorService executorService = new ThreadPoolExecutor(parallelism, parallelism, 0L,
				TimeUnit.MILLISECONDS, queue);

		// submit runnable
		long count = 0;
		while (iterator.hasNext()) {
			final T item = iterator.next();
			count++;

			while (queue.size() >= parallelism * 3) {
				Thread.sleep(10);
			}

			executorService.submit(new Runnable() {
				@Override
				public void run() {
					try {
						onNext.accept(item);

					} catch (final Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		}

		// shutdown, still executing
		executorService.shutdown();

		// waiting for executing finished
		try {
			if (!executorService.awaitTermination(1, TimeUnit.DAYS)) {
				executorService.shutdownNow();
			}
		} catch (final InterruptedException e) {

			// shutdown, stop executing
			executorService.shutdownNow();

			throw e;
		}

		return count;
	}

	public static Threads create(final Iterator<JSONObject> iterator) throws Exception {
		return new Threads(iterator);
	}

	public static Threads create(final Iterator<JSONObject> iterator, final int parallelism) throws Exception {
		return new Threads(iterator, DefaultParallelism);
	}

	public static Threads create(final Iterable<JSONObject> iterable) throws Exception {
		return new Threads(iterable);
	}

	public static Threads create(final Iterable<JSONObject> iterable, final int parallelism) throws Exception {
		return new Threads(iterable, DefaultParallelism);
	}

	private final Iterator<JSONObject> iterator;
	private final int parallelism;

	public Threads(final Iterator<JSONObject> iterator) {
		this(iterator, DefaultParallelism);
	}

	public Threads(final Iterator<JSONObject> iterator, final int parallelism) {
		this.iterator = iterator;
		this.parallelism = parallelism;
	}

	public Threads(final Iterable<JSONObject> iterable) {
		this(iterable.iterator());
	}

	public Threads(final Iterable<JSONObject> iterable, final int parallelism) {
		this(iterable.iterator(), parallelism);
	}

	public long forEach(final Consumer<JSONObject> onNext) throws Exception {
		final long count = forEach(this.parallelism, this.iterator, onNext);

		logger.info("OK finished! count = " + count);
		return count;
	}

	public long forBatch(final Consumer<List<JSONObject>> onNext) throws Exception {
		return forBatch(DefaultBatchSize, onNext);
	}

	public long forBatch(final int batchSize, final Consumer<List<JSONObject>> onNext) throws Exception {
		final AtomicLong count = new AtomicLong();
		final Iterator<List<JSONObject>> groupIterator = new Iterator<List<JSONObject>>() {

			final List<JSONObject> group = new ArrayList<>();

			@Override
			public boolean hasNext() {
				return Threads.this.iterator.hasNext();
			}

			@Override
			public List<JSONObject> next() {
				do {
					final JSONObject item = Threads.this.iterator.next();
					group.add(item);
					count.incrementAndGet();

					if (group.size() >= batchSize) {
						try {
							return new ArrayList<>(group);
						} finally {
							group.clear();
						}
					}

				} while (Threads.this.iterator.hasNext());

				if (group.size() > 0) {
					return new ArrayList<>(group);
				} else {
					return null;
				}
			}
		};

		final long group = forEach(this.parallelism, groupIterator, onNext);
		logger.info("OK finished! count = " + count + "; group = " + group);
		return count.get();
	}
}
