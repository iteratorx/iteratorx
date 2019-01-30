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

import io.iteratorx.Consumer;

public class Threads<T> implements Parallels {
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

	public static <T> Threads<T> create(final Iterator<T> iterator) throws Exception {
		return new Threads<T>(iterator);
	}

	public static <T> Threads<T> create(final Iterator<T> iterator, final int parallelism) throws Exception {
		return new Threads<T>(iterator, DefaultParallelism);
	}

	public static <T> Threads<T> create(final Iterable<T> iterable) throws Exception {
		return new Threads<T>(iterable);
	}

	public static <T> Threads<T> create(final Iterable<T> iterable, final int parallelism) throws Exception {
		return new Threads<T>(iterable, DefaultParallelism);
	}

	private final Iterator<T> iterator;
	private final int parallelism;

	public Threads(final Iterator<T> iterator) {
		this(iterator, DefaultParallelism);
	}

	public Threads(final Iterator<T> iterator, final int parallelism) {
		this.iterator = iterator;
		this.parallelism = parallelism;
	}

	public Threads(final Iterable<T> iterable) {
		this(iterable.iterator());
	}

	public Threads(final Iterable<T> iterable, final int parallelism) {
		this(iterable.iterator(), parallelism);
	}

	public long forEach(final Consumer<T> onNext) throws Exception {
		final long count = forEach(this.parallelism, this.iterator, onNext);

		logger.info("OK finished! count = " + count);
		return count;
	}

	public long forBatch(final Consumer<List<T>> onNext) throws Exception {
		return forBatch(DefaultBatchSize, onNext);
	}

	public long forBatch(final int batchSize, final Consumer<List<T>> onNext) throws Exception {
		final AtomicLong count = new AtomicLong();
		final Iterator<List<T>> groupIterator = new Iterator<List<T>>() {

			final List<T> group = new ArrayList<>();

			@Override
			public boolean hasNext() {
				return Threads.this.iterator.hasNext();
			}

			@Override
			public List<T> next() {
				do {
					final T item = Threads.this.iterator.next();
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
