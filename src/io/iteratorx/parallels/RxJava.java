package io.iteratorx.parallels;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import io.iteratorx.Consumer;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;

public class RxJava<T> implements Parallels {
	private static final Logger logger = Logger.getLogger(RxJava.class.getName());

	public static <T> void forEach(final Observable<T> observable, final Consumer<T> onNext) {
		observable.subscribe(t -> {
			onNext.accept(t);
		});

		logger.info("OK finished!");
	}

	public static <T> RxJava<T> from(final Iterator<T> iterator) throws Exception {
		return new RxJava<T>(iterator);
	}

	public static <T> RxJava<T> from(final Iterator<T> iterator, final int parallelism) throws Exception {
		return new RxJava<T>(iterator, DefaultParallelism);
	}

	public static <T> RxJava<T> from(final Iterable<T> iterable) throws Exception {
		return new RxJava<T>(iterable);
	}

	public static <T> RxJava<T> from(final Iterable<T> iterable, final int parallelism) throws Exception {
		return new RxJava<T>(iterable, DefaultParallelism);
	}

	private final Observable<T> observable;

	public RxJava(final Iterator<T> iterator) {
		this(iterator, DefaultParallelism);
	}

	public RxJava(final Iterator<T> iterator, final int parallelism) {
		this(new Iterable<T>() {

			@Override
			public Iterator<T> iterator() {
				return iterator;
			}
		}, parallelism);
	}

	public RxJava(final Iterable<T> iterable) {
		this(iterable, DefaultParallelism);
	}

	public RxJava(final Iterable<T> iterable, final int parallelism) {
		this.observable = Observable.fromIterable(iterable)
				.observeOn(Schedulers.from(Executors.newFixedThreadPool(parallelism)))
				.subscribeOn(Schedulers.from(Executors.newFixedThreadPool(parallelism)));
	}

	public Observable<T> observable() {
		return this.observable;
	}

	public void forEach(final Consumer<T> onNext) {
		forEach(this.observable, onNext);
	}

	public void forBatch(final Consumer<List<T>> onNext) {
		forBatch(DefaultBatchSize, onNext);
	}

	public void forBatch(final int batchSize, final Consumer<List<T>> onNext) {
		forEach(this.observable.buffer(batchSize), onNext);
	}

}
