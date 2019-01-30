package io.iteratorx.parallels;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;

import com.alibaba.fastjson.JSONObject;

import io.iteratorx.Consumer;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;

public class RxJava implements Parallels {

	public static <T> void forEach(final Observable<T> observable, final Consumer<T> onNext) {
		observable.subscribe(t -> {
			onNext.accept(t);
		});
	}

	public static RxJava create(final Iterator<JSONObject> iterator) throws Exception {
		return new RxJava(iterator);
	}

	public static RxJava create(final Iterator<JSONObject> iterator, final int parallelism) throws Exception {
		return new RxJava(iterator, DefaultParallelism);
	}

	public static RxJava create(final Iterable<JSONObject> iterable) throws Exception {
		return new RxJava(iterable);
	}

	public static RxJava create(final Iterable<JSONObject> iterable, final int parallelism) throws Exception {
		return new RxJava(iterable, DefaultParallelism);
	}

	private final Observable<JSONObject> observable;

	public RxJava(final Iterator<JSONObject> iterator) {
		this(iterator, DefaultParallelism);
	}

	public RxJava(final Iterator<JSONObject> iterator, final int parallelism) {
		this(new Iterable<JSONObject>() {

			@Override
			public Iterator<JSONObject> iterator() {
				return iterator;
			}
		}, parallelism);
	}

	public RxJava(final Iterable<JSONObject> iterable) {
		this(iterable, DefaultParallelism);
	}

	public RxJava(final Iterable<JSONObject> iterable, final int parallelism) {
		this.observable = Observable.fromIterable(iterable)
				.observeOn(Schedulers.from(Executors.newFixedThreadPool(parallelism)))
				.subscribeOn(Schedulers.from(Executors.newFixedThreadPool(parallelism)));
	}

	public void forEach(final Consumer<JSONObject> onNext) {
		forEach(this.observable, onNext);
	}

	public void forBatch(final Consumer<List<JSONObject>> onNext) {
		forBatch(DefaultBatchSize, onNext);
	}

	public void forBatch(final int batchSize, final Consumer<List<JSONObject>> onNext) {
		forEach(this.observable.buffer(batchSize), onNext);
	}

}
