package io.iteratorx;

import java.io.Serializable;

public interface Function<T, R> extends Serializable {

	R apply(T t) throws Exception;
}