package io.iteratorx;

import java.io.Serializable;

public interface Consumer<T> extends Serializable {

	void accept(T t) throws Exception;
}