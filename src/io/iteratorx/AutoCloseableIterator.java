package io.iteratorx;

import java.io.Serializable;
import java.util.Iterator;

public interface AutoCloseableIterator<T> extends Iterator<T>, AutoCloseable, Serializable {

}
