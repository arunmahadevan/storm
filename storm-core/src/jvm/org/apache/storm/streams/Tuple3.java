package org.apache.storm.streams;

/**
 * A tuple of three elements along the lines of Scala's Tuple.
 *
 * @param <T1> the type of the first element
 * @param <T2> the type of the second element
 * @param <T3> the type of the third element
 */
public class Tuple3<T1, T2, T3> {
    public final T1 _1;
    public final T2 _2;
    public final T3 _3;

    /**
     * Constructs a new tuple of three elements.
     *
     * @param _1 the first element
     * @param _2 the second element
     * @param _3 the third element
     */
    public Tuple3(T1 _1, T2 _2, T3 _3) {
        this._1 = _1;
        this._2 = _2;
        this._3 = _3;
    }

    @Override
    public String toString() {
        return "(" + _1 + "," + _2 + "," + _3 + ")";
    }
}
