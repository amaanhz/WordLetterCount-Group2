package org.wordlettercount;

import java.io.Serializable;
import java.util.Comparator;
import scala.Tuple2;

/**
 * To implement sorting by two ways, we need to implement a custom comparator that allows us to do this.
 * 
 * Inspiration from https://stackoverflow.com/questions/38131604/sorting-javapairrdd-first-by-value-and-then-by-key?rq=3
 */
public class TuplePrimarySecondaryComparator implements Comparator<Tuple2<Integer, String>>, Serializable {
    @Override
    public int compare(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
        int c = t1._1.compareTo(t2._1);
        if (c != 0) {
            return c;
        } else {
            return t1._2.compareTo(t2._2);
        }
    }
}
