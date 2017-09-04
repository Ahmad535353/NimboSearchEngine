package utils;

import java.util.Map;

public class Pair<K, V> implements Map.Entry<K, V> {
    private K key;
    private V value;

//    public utils.Pair() {
//        this.key = key;
//        this.value = value;
//    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        V old = this.value;
        this.value = value;
        return old;
    }
    public void setKeyVal(K key, V val){
        this.key = key;
        this.value = val;
    }
}