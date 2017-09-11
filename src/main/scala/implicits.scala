package kstreamex

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Aggregator, Initializer, KeyValueMapper, Reducer}

object Implicits {
  implicit def Aggregator[K, V, T](f: (K, V, T) => T): Aggregator[K, V, T] =
    new Aggregator[K, V, T]{
      def apply(key: K, value: V, aggregate: T): T =
        f(key, value, aggregate)
    }


  implicit def reducer[V](f: (V, V) => V): Reducer[V] =
    new Reducer[V]{
      override def apply(value1: V, value2: V): V =
        f(value1, value2)
    }

  implicit def keyValueMapper[K, V, R](f: (K, V) => R): KeyValueMapper[K, V, R] =
    new KeyValueMapper[K, V, R]{
      def apply(key: K, value: V): R =
        f(key, value)
    }

  implicit def initializer[T](i: T): Initializer[T] =
    new Initializer[T]{
      def apply(): T =
        i
    }

  implicit def keyValue[K, V](t: (K, V)): KeyValue[K, V] =
    new KeyValue(t._1, t._2)
}
