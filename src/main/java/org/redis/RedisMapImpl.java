package org.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Collection;
import java.util.Map;
import java.util.Set;


/**
 * Реализация интерфейса {@link Map} для работы с Redis Hash.
 * Хранит данные в виде хэша Redis с заданным именем, обеспечивая
 * прозрачный доступ через стандартный Map-интерфейс.
 *
 * <p><b>Особенности реализации:</b>
 * <ul>
 *   <li>Все операции атомарны на уровне Redis</li>
 *   <li>Потокобезопасность обеспечивается через {@link JedisPool}</li>
 *   <li>Ключи и значения должны быть строками</li>
 *   <li>Некоторые методы могут быть неэффективны для больших данных</li>
 * </ul>
 *
 * @see Jedis
 * @see JedisPool
 */
public class RedisMapImpl implements Map<String, String> {

    private final JedisPool jedisPool;
    private final String mapName;

    public RedisMapImpl(JedisPool jedisPool, String mapName) {
        this.jedisPool = jedisPool;
        this.mapName = mapName;
    }

    /**
     * Возвращает количество элементов в хэше.
     *
     * @return количество элементов, 0 если хэш не существует
     */
    @Override
    public int size() {
        try (Jedis jedis = jedisPool.getResource()) {
            return (int) jedis.hlen(mapName);
        }
    }

    /**
     * Проверяет отсутствие элементов в хэше.
     *
     * @return true если хэш пуст или не существует
     */
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Проверяет наличие ключа в хэше.
     *
     * @param key ключ для проверки (должен быть String)
     * @return true если ключ существует
     * @throws ClassCastException если ключ не String
     */
    @Override
    public boolean containsKey(Object key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hexists(mapName, (String) key);
        }
    }

    /**
     * Проверяет наличие значения в хэше.
     *
     * @param value значение для поиска
     * @return true если значение присутствует
     */
    @Override
    public boolean containsValue(Object value) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> all = jedis.hgetAll(mapName);
            return all.containsValue(value);
        }
    }

    /**
     * Возвращает значение по ключу.
     *
     * @param key ключ для поиска
     * @return значение или null если ключ отсутствует
     * @throws ClassCastException если ключ не String
     */
    @Override
    public String get(Object key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hget(mapName, (String) key);
        }
    }

    /**
     * Добавляет или обновляет значение по ключу.
     *
     * @param key   ключ для добавления (не null)
     * @param value значение для добавления
     * @return предыдущее значение или null
     */
    @Override
    public String put(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            String oldValue = get(key);
            jedis.hset(mapName, key, value);
            return oldValue;
        }
    }

    /**
     * Удаляет ключ из хэша.
     *
     * @param key ключ для удаления
     * @return предыдущее значение или null
     * @throws ClassCastException если ключ не String
     */
    @Override
    public String remove(Object key) {
        try (Jedis jedis = jedisPool.getResource()) {
            String oldValue = get(key);
            jedis.hdel(mapName, (String) key);
            return oldValue;
        }
    }

    /**
     * Добавляет все элементы из указанной Map.
     *
     * @param m Map с элементами для добавления
     * @throws NullPointerException если Map или любой ключ/значение null
     */
    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hset(mapName, (Map<String, String>) m);
        }
    }

    /**
     * Полностью удаляет хэш из Redis.
     */
    @Override
    public void clear() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(mapName);
        }
    }

    /**
     * Возвращает неизменяемый набор ключей.
     *
     * @return множество ключей (не null)
     */
    @Override
    public Set<String> keySet() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hkeys(mapName);
        }
    }

    /**
     * Возвращает коллекцию значений.
     *
     * @return коллекция значений (не null)
     */
    @Override
    public Collection<String> values() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hvals(mapName);
        }
    }

    /**
     * Возвращает набор пар ключ-значение.
     *
     * @return неизменяемый набор записей
     */
    @Override
    public Set<Map.Entry<String, String>> entrySet() {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, String> all = jedis.hgetAll(mapName);
            return all.entrySet();
        }
    }
}
