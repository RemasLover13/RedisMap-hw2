package org.redis;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisPool;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class RedisMapImplTest {

    private JedisPool jedisPool;
    private Map<String, String> map;
    private static final int REDIS_PORT = 6379;

    @Container
    public GenericContainer<?> redis =
            new GenericContainer<>("redis:latest")
                    .withExposedPorts(REDIS_PORT);

    @BeforeEach
    void setUp() {
        jedisPool = new JedisPool(
                redis.getHost(),
                redis.getMappedPort(REDIS_PORT)
        );
        map = new RedisMapImpl(jedisPool, "testMap");
    }

    @AfterEach
    void tearDown() {
        map.clear();
        jedisPool.close();
    }

    @Test
    void testPutAndGet() {
        assertNull(map.put("key1", "value1"));
        assertEquals("value1", map.get("key1"));

        assertEquals("value1", map.put("key1", "new_value"));
        assertEquals("new_value", map.get("key1"));
    }

    @Test
    void testSize() {
        assertEquals(0, map.size());
        map.put("a", "1");
        assertEquals(1, map.size());
        map.put("b", "2");
        assertEquals(2, map.size());
        map.remove("a");
        assertEquals(1, map.size());
    }

    @Test
    void testIsEmpty() {
        assertTrue(map.isEmpty());
        map.put("temp", "value");
        assertFalse(map.isEmpty());
        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    void testContainsKey() {
        assertFalse(map.containsKey("missing"));
        map.put("existing", "value");
        assertTrue(map.containsKey("existing"));
        assertFalse(map.containsKey("EXISTING"));
    }

    @Test
    void testContainsValue() {
        assertFalse(map.containsValue("value"));
        map.put("key", "value");
        assertTrue(map.containsValue("value"));
        assertFalse(map.containsValue("VALUE"));
    }

    @Test
    void testRemove() {
        assertNull(map.remove("non-existent"));

        map.put("toRemove", "value");
        assertEquals("value", map.remove("toRemove"));
        assertNull(map.get("toRemove"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPutAll() {
        Map<String, String> data = Map.of(
                "k1", "v1",
                "k2", "v2",
                "k3", "v3"
        );

        map.putAll(data);
        assertEquals(3, map.size());
        assertEquals("v1", map.get("k1"));
        assertEquals("v3", map.get("k3"));
    }

    @Test
    void testClear() {
        map.put("a", "1");
        map.put("b", "2");
        map.clear();
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
    }

    @Test
    void testKeySet() {
        map.put("k1", "v1");
        map.put("k2", "v2");

        Set<String> keys = map.keySet();
        assertEquals(2, keys.size());
        assertTrue(keys.containsAll(Set.of("k1", "k2")));
    }

    @Test
    void testValues() {
        map.put("k1", "v1");
        map.put("k2", "v2");

        Collection<String> values = map.values();
        assertEquals(2, values.size());
        assertTrue(values.containsAll(List.of("v1", "v2")));
    }

    @Test
    void testEntrySet() {
        map.put("name", "Vanya");
        map.put("age", "32");

        Set<Map.Entry<String, String>> entries = map.entrySet();
        assertEquals(2, entries.size());

        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : entries) {
            result.put(entry.getKey(), entry.getValue());
        }

        assertEquals("Vanya", result.get("name"));
        assertEquals("32", result.get("age"));
    }

    @Test
    void testConcurrentModification() {
        map.put("old", "value");

        map.put("newKey", "newValue");
        assertTrue(map.containsKey("newKey"));

        map.remove("old");
        assertFalse(map.containsKey("old"));
    }
}
