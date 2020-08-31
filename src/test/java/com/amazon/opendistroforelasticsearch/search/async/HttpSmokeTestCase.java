package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.nio.MockNioTransportPlugin;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collection;

public abstract class HttpSmokeTestCase extends ESIntegTestCase {

    private static String nodeTransportTypeKey;
    private static String nodeHttpTypeKey;
    private static String clientTypeKey;

    @SuppressWarnings("unchecked")
    @BeforeClass
    public static void setUpTransport() {
        nodeTransportTypeKey = getTypeKey(randomFrom(getTestTransportPlugin(), Netty4Plugin.class));
        nodeHttpTypeKey = getHttpTypeKey(randomFrom(Netty4Plugin.class));
        clientTypeKey = getTypeKey(randomFrom(getTestTransportPlugin(), Netty4Plugin.class));
    }

    private static String getTypeKey(Class<? extends Plugin> clazz) {
        if (clazz.equals(MockNioTransportPlugin.class)) {
            return MockNioTransportPlugin.MOCK_NIO_TRANSPORT_NAME;
        } else {
            assert clazz.equals(Netty4Plugin.class);
            return Netty4Plugin.NETTY_TRANSPORT_NAME;
        }
    }

    private static String getHttpTypeKey(Class<? extends Plugin> clazz) {
            assert clazz.equals(Netty4Plugin.class);
            return Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME;

    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.TRANSPORT_TYPE_KEY, nodeTransportTypeKey)
                .put(NetworkModule.HTTP_TYPE_KEY, nodeHttpTypeKey).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(getTestTransportPlugin(), Netty4Plugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Arrays.asList(getTestTransportPlugin(), Netty4Plugin.class);
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(super.transportClientSettings())
                .put(NetworkModule.TRANSPORT_TYPE_KEY, clientTypeKey)
                .build();
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

}
