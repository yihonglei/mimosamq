package com.mimosamq.common.logging;

import org.junit.Test;

public class Slf4JLoggerFactoryTest {

    @Test
    public void testCreation() {
        Slf4JLoggerFactory slf4JLoggerFactory = new Slf4JLoggerFactory(true);
        InternalLogger n = slf4JLoggerFactory.newInstance("n");
    }
}
