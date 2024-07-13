package com.mimosamq.common.logging;

import org.junit.Test;

public class InternalLoggerFactoryTest {
    @Test
    public void testCreation() {
        final InternalLogger logger = InternalLoggerFactory.getInstance(InternalLoggerFactoryTest.class);
        logger.info("....");
    }
}
