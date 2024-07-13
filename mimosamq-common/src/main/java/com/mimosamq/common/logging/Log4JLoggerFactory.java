package com.mimosamq.common.logging;

import org.apache.log4j.Logger;

/**
 * Logger factory which creates an
 * <a href="https://logging.apache.org/log4j/1.2/index.html">Apache Log4J</a>
 * logger.
 */
public class Log4JLoggerFactory extends InternalLoggerFactory {

    public static final InternalLoggerFactory INSTANCE = new Log4JLoggerFactory();
    
    @Override
    public InternalLogger newInstance(String name) {
        return new Log4JLogger(Logger.getLogger(name));
    }
}
