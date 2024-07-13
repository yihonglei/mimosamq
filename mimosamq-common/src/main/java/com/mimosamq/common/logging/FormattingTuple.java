package com.mimosamq.common.logging;


/**
 * Holds the results of formatting done by {@link MessageFormatter}.
 */
public final class FormattingTuple {

    private final String message;
    private final Throwable throwable;

    public FormattingTuple(String message, Throwable throwable) {
        this.message = message;
        this.throwable = throwable;
    }

    public String getMessage() {
        return message;
    }

    public Throwable getThrowable() {
        return throwable;
    }
}
